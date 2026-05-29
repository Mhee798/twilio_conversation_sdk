package com.at.twilio_conversation_sdk.conversation;

import static org.apache.commons.io.FileUtils.openInputStream;

import android.os.Handler;
import android.os.Looper;

import androidx.annotation.NonNull;

import com.at.twilio_conversation_sdk.app_interface.AccessTokenInterface;
import com.at.twilio_conversation_sdk.app_interface.ClientInterface;
import com.at.twilio_conversation_sdk.app_interface.MessageInterface;
import com.at.twilio_conversation_sdk.utility.Strings;
import com.twilio.conversations.Attributes;
import com.twilio.conversations.CallbackListener;
import com.twilio.conversations.Conversation;
import com.twilio.conversations.ConversationListener;
import com.twilio.conversations.ConversationsClient;
import com.twilio.conversations.ConversationsClientListener;
import com.twilio.conversations.Media;
import com.twilio.conversations.MediaUploadListener;
import com.twilio.conversations.Message;
import com.twilio.conversations.Messages;
import com.twilio.conversations.Participant;
import com.twilio.conversations.StatusListener;
import com.twilio.conversations.User;
import com.twilio.jwt.accesstoken.AccessToken;
import com.twilio.jwt.accesstoken.ChatGrant;
import com.twilio.util.ErrorInfo;

import org.json.JSONException;
import org.json.JSONObject;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import io.flutter.embedding.engine.plugins.FlutterPlugin;
import io.flutter.plugin.common.MethodChannel;

public class ConversationHandler {
    /// Entry point for the Conversations SDK.
    //
    // `volatile` on every static mutable shared with Twilio worker threads:
    // writers (initializeConversationClient / shutdownClient / detachPluginFromClient
    // on the platform thread; status callbacks on Twilio worker threads) and
    // readers (isClientInitialized() from any thread; triggerEvent / token
    // dispatch from Twilio threads) otherwise have no happens-before edge.
    // Without volatile a worker can see a stale (cached) value of any of these
    // fields long after a teardown has run, NPE'ing on flutterPluginBinding or
    // mis-reporting connection state.
    public static volatile ConversationsClient conversationClient;
    public static volatile FlutterPlugin.FlutterPluginBinding flutterPluginBinding;
    private static volatile MessageInterface messageInterface;
    private static volatile AccessTokenInterface accessTokenInterface;
    private static volatile ConversationsClient.SynchronizationStatus currentSynchronizationStatus = null;
    private static volatile ConversationsClient.ConnectionState currentConnectionState = null;
    /**
     * Reference to the {@link ConversationsClientListener} we attach to the
     * current client in {@link #initializeConversationClient}. Kept so
     * teardown paths (shutdownClient, detachPluginFromClient, the A18
     * old-client cleanup in initializeConversationClient) can call
     * {@code conversationClient.removeListener(...)} explicitly — leaving
     * the listener attached pins the captured {@code clientInterface}
     * (plugin instance) across hot-restart, leaking the previous plugin.
     */
    private static volatile ConversationsClientListener clientListener;

    /**
     * Check if the client is initialized and ready to use
     * 
     * @return true if client is initialized and synchronized, false otherwise
     */
    public static boolean isClientInitialized() {
        if (conversationClient == null) {
            return false;
        }
        return currentConnectionState == ConversationsClient.ConnectionState.CONNECTED
                && currentSynchronizationStatus == ConversationsClient.SynchronizationStatus.COMPLETED;
    }

    /** Max wait for a conversation to reach ALL before we surface a timeout error. */
    private static final long CONVERSATION_SYNC_TIMEOUT_MS = 30_000L;

    /**
     * A10: tracks the {@link ConversationListener} attached by
     * {@link #subscribeToMessageUpdate(String)} per conversation sid so that
     * (a) re-subscribing doesn't stack duplicate listeners and (b)
     * {@link #unSubscribeToMessageUpdate(String)} can remove only its own
     * listener without disturbing in-flight listeners from
     * {@link #runWhenConversationSynchronized}.
     */
    private static final Map<String, ConversationListener> activeMessageListeners =
            new ConcurrentHashMap<>();

    /** Max wait for updateMessages to finish processing all per-message callbacks. */
    private static final long UPDATE_MESSAGES_TIMEOUT_MS = 30_000L;

    /**
     * Clear the per-conversation listener map at client teardown.
     *
     * <p>An earlier iteration of this method tried to call
     * {@code conversation.removeListener(...)} on each entry via an async
     * {@code conversationClient.getConversation(sid, ...)} lookup, but the
     * call sites (shutdownClient, initializeConversationClient's previous-
     * client cleanup) invoke this RIGHT BEFORE {@code conversationClient.shutdown()}
     * — by the time the async getConversation callback fired, the client was
     * already shut down and the lookup either failed or returned a dead
     * Conversation. The "removal" never actually reached Twilio's listener
     * collection.
     *
     * <p>Per Twilio's contract, {@link ConversationsClient#shutdown()} stops
     * dispatching events from any registered ConversationListener on any
     * Conversation belonging to that client. So once shutdown() runs, the
     * listeners can no longer fire — we don't need to explicitly remove them
     * to stop event delivery. The only reason to detach was to allow
     * Conversation/Listener Java references to be GC'd; clearing the static
     * map drops the plugin's hold on the listeners, and the SDK drops the
     * Conversation references when shutdown completes.
     */
    private static void detachAllMessageListeners() {
        activeMessageListeners.clear();
    }

    /**
     * Sync-failure callback used by {@link #runWhenConversationSynchronized}.
     * Defined locally rather than using {@code java.util.function.Consumer} so
     * the plugin's declared {@code minSdk=21} stays valid without core library
     * desugaring ({@code Consumer} is API 24+).
     */
    @FunctionalInterface
    private interface SyncErrorCallback {
        void accept(String message);
    }

    /**
     * Wait until the conversation has synchronized its messages before running
     * {@code onReady}. Twilio's getMessages / getLastMessages / getMessageByIndex
     * throw {@code IllegalStateException("Synchronize the conversation first.")}
     * when called before the conversation reaches
     * {@link Conversation.SynchronizationStatus#ALL}.
     *
     * <p>{@code onFailed} is invoked exactly once with an error message on
     * synchronization failure, listener-registration failure, or after
     * {@link #CONVERSATION_SYNC_TIMEOUT_MS}. {@code onFailed} is always
     * dispatched on the main looper so callers can safely invoke
     * MethodChannel.Result.success from inside it.
     *
     * <p>Either {@code onReady} or {@code onFailed} is guaranteed to run, so
     * callers can rely on it to complete their MethodChannel.Result.
     */
    private static void runWhenConversationSynchronized(
            Conversation conversation,
            Runnable onReady,
            SyncErrorCallback onFailed) {
        final AtomicBoolean handled = new AtomicBoolean(false);
        final Handler mainHandler = new Handler(Looper.getMainLooper());
        final AtomicReference<ConversationListener> listenerHolder = new AtomicReference<>();
        final AtomicReference<Runnable> timeoutHolder = new AtomicReference<>();

        // Post listener removal to the main looper so we never mutate Twilio's
        // listener collection while it is iterating during dispatch.
        final Runnable detachListener = () -> mainHandler.post(() -> {
            try {
                ConversationListener l = listenerHolder.getAndSet(null);
                if (l != null) {
                    conversation.removeListener(l);
                }
            } catch (RuntimeException e) {
                System.err.println(
                        "Conversation removeListener threw for " + conversation.getSid() + ": " + e);
            }
        });

        final Runnable cancelTimeout = () -> {
            Runnable t = timeoutHolder.getAndSet(null);
            if (t != null) {
                mainHandler.removeCallbacks(t);
            }
        };

        // onFailed must run on the main looper because callers invoke
        // MethodChannel.Result.success from inside it, which Flutter requires
        // on the platform thread. Wrap in try/catch so a throwing onFailed
        // (e.g. Result.success after the channel has already been replied to
        // or torn down) does not propagate to Looper.loop() and crash the app.
        final SyncErrorCallback deliverFailed = msg -> mainHandler.post(() -> {
            try {
                onFailed.accept(msg);
            } catch (RuntimeException e) {
                System.err.println("Sync onFailed handler threw for '" + msg + "': " + e);
            }
        });

        // Fast path: terminal at entry.
        Conversation.SynchronizationStatus status = conversation.getSynchronizationStatus();
        if (status == Conversation.SynchronizationStatus.ALL) {
            if (handled.compareAndSet(false, true)) {
                onReady.run();
            }
            return;
        }
        if (status == Conversation.SynchronizationStatus.FAILED) {
            if (handled.compareAndSet(false, true)) {
                System.err.println(
                        "Conversation synchronization already FAILED for " + conversation.getSid());
                deliverFailed.accept(
                        "Conversation synchronization FAILED for " + conversation.getSid());
            }
            return;
        }

        listenerHolder.set(new ConversationListener() {
            @Override
            public void onSynchronizationChanged(Conversation conv) {
                Conversation.SynchronizationStatus s = conv.getSynchronizationStatus();
                if (s == Conversation.SynchronizationStatus.ALL) {
                    if (handled.compareAndSet(false, true)) {
                        cancelTimeout.run();
                        detachListener.run();
                        onReady.run();
                    }
                } else if (s == Conversation.SynchronizationStatus.FAILED) {
                    if (handled.compareAndSet(false, true)) {
                        cancelTimeout.run();
                        detachListener.run();
                        System.err.println(
                                "Conversation synchronization FAILED for " + conv.getSid());
                        deliverFailed.accept(
                                "Conversation synchronization FAILED for " + conv.getSid());
                    }
                }
            }

            @Override
            public void onMessageAdded(Message message) {
            }

            @Override
            public void onMessageUpdated(Message message, Message.UpdateReason reason) {
            }

            @Override
            public void onMessageDeleted(Message message) {
            }

            @Override
            public void onParticipantAdded(Participant participant) {
            }

            @Override
            public void onParticipantUpdated(Participant participant, Participant.UpdateReason reason) {
            }

            @Override
            public void onParticipantDeleted(Participant participant) {
            }

            @Override
            public void onTypingStarted(Conversation c, Participant p) {
            }

            @Override
            public void onTypingEnded(Conversation c, Participant p) {
            }
        });

        // Schedule the timeout BEFORE addListener so any synchronous initial
        // dispatch from addListener can cancel it (Twilio's ConversationImpl
        // fires onSynchronizationChanged with the current state on register).
        // Also rescues the case where another caller invokes
        // conversation.removeAllListeners() on us — the Future still resolves
        // with a timeout error rather than hanging.
        Runnable timeoutRunnable = () -> {
            if (handled.compareAndSet(false, true)) {
                detachListener.run();
                System.err.println(
                        "Conversation synchronization timed out for " + conversation.getSid());
                deliverFailed.accept(
                        "Conversation synchronization timed out for " + conversation.getSid());
            }
        };
        timeoutHolder.set(timeoutRunnable);
        mainHandler.postDelayed(timeoutRunnable, CONVERSATION_SYNC_TIMEOUT_MS);

        try {
            conversation.addListener(listenerHolder.get());
        } catch (RuntimeException e) {
            if (handled.compareAndSet(false, true)) {
                cancelTimeout.run();
                // Use detachListener (not listenerHolder.set(null)) so that if Twilio
                // had partially registered the listener before throwing, we attempt
                // removeListener to clean up. detachListener tolerates a no-op /
                // throwing remove via its own try/catch.
                detachListener.run();
                System.err.println(
                        "Failed to register sync listener for " + conversation.getSid() + ": " + e);
                deliverFailed.accept(
                        "Failed to register sync listener: " + e.getMessage());
            }
            return;
        }

        // Defensive recheck after attaching — covers SDK variants that don't
        // dispatch the current state synchronously on register.
        Conversation.SynchronizationStatus recheck = conversation.getSynchronizationStatus();
        if (recheck == Conversation.SynchronizationStatus.ALL) {
            if (handled.compareAndSet(false, true)) {
                cancelTimeout.run();
                detachListener.run();
                onReady.run();
            }
            return;
        }
        if (recheck == Conversation.SynchronizationStatus.FAILED) {
            if (handled.compareAndSet(false, true)) {
                cancelTimeout.run();
                detachListener.run();
                System.err.println(
                        "Conversation synchronization FAILED for " + conversation.getSid());
                deliverFailed.accept(
                        "Conversation synchronization FAILED for " + conversation.getSid());
            }
            return;
        }
    }

    /// Generate token and authenticate user #
    public static String generateAccessToken(String accountSid, String apiKey, String apiSecret, String identity,
            String serviceSid, String pushSid) {
        // Reject missing credentials early — apiSecret.getBytes() / Builder /
        // ChatGrant setters would otherwise NPE deeper in the JWT lib, where
        // the exception escapes back through generateAccessToken and the
        // Flutter handler thread, leaving MethodChannel.Result un-invoked
        // and the Dart Future hanging forever.
        if (accountSid == null || apiKey == null || apiSecret == null
                || identity == null || serviceSid == null || pushSid == null) {
            System.err.println("generateAccessToken: missing credential(s); aborting token build");
            return "";
        }
        // Removed a debug System.out.println that dumped apiSecret bytes to
        // logcat — those are trivially decodable back to the signing secret.
        AccessToken.Builder builder = new AccessToken.Builder(accountSid, apiKey, apiSecret.getBytes());
        // Set the identity of the token
        builder.identity(identity);
        // builder.ttl(0);
        builder.ttl(3600);
        // Create a Chat grant and add it to the token
        ChatGrant chatGrant = new ChatGrant();
        chatGrant.setServiceSid(serviceSid);
        chatGrant.setPushCredentialSid(pushSid);
        builder.grant(chatGrant);
        // Build the token
        AccessToken token = builder.build();
        return token.toJwt();
    }

    public static void registerFCMToken(String token, MethodChannel.Result result) {
        if (!isClientInitialized()) {
            result.success("Client not initialized");
            return;
        }

        conversationClient.registerFCMToken(new ConversationsClient.FCMToken(token), new StatusListener() {
            @Override
            public void onSuccess() {
                result.success(Strings.fcmSuccess);

            }

            @Override
            public void onError(ErrorInfo errorInfo) {
                StatusListener.super.onError(errorInfo);
                result.success(Strings.fcmFail);
            }
        });
    }

    public static void unregisterFCMToken(String token, MethodChannel.Result result) {
        if (!isClientInitialized()) {
            result.success("Client not initialized");
            return;
        }

        conversationClient.unregisterFCMToken(new ConversationsClient.FCMToken(token), new StatusListener() {
            @Override
            public void onSuccess() {
                result.success(Strings.fcmUnSuccess);

            }

            @Override
            public void onError(ErrorInfo errorInfo) {
                StatusListener.super.onError(errorInfo);
                result.success(Strings.fcmUnFail);
            }
        });
    }

    /// Create new conversation #
    public static void createConversation(String conversationName, String identity, MethodChannel.Result result) {
        if (!isClientInitialized()) {
            result.success("Client not initialized");
            return;
        }

        conversationClient.createConversation(conversationName, new CallbackListener<Conversation>() {
            @Override
            public void onSuccess(Conversation conversation) {
                // Previously called addParticipant(identity, conversationName, result)
                // and then result.success(sid) — two bugs:
                //   1. result.success fired twice (addParticipant calls it too).
                //   2. addParticipant's 2nd arg is conversationId (sid), but
                //      conversationName (friendly name) was passed, causing
                //      its inner getConversation lookup to fail.
                // Add the creator as a participant directly here and reply
                // exactly once with the new sid.
                final String sid = conversation.getSid();
                conversation.addParticipantByIdentity(identity, null, new StatusListener() {
                    @Override
                    public void onSuccess() {
                        result.success(sid);
                    }

                    @Override
                    public void onError(ErrorInfo errorInfo) {
                        StatusListener.super.onError(errorInfo);
                        // Best-effort: conversation was created, return its
                        // sid even if the participant add failed so callers
                        // can retry the add separately.
                        System.err.println("createConversation: addParticipant failed: " + errorInfo.getMessage());
                        result.success(sid);
                    }
                });
            }

            @Override
            public void onError(ErrorInfo errorInfo) {
                CallbackListener.super.onError(errorInfo);
                // Null-safe equals (was errorInfo.getMessage().equals(...) -> NPE).
                if (Strings.conversationExists.equals(errorInfo.getMessage())) {
                    result.success(Strings.conversationExists);
                } else {
                    result.success(Strings.createConversationFailure);
                }
            }
        });
    }

    /// Add participant in a conversation #
    public static void addParticipant(String participantName, String conversationId, MethodChannel.Result result) {
        if (!isClientInitialized()) {
            result.success("Client not initialized");
            return;
        }

        conversationClient.getConversation(conversationId, new CallbackListener<Conversation>() {
            @Override
            public void onSuccess(Conversation conversation) {
                // Retrieve the conversation object using the conversation SID

                conversation.addParticipantByIdentity(participantName, null, new StatusListener() {
                    @Override
                    public void onSuccess() {
                        result.success(Strings.addParticipantSuccess);
                    }

                    @Override
                    public void onError(ErrorInfo errorInfo) {
                        StatusListener.super.onError(errorInfo);
                        System.out.println(errorInfo.getMessage());
                        result.success(errorInfo.getMessage());
                    }
                });
            }

            @Override
            public void onError(ErrorInfo errorInfo) {
                CallbackListener.super.onError(errorInfo);
                result.success(errorInfo.getMessage());
            }
        });
    }

    /// Remove participant in a conversation #
    public static void removeParticipant(String participantName, String conversationId, MethodChannel.Result result) {
        if (!isClientInitialized()) {
            result.success("Client not initialized");
            return;
        }

        conversationClient.getConversation(conversationId, new CallbackListener<Conversation>() {
            @Override
            public void onSuccess(Conversation conversation) {
                // Retrieve the conversation object using the conversation SID
                System.out.println("admin-" + conversation.getCreatedBy() + "---" + conversationClient.getMyIdentity());

                // if (conversationClient.getMyIdentity().equals(conversation.getCreatedBy())){
                conversation.removeParticipantByIdentity(participantName, new StatusListener() {
                    @Override
                    public void onSuccess() {
                        result.success(Strings.removedParticipantSuccess);
                    }

                    @Override
                    public void onError(ErrorInfo errorInfo) {
                        StatusListener.super.onError(errorInfo);
                        result.success(errorInfo.getMessage());
                    }
                });
                // }
            }

            @Override
            public void onError(ErrorInfo errorInfo) {
                CallbackListener.super.onError(errorInfo);
                result.success(errorInfo.getMessage());
            }
        });
    }

    /// Join the existing conversation #
    // A14: reply from the join callback instead of returning synchronously. The
    // old version returned conversationId immediately and ignored the async join
    // outcome (callbacks were empty no-ops, errors swallowed), so the Dart Future
    // resolved before the join completed — callers then raced ahead to fetch
    // messages on a not-yet-joined conversation. Now the MethodChannel.Result
    // fires only when the join succeeds/fails. Single-fire is guaranteed by the
    // mutually-exclusive callback paths (MainThreadResult also drops any stray
    // double-call).
    public static void joinConversation(String conversationId, MethodChannel.Result result) {
        if (!isClientInitialized()) {
            result.success("Client not initialized");
            return;
        }
        conversationClient.getConversation(conversationId, new CallbackListener<Conversation>() {
            @Override
            public void onSuccess(Conversation conversation) {
                conversation.join(new StatusListener() {
                    @Override
                    public void onSuccess() {
                        result.success(conversationId);
                    }

                    @Override
                    public void onError(ErrorInfo errorInfo) {
                        System.err.println("joinConversation: join failed for "
                                + conversationId + ": " + errorInfo.getMessage());
                        result.success(Strings.failed);
                    }
                });
            }

            @Override
            public void onError(ErrorInfo errorInfo) {
                System.err.println("joinConversation: getConversation failed for "
                        + conversationId + ": " + errorInfo.getMessage());
                result.success(Strings.failed);
            }
        });
    }

    /// Send message #
    public static void sendMessages(String enteredMessage, String conversationId, HashMap attribute,
            MethodChannel.Result result) {
        if (!isClientInitialized()) {
            result.success("Client not initialized");
            return;
        }

        conversationClient.getConversation(conversationId, new CallbackListener<Conversation>() {
            @Override
            public void onSuccess(Conversation conversation) {
                // Join the conversation with the given participant identity
                // attribute may be null when the caller doesn't need to set
                // attributes. Guard against new JSONObject(null) -> NPE and
                // skip setAttributes entirely in that case (matches body()).
                Conversation.MessageBuilder builder = conversation.prepareMessage().setBody(enteredMessage);
                if (attribute != null) {
                    JSONObject jsonObject = new JSONObject(attribute);
                    Attributes attributes = new Attributes(jsonObject);
                    builder.setAttributes(attributes);
                }
                builder.buildAndSend(new CallbackListener() {
                            @Override
                            public void onSuccess(Object data) {
                                if (data instanceof Message) {
                                    Message message = (Message) data;
                                    result.success(message.getSid());
                                } else {
                                    System.out.println("Unexpected data type: " + data.getClass());
                                    result.success("send");
                                }
                            }

                            @Override
                            public void onError(ErrorInfo errorInfo) {
                                System.out.println("messageMap- onError");
                                result.success(errorInfo.getMessage());
                            }
                        });
            }

            @Override
            public void onError(ErrorInfo errorInfo) {
                CallbackListener.super.onError(errorInfo);
                result.success(errorInfo.getMessage());
            }
        });
    }

    /// Update multiple messages #
    public static void updateMessages(String conversationId, List<HashMap<String, Object>> messages,
            MethodChannel.Result result) {
        if (messages == null || messages.isEmpty()) {
            Map<String, Object> responseMap = new HashMap<>();
            responseMap.put("success", new ArrayList<>());
            responseMap.put("errors", new ArrayList<>());
            responseMap.put("totalSuccess", 0);
            responseMap.put("totalErrors", 0);
            result.success(responseMap);
            return;
        }

        conversationClient.getConversation(conversationId, new CallbackListener<Conversation>() {
            @Override
            public void onSuccess(Conversation conversation) {
                // ✅ ดึงข้อความล่าสุดทั้งหมดใน conversation
                runWhenConversationSynchronized(conversation,
                        () -> conversation.getLastMessages(1000, new CallbackListener<List<Message>>() {
                            @Override
                            public void onSuccess(List<Message> messagesList) {
                                // ใช้ CountDownLatch เพื่อรอให้ทุก message อัปเดตเสร็จ
                                CountDownLatch latch = new CountDownLatch(messages.size());
                                // A15: previously plain ArrayList → mutated from
                                // Twilio worker threads (updateBody / setAttributes
                                // callbacks) without synchronization, risking
                                // corrupted internal arrays or lost updates.
                                List<String> successList = Collections.synchronizedList(new ArrayList<>());
                                List<String> errorList = Collections.synchronizedList(new ArrayList<>());
                                // Flipped to true when the awaiter has already replied
                                // (either via successful latch completion or timeout).
                                // Per-message callbacks check this and bail without
                                // touching the lists — so late completions don't
                                // silently corrupt counts on the awaiter's snapshot,
                                // and don't race the codec iteration of those lists.
                                final AtomicBoolean awaiterDone = new AtomicBoolean(false);

                                // วนลูปผ่านแต่ละ message ที่ต้องการอัปเดต
                                for (HashMap<String, Object> messageData : messages) {
                                    try {
                                        // ✅ ปลอดภัยกว่า - ตรวจสอบและแปลง type อย่างระมัดระวัง
                                        Object msgIdObj = messageData.get("msgId");
                                        Object newBodyObj = messageData.get("message");
                                        Object newAttributeObj = messageData.get("attribute");

                                        if (msgIdObj == null || newBodyObj == null) {
                                            errorList.add("Invalid data: msgId or message is null");
                                            latch.countDown();
                                            continue;
                                        }

                                        final String msgId = String.valueOf(msgIdObj);
                                        final String newBody = String.valueOf(newBodyObj);

                                        // 🔍 หา message ที่มี sid ตรงกับ msgId
                                        Message foundMessage = null;
                                        for (Message msg : messagesList) {
                                            if (msg.getSid().equals(msgId)) {
                                                foundMessage = msg;
                                                break;
                                            }
                                        }

                                        if (foundMessage != null) {
                                            final Message targetMessage = foundMessage;

                                            // สร้าง attributes (ถ้ามี)
                                            Attributes finalAttributes = null;
                                            if (newAttributeObj instanceof HashMap) {
                                                try {
                                                    JSONObject jsonObject = new JSONObject(
                                                            (HashMap<String, Object>) newAttributeObj);
                                                    finalAttributes = new Attributes(jsonObject);
                                                } catch (Exception e) {
                                                    System.err.println("Error creating attributes: " + e.getMessage());
                                                }
                                            }

                                            final Attributes attributesToSet = finalAttributes;

                                            // ✅ อัปเดต body ก่อน. Each callback first
                                            // checks awaiterDone — once the awaiter has
                                            // replied (latch completed or timed out), any
                                            // late callback bails without mutating the
                                            // already-snapshotted lists, matching the
                                            // contract documented at the declaration site.
                                            targetMessage.updateBody(newBody, new StatusListener() {
                                                @Override
                                                public void onSuccess() {
                                                    if (awaiterDone.get()) return;
                                                    if (attributesToSet != null) {
                                                        targetMessage.setAttributes(attributesToSet,
                                                                new StatusListener() {
                                                                    @Override
                                                                    public void onSuccess() {
                                                                        if (awaiterDone.get()) return;
                                                                        successList.add(msgId);
                                                                        latch.countDown();
                                                                    }

                                                                    @Override
                                                                    public void onError(ErrorInfo errorInfo) {
                                                                        if (awaiterDone.get()) return;
                                                                        errorList.add(msgId + ": setAttributes error - "
                                                                                + errorInfo.getMessage());
                                                                        latch.countDown();
                                                                    }
                                                                });
                                                    } else {
                                                        // No attributes — already done.
                                                        successList.add(msgId);
                                                        latch.countDown();
                                                    }
                                                }

                                                @Override
                                                public void onError(ErrorInfo errorInfo) {
                                                    if (awaiterDone.get()) return;
                                                    errorList.add(
                                                            msgId + ": updateBody error - " + errorInfo.getMessage());
                                                    latch.countDown();
                                                }
                                            });
                                        } else {
                                            errorList.add(msgId + ": Message not found");
                                            latch.countDown();
                                        }
                                    } catch (Exception e) {
                                        errorList.add("Exception processing message: " + e.getMessage());
                                        latch.countDown();
                                    }
                                }

                                // A15: bounded latch await + main-looper reply.
                                // Previous code used a raw `new Thread(...)` with
                                // `latch.await()` (no timeout) — if any single
                                // updateBody/setAttributes callback was dropped
                                // by Twilio (worker thread died, listener leaked,
                                // etc.) the latch never reached zero and the
                                // Flutter Future hung indefinitely. 30s matches
                                // CONVERSATION_SYNC_TIMEOUT_MS so the user-visible
                                // upper bound is consistent.
                                //
                                // We still spawn a thread because we are inside a
                                // Twilio worker thread here and must not call
                                // latch.await() on it (that would block the
                                // shared worker pool the per-message callbacks
                                // need to fire on, deadlocking the latch).
                                Thread awaiter = new Thread(() -> {
                                    Map<String, Object> responseMap = new HashMap<>();
                                    // Outer Throwable catch ensures the Dart Future
                                    // always resolves — without this, an OOM during
                                    // ArrayList copy, an unexpected RuntimeException in
                                    // responseMap.put, or any other unchecked failure
                                    // would kill the awaiter thread silently and the
                                    // Future would hang forever. That's precisely the
                                    // A15 failure mode the timeout was meant to fix.
                                    try {
                                        boolean completed;
                                        try {
                                            completed = latch.await(
                                                    UPDATE_MESSAGES_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                                        } catch (InterruptedException ie) {
                                            Thread.currentThread().interrupt();
                                            awaiterDone.set(true);
                                            responseMap.clear();
                                            responseMap.put("error", "Thread interrupted: " + ie.getMessage());
                                            result.success(responseMap);
                                            return;
                                        }
                                        // Defensive snapshot under each list's own
                                        // monitor — Collections.synchronizedList
                                        // requires the caller to hold its monitor for
                                        // any iteration, including the iterator() call
                                        // the ArrayList copy constructor performs. Set
                                        // awaiterDone BEFORE the snapshot so any
                                        // per-message callback that wakes up between
                                        // the latch release and our snapshot still sees
                                        // awaiterDone=true and bails (it would only add
                                        // entries that don't make it into the snapshot
                                        // anyway, but doing the redundant work is
                                        // wasteful and the bail is cheap).
                                        awaiterDone.set(true);
                                        List<String> successSnapshot;
                                        List<String> errorSnapshot;
                                        synchronized (successList) {
                                            successSnapshot = new ArrayList<>(successList);
                                        }
                                        synchronized (errorList) {
                                            errorSnapshot = new ArrayList<>(errorList);
                                        }
                                        responseMap.put("success", successSnapshot);
                                        responseMap.put("errors", errorSnapshot);
                                        // totalSuccess / totalErrors reflect REAL per-message
                                        // outcomes only — the synthetic timeout marker that
                                        // the previous implementation added to errorList
                                        // would otherwise inflate totalErrors by 1 on every
                                        // timeout, misleading the Dart caller into thinking
                                        // an extra message failed. The timeout signal lives
                                        // in its own keys.
                                        responseMap.put("totalSuccess", successSnapshot.size());
                                        responseMap.put("totalErrors", errorSnapshot.size());
                                        responseMap.put("timedOut", !completed);
                                        if (!completed) {
                                            responseMap.put(
                                                    "timeoutReason",
                                                    "updateMessages timed out after "
                                                            + UPDATE_MESSAGES_TIMEOUT_MS
                                                            + "ms — partial results returned");
                                        }
                                        // No inner Handler.post — MainThreadResult (the
                                        // wrapping at TwilioConversationSdkPlugin.onMethodCall)
                                        // already main-thread-dispatches every reply.
                                        result.success(responseMap);
                                    } catch (Throwable t) {
                                        // Last-ditch: avoid leaving the Dart Future
                                        // hanging on any unchecked failure (OOM,
                                        // unencodable map value found later by
                                        // MainThreadResult — though that's caught in
                                        // the wrapper too, etc.).
                                        System.err.println(
                                                "updateMessages awaiter threw: " + t);
                                        awaiterDone.set(true);
                                        try {
                                            result.error(
                                                    "UPDATE_MESSAGES_AWAITER_DIED",
                                                    t.getClass().getSimpleName() + ": " + t.getMessage(),
                                                    null);
                                        } catch (Throwable t2) {
                                            System.err.println(
                                                    "updateMessages awaiter fallback error also threw: " + t2);
                                        }
                                    }
                                }, "twilio-updateMessages-await");
                                awaiter.setDaemon(true);
                                awaiter.start();
                            }

                            @Override
                            public void onError(ErrorInfo errorInfo) {
                                // MainThreadResult already dispatches on main looper.
                                Map<String, Object> errorResponse = new HashMap<>();
                                errorResponse.put("error", "getLastMessages error: " + errorInfo.getMessage());
                                result.success(errorResponse);
                            }
                        }),
                        errMsg -> {
                            Map<String, Object> errorResponse = new HashMap<>();
                            errorResponse.put("error", "Sync error: " + errMsg);
                            result.success(errorResponse);
                        });
            }

            @Override
            public void onError(ErrorInfo errorInfo) {
                Map<String, Object> errorResponse = new HashMap<>();
                errorResponse.put("error", "getConversation error: " + errorInfo.getMessage());
                result.success(errorResponse);
            }
        });
    }

    /// Update message #
    public static void body(String enteredMessage, String conversationId, String msgId, HashMap attribute,
            MethodChannel.Result result) {
        if (!isClientInitialized()) {
            result.success("Client not initialized");
            return;
        }
        conversationClient.getConversation(conversationId, new CallbackListener<Conversation>() {
            @Override
            public void onSuccess(Conversation conversation) {

                // ✅ ดึงข้อความล่าสุดทั้งหมดใน conversation
                runWhenConversationSynchronized(conversation,
                        () -> conversation.getLastMessages(1000, new CallbackListener<List<Message>>() {
                            @Override
                            public void onSuccess(List<Message> messages) {
                                Message foundMessage = null;

                                // 🔍 หา message ที่มี sid ตรงกับ messageId
                                for (Message msg : messages) {
                                    if (msg.getSid().equals(msgId)) {
                                        foundMessage = msg;
                                        break;
                                    }
                                }

                                if (foundMessage != null) {
                                    final Message targetMessage = foundMessage;
                                    // attribute may be null when the caller only wants to update
                                    // the body. Guard against new JSONObject(null) -> NPE and skip
                                    // setAttributes entirely in that case (matches updateMessages).
                                    final Attributes finalAttributes;
                                    if (attribute != null) {
                                        JSONObject jsonObject = new JSONObject(attribute);
                                        finalAttributes = new Attributes(jsonObject);
                                    } else {
                                        finalAttributes = null;
                                    }

                                    // ✅ อัปเดต body ก่อน
                                    targetMessage.updateBody(enteredMessage, new StatusListener() {
                                        @Override
                                        public void onSuccess() {
                                            if (finalAttributes == null) {
                                                result.success("Success");
                                                return;
                                            }
                                            // ✅ จากนั้นอัปเดต attributes ต่อ
                                            targetMessage.setAttributes(finalAttributes, new StatusListener() {
                                                @Override
                                                public void onSuccess() {
                                                    result.success("Success");
                                                }

                                                @Override
                                                public void onError(ErrorInfo errorInfo) {
                                                    result.success("setAttributes error: " + errorInfo.getMessage());
                                                }
                                            });
                                        }

                                        @Override
                                        public void onError(ErrorInfo errorInfo) {
                                            result.success("updateBody error: " + errorInfo.getMessage());
                                        }
                                    });
                                } else {
                                    // ❌ ถ้าไม่เจอ messageId ที่ระบุ
                                    result.success("Message not found for ID: " + msgId);
                                }
                            }

                            @Override
                            public void onError(ErrorInfo errorInfo) {
                                result.success("getLastMessages error: " + errorInfo.getMessage());
                            }
                        }),
                        errMsg -> result.success("Sync error: " + errMsg));
            }

            @Override
            public void onError(ErrorInfo errorInfo) {
                result.success("getConversation error: " + errorInfo.getMessage());
            }
        });
    }

    /// Send message #
    public static void sendMessageWithMedia(String enteredMessage, String conversationId, HashMap attribute,
            String mediaFilePath, String mimeType, String fileName, MethodChannel.Result result) {
        if (!isClientInitialized()) {
            result.success("Client not initialized");
            return;
        }
        // Fetch the conversation using the conversationId
        // Single-shot reply gate: ensures result.success fires exactly once
        // across every async/sync path below — MediaUploadListener.onFailed,
        // buildAndSend.onSuccess, buildAndSend.onError, the catch block, and
        // the outer getConversation.onError (A4 + A5 fix). Previously the
        // error branches only fired triggerEvent and the Dart Future hung.
        final AtomicBoolean resultDelivered = new AtomicBoolean(false);
        conversationClient.getConversation(conversationId, new CallbackListener<Conversation>() {
            @Override
            public void onSuccess(Conversation conversation) {
                final AtomicBoolean mediaUploadFailed = new AtomicBoolean(false);
                // A16: hold the InputStream so we can close it from the
                // terminal MediaUploadListener callbacks (onCompleted /
                // onFailed). Previously openInputStream(File) was passed
                // straight to addMedia and never explicitly closed → fd leak
                // on every media send. We cannot try-with-resources because
                // Twilio reads the stream asynchronously after addMedia
                // returns.
                final AtomicReference<InputStream> mediaStreamRef = new AtomicReference<>();
                // Tracks whether addMedia has been called: once it has, the SDK
                // owns the stream and may still be reading from it on a worker
                // thread, so neither the catch block nor buildAndSend.onSuccess
                // is safe to close the stream — only the MediaUploadListener's
                // terminal callbacks know when the SDK is done. Before addMedia
                // is called (e.g. an exception thrown by openInputStream or
                // setAttributes), the stream still belongs to us and the catch
                // is the right place to close it.
                final AtomicBoolean addMediaCalled = new AtomicBoolean(false);
                final Runnable closeMediaStream = () -> {
                    InputStream s = mediaStreamRef.getAndSet(null);
                    if (s != null) {
                        try {
                            s.close();
                        } catch (IOException ioe) {
                            System.err.println("sendMessageWithMedia: stream close threw: " + ioe);
                        }
                    }
                };
                try {
                    System.out.println("enteredMessage:" + enteredMessage);
                    System.out.println("conversationId:" + conversationId);
                    System.out.println("MediaFile:" + mediaFilePath);
                    System.out.println("MediaType" + mimeType);
                    System.out.println("MediaName" + fileName);
                    // Prepare the message with media. `attribute` may be null
                    // when the caller doesn't need to set attributes — skip
                    // setAttributes in that case (matches body() / sendMessages).
                    final Attributes attributes;
                    if (attribute != null) {
                        attributes = new Attributes(new JSONObject(attribute));
                    } else {
                        attributes = null;
                    }
                    InputStream fileInputStream = null;
                    if (mediaFilePath != null) {
                        fileInputStream = openInputStream(new File(mediaFilePath));
                    }
                    if (fileInputStream == null) {
                        // Replaces release-build-stripped `assert`. Without a
                        // media stream the SDK call would NPE / never resolve.
                        System.err.println("sendMessageWithMedia: missing or unreadable media file: " + mediaFilePath);
                        HashMap<String, Object> progressData = new HashMap<>();
                        progressData.put("messageStatus", Strings.failed);
                        triggerEvent(progressData);
                        if (resultDelivered.compareAndSet(false, true)) {
                            result.success(Strings.failed);
                        }
                        return;
                    }
                    mediaStreamRef.set(fileInputStream);
                    Conversation.MessageBuilder builder = conversation.prepareMessage().setBody(enteredMessage);
                    if (attributes != null) {
                        builder.setAttributes(attributes);
                    }
                    addMediaCalled.set(true);
                    builder.addMedia(fileInputStream, mimeType, fileName, new MediaUploadListener() {
                                @Override
                                public void onStarted() {
                                    System.out.println("Media onStarted:");
                                }

                                @Override
                                public void onProgress(long bytesSent) {
                                    System.out.println("Media upload progress: " + bytesSent);
                                    HashMap<String, Object> progressData = new HashMap<>();
                                    progressData.put("bytesSent", bytesSent);
                                    triggerEvent(progressData);
                                }

                                @Override
                                public void onCompleted(@NonNull String mediaSid) {
                                    System.out.println("Media uploaded successfully with SID: " + mediaSid);
                                    // A16: terminal success — stream no longer
                                    // needed by Twilio.
                                    closeMediaStream.run();
                                    HashMap<String, Object> progressData = new HashMap<>();
                                    progressData.put("mediaStatus", "Completed");
                                    triggerEvent(progressData);
                                }

                                @Override
                                public void onFailed(@NonNull ErrorInfo errorInfo) {
                                    // Handle media upload failure
                                    System.err.println("Media upload failed:" + errorInfo.getMessage());
                                    // A16: terminal failure — release fd.
                                    closeMediaStream.run();
                                    mediaUploadFailed.set(true);
                                    HashMap<String, Object> progressData = new HashMap<>();
                                    progressData.put("mediaStatus", Strings.failed);
                                    triggerEvent(progressData);
                                    if (resultDelivered.compareAndSet(false, true)) {
                                        result.success(Strings.failed);
                                    }
                                }
                            }).buildAndSend(new CallbackListener() {
                                @Override
                                public void onSuccess(Object data) {
                                    // buildAndSend completes when the message envelope
                                    // is server-acked; if media upload failed earlier
                                    // (onFailed already fired), don't report success.
                                    //
                                    // Do NOT close the InputStream here. Twilio does not
                                    // contractually guarantee buildAndSend.onSuccess
                                    // fires AFTER MediaUploadListener.onCompleted —
                                    // closing here while the media worker is still
                                    // pumping bytes would truncate the upload silently,
                                    // and Dart would receive a success reply for a
                                    // corrupted message. Stream closure is owned by the
                                    // MediaUploadListener's terminal callbacks
                                    // (onCompleted / onFailed) which fire exactly when
                                    // the stream is no longer needed.
                                    System.out.println("Message sent successfully!");
                                    if (mediaUploadFailed.get()) {
                                        return;
                                    }
                                    String reply;
                                    if (data instanceof Message) {
                                        reply = ((Message) data).getSid();
                                    } else {
                                        reply = "send";
                                    }
                                    if (resultDelivered.compareAndSet(false, true)) {
                                        result.success(reply);
                                    }
                                }

                                @Override
                                public void onError(ErrorInfo errorInfo) {
                                    // Handle message send error. Twilio may reject
                                    // the message envelope BEFORE the media upload
                                    // starts — in which case MediaUploadListener
                                    // never fires its terminal callback and the
                                    // stream would leak. closeMediaStream is
                                    // idempotent (AtomicReference.getAndSet), so
                                    // calling it here is safe even if onFailed
                                    // already fired and closed the stream.
                                    System.err.println("Error sending message: " + errorInfo.getMessage());
                                    closeMediaStream.run();
                                    HashMap<String, Object> progressData = new HashMap<>();
                                    progressData.put("messageStatus", Strings.failed);
                                    triggerEvent(progressData);
                                    if (resultDelivered.compareAndSet(false, true)) {
                                        result.success(Strings.failed);
                                    }
                                }
                            });
                } catch (Exception e) {
                    // Handle exceptions (e.g., JSONException, FileNotFoundException)
                    System.err.println("Error preparing message: " + e.getMessage());
                    // A16: only close the stream if Twilio has NOT yet taken
                    // ownership via addMedia — once addMedia has returned, the
                    // SDK may still be reading from the stream on a worker
                    // thread (the exception we're handling could have come
                    // from buildAndSend, which runs AFTER addMedia). Closing
                    // mid-read would truncate the upload silently while the
                    // MediaUploadListener still fires onCompleted reporting
                    // success.
                    if (!addMediaCalled.get()) {
                        closeMediaStream.run();
                    }
                    HashMap<String, Object> progressData = new HashMap<>();
                    progressData.put("messageStatus", Strings.failed);
                    triggerEvent(progressData);
                    if (resultDelivered.compareAndSet(false, true)) {
                        result.success(Strings.failed);
                    }
                }
            }

            @Override
            public void onError(ErrorInfo errorInfo) {
                // Handle error in fetching conversation
                System.err.println("Error fetching conversation: " + errorInfo.getMessage());
                HashMap<String, Object> progressData = new HashMap<>();
                progressData.put("messageStatus", Strings.failed);
                triggerEvent(progressData);
                if (resultDelivered.compareAndSet(false, true)) {
                    result.success(Strings.failed);
                }
            }
        });
    }

    /// Subscribe To Message Update #
    public static void subscribeToMessageUpdate(String conversationId) {
        conversationClient.getConversation(conversationId, new CallbackListener<Conversation>() {
            @Override
            public void onSuccess(Conversation result) {
                // A10: if a previous subscription on this conversation is still
                // attached, detach it before adding a new one. Re-subscribing
                // would otherwise stack listeners and fire onMessageAdded N
                // times for one inbound message.
                final String sid = result.getSid();
                final ConversationListener previous = activeMessageListeners.get(sid);
                if (previous != null) {
                    try {
                        result.removeListener(previous);
                        // Only drop the map entry AFTER a successful removal.
                        // If removeListener threw, the previous listener is
                        // still attached on the conversation; we must keep
                        // the map entry so the next subscribe (or
                        // unSubscribeToMessageUpdate) can retry the removal.
                        // Re-attaching a new listener now would create
                        // duplicates — exactly what A10 was meant to prevent.
                        activeMessageListeners.remove(sid);
                    } catch (RuntimeException e) {
                        System.err.println(
                                "subscribeToMessageUpdate: removeListener (previous) threw for "
                                        + sid + "; aborting to avoid duplicate listener: " + e);
                        return;
                    }
                }

                // Build the new listener, register it, then remember it so
                // unSubscribeToMessageUpdate can detach it specifically
                // (instead of removeAllListeners(), which would also kill
                // listeners belonging to in-flight runWhenConversationSynchronized
                // calls and make their getMessages/etc. time out).
                ConversationListener listener = new ConversationListener() {
                    @Override
                    public void onMessageAdded(Message message) {
                        // new code for attach media check
                        try {
                            final Map<String, Object> messageMap = new HashMap<>();
                            messageMap.put("sid", message.getSid());
                            messageMap.put("author", message.getAuthor());
                            messageMap.put("body", message.getBody());
                            messageMap.put("attributes", message.getAttributes().toString());
                            messageMap.put("dateCreated", message.getDateCreated());
                            messageMap.put("conversationSid", result.getSid());

                            // A20: synchronizedList so concurrent URL callbacks add safely.
                            final List<Map<String, Object>> mediaList =
                                    Collections.synchronizedList(new ArrayList<>());
                            // A7-analog: pre-arm at 1 + single-fire AtomicBoolean so a
                            // cached/sync getTemporaryContentUrl callback firing inside
                            // the loop cannot drive the counter to 0 mid-build and emit
                            // multiple partial-state triggerEvent calls. The post-loop
                            // decrement releases the pre-arm.
                            final int[] pendingMediaCount = { 1 };
                            final AtomicBoolean delivered = new AtomicBoolean(false);
                            final Runnable maybeDeliver = () -> {
                                if (delivered.compareAndSet(false, true)) {
                                    // Deep-snapshot mediaList — the live
                                    // synchronizedList may still be mutated
                                    // by a spurious late getTemporaryContentUrl
                                    // callback after `delivered` flipped. The
                                    // codec on the platform thread iterates
                                    // this without acquiring the monitor; a
                                    // late add would CME. Match the deep-copy
                                    // policy used in getAllMessages.replyWithList.
                                    List<Map<String, Object>> snapshot;
                                    synchronized (mediaList) {
                                        snapshot = new ArrayList<>(mediaList);
                                    }
                                    messageMap.put("attachMedia", snapshot);
                                    triggerEvent(messageMap);
                                }
                            };

                            for (Media media : message.getAttachedMedia()) {
                                final Map<String, Object> mediaMap = new HashMap<>();
                                mediaMap.put("sid", media.getSid());
                                mediaMap.put("contentType", media.getContentType());
                                mediaMap.put("filename", media.getFilename());

                                synchronized (pendingMediaCount) {
                                    pendingMediaCount[0]++;
                                }

                                media.getTemporaryContentUrl(new CallbackListener<String>() {
                                    @Override
                                    public void onSuccess(String mediaUrl) {
                                        mediaMap.put("mediaUrl", mediaUrl);
                                        // A20: mediaList.add + counter decrement in
                                        // one critical section. Capture the
                                        // "should-deliver" decision INSIDE the lock
                                        // but call maybeDeliver OUTSIDE — the
                                        // delivery path posts to the main looper /
                                        // may run inline and we don't want to hold
                                        // the pendingMediaCount monitor while doing
                                        // unbounded work.
                                        boolean shouldDeliver;
                                        synchronized (pendingMediaCount) {
                                            mediaList.add(mediaMap);
                                            pendingMediaCount[0]--;
                                            shouldDeliver = pendingMediaCount[0] == 0;
                                        }
                                        if (shouldDeliver) maybeDeliver.run();
                                    }

                                    @Override
                                    public void onError(ErrorInfo errorInfo) {
                                        System.err.println("Error retrieving media URL: " + errorInfo.getMessage());
                                        boolean shouldDeliver;
                                        synchronized (pendingMediaCount) {
                                            pendingMediaCount[0]--;
                                            shouldDeliver = pendingMediaCount[0] == 0;
                                        }
                                        if (shouldDeliver) maybeDeliver.run();
                                    }
                                });
                            }

                            // Release the +1 pre-arm. If every media URL has already
                            // resolved synchronously this is the trigger that delivers
                            // the event; otherwise the last pending callback fires it.
                            boolean shouldDeliver;
                            synchronized (pendingMediaCount) {
                                pendingMediaCount[0]--;
                                shouldDeliver = pendingMediaCount[0] == 0;
                            }
                            if (shouldDeliver) maybeDeliver.run();

                            // Update the last read message index
                            result.setLastReadMessageIndex(result.getLastMessageIndex() + 1,
                                    new CallbackListener<Long>() {
                                        @Override
                                        public void onSuccess(Long result) {
                                            System.out.println("LastReadMessageIndex- " + result);
                                        }

                                        @Override
                                        public void onError(ErrorInfo errorInfo) {
                                            System.err.println(
                                                    "setLastReadMessageIndex (onMessageAdded) failed: "
                                                            + errorInfo.getMessage());
                                        }
                                    });

                        } catch (Exception e) {
                            System.err.println("Exception: " + e.getMessage());
                            HashMap<String, Object> progressData = new HashMap<>();
                            progressData.put("messageStatus", Strings.failed);
                            triggerEvent(progressData);
                        }
                    }

                    @Override
                    public void onMessageUpdated(Message message, Message.UpdateReason reason) {
                        System.out.println("onMessageUpdated->" + message.toString());
                        System.out.println("reason->" + reason.toString());
                        try {
                            final Map<String, Object> messageMap = new HashMap<>();
                            messageMap.put("sid", message.getSid());
                            messageMap.put("author", message.getAuthor());
                            messageMap.put("body", message.getBody());
                            messageMap.put("attributes", message.getAttributes().toString());
                            messageMap.put("dateCreated", message.getDateCreated());
                            messageMap.put("conversationSid", result.getSid());

                            // A7-analog + A20: same pattern as onMessageAdded
                            // (pre-arm counter + AtomicBoolean single-fire +
                            // mediaList.add inside the synchronized block +
                            // deep-snapshot mediaList in maybeDeliver to
                            // avoid CME on a late callback).
                            final List<Map<String, Object>> mediaList =
                                    Collections.synchronizedList(new ArrayList<>());
                            final int[] pendingMediaCount = { 1 };
                            final AtomicBoolean delivered = new AtomicBoolean(false);
                            final Runnable maybeDeliver = () -> {
                                if (delivered.compareAndSet(false, true)) {
                                    List<Map<String, Object>> snapshot;
                                    synchronized (mediaList) {
                                        snapshot = new ArrayList<>(mediaList);
                                    }
                                    messageMap.put("attachMedia", snapshot);
                                    triggerEvent(messageMap);
                                }
                            };

                            for (Media media : message.getAttachedMedia()) {
                                final Map<String, Object> mediaMap = new HashMap<>();
                                mediaMap.put("sid", media.getSid());
                                mediaMap.put("contentType", media.getContentType());
                                mediaMap.put("filename", media.getFilename());

                                synchronized (pendingMediaCount) {
                                    pendingMediaCount[0]++;
                                }

                                media.getTemporaryContentUrl(new CallbackListener<String>() {
                                    @Override
                                    public void onSuccess(String mediaUrl) {
                                        mediaMap.put("mediaUrl", mediaUrl);
                                        boolean shouldDeliver;
                                        synchronized (pendingMediaCount) {
                                            mediaList.add(mediaMap);
                                            pendingMediaCount[0]--;
                                            shouldDeliver = pendingMediaCount[0] == 0;
                                        }
                                        if (shouldDeliver) maybeDeliver.run();
                                    }

                                    @Override
                                    public void onError(ErrorInfo errorInfo) {
                                        System.err.println("Error retrieving media URL: " + errorInfo.getMessage());
                                        boolean shouldDeliver;
                                        synchronized (pendingMediaCount) {
                                            pendingMediaCount[0]--;
                                            shouldDeliver = pendingMediaCount[0] == 0;
                                        }
                                        if (shouldDeliver) maybeDeliver.run();
                                    }
                                });
                            }

                            // Release the +1 pre-arm.
                            boolean shouldDeliver;
                            synchronized (pendingMediaCount) {
                                pendingMediaCount[0]--;
                                shouldDeliver = pendingMediaCount[0] == 0;
                            }
                            if (shouldDeliver) maybeDeliver.run();

                            // Update the last read message index
                            result.setLastReadMessageIndex(result.getLastMessageIndex() + 1,
                                    new CallbackListener<Long>() {
                                        @Override
                                        public void onSuccess(Long result) {
                                            System.out.println("LastReadMessageIndex- " + result);
                                        }

                                        @Override
                                        public void onError(ErrorInfo errorInfo) {
                                            System.err.println(
                                                    "setLastReadMessageIndex (onMessageUpdated) failed: "
                                                            + errorInfo.getMessage());
                                        }
                                    });

                        } catch (Exception e) {
                            System.err.println("Exception: " + e.getMessage());
                            HashMap<String, Object> progressData = new HashMap<>();
                            progressData.put("messageStatus", Strings.failed);
                            triggerEvent(progressData);
                        }
                    }

                    @Override
                    public void onMessageDeleted(Message message) {
                        System.out.println("onMessageDeleted->" + message.getBody());
                    }

                    @Override
                    public void onParticipantAdded(Participant participant) {
                    }

                    @Override
                    public void onParticipantUpdated(Participant participant, Participant.UpdateReason reason) {
                    }

                    @Override
                    public void onParticipantDeleted(Participant participant) {
                    }

                    @Override
                    public void onTypingStarted(Conversation conversation, Participant participant) {
                        System.out.println("onTypingStarted->" + participant.getIdentity());
                        Map<String, Object> typingMap = new HashMap<>();
                        typingMap.put("typingStatus", true);
                        typingMap.put("identity", participant.getIdentity());
                        typingMap.put("conversationSid", conversation.getSid());
                        triggerEvent(typingMap);
                    }

                    @Override
                    public void onTypingEnded(Conversation conversation, Participant participant) {
                        System.out.println("onTypingEnded->" + participant.getIdentity());
                        Map<String, Object> typingMap = new HashMap<>();
                        typingMap.put("typingStatus", false);
                        typingMap.put("identity", participant.getIdentity());
                        typingMap.put("conversationSid", conversation.getSid());
                        triggerEvent(typingMap);
                    }

                    @Override
                    public void onSynchronizationChanged(Conversation conversation) {
                        System.out.println("conversation onSynchronizationChanged->"
                                + conversation.getSynchronizationStatus().toString() + ": "
                                + conversation.getSynchronizationStatus().getValue());
                        if (messageInterface != null) {
                            Map<String, Object> syncMap = new HashMap<>();
                            syncMap.put("status", conversation.getSynchronizationStatus().getValue());
                            messageInterface.onSynchronizationChanged(syncMap);
                        }
                    }
                };
                boolean attached = false;
                try {
                    result.addListener(listener);
                    attached = true;
                    activeMessageListeners.put(sid, listener);
                } catch (RuntimeException e) {
                    System.err.println("subscribeToMessageUpdate: addListener threw for "
                            + sid + ": " + e);
                    // Best-effort cleanup: Twilio may have partially registered the
                    // listener before throwing. removeListener is idempotent (Twilio's
                    // implementation no-ops if the listener isn't found), so this is
                    // safe whether the listener was actually attached or not, and
                    // prevents an orphaned listener from firing into Dart forever.
                    try {
                        result.removeListener(listener);
                    } catch (RuntimeException re) {
                        System.err.println(
                                "subscribeToMessageUpdate: cleanup removeListener threw for "
                                        + sid + ": " + re);
                    }
                }
                if (!attached) {
                    // Make sure no stale entry remains keyed by sid.
                    activeMessageListeners.remove(sid);
                }
            }

            @Override
            public void onError(ErrorInfo errorInfo) {
                // System.out.println("client12-" +
                // errorInfo.getStatus()+"-"+errorInfo.getCode()+"-"+errorInfo.getMessage()+"-"+errorInfo.getDescription()+"-"+errorInfo.getReason());
                CallbackListener.super.onError(errorInfo);
            }
        });
    }

    public static void setTypingStatus(String conversationId, boolean isTyping, MethodChannel.Result result) {
        conversationClient.getConversation(conversationId, new CallbackListener<Conversation>() {
            @Override
            public void onSuccess(Conversation conversation) {
                if (isTyping) {
                    // ✅ เริ่มพิมพ์
                    conversation.typing();
                    System.out.println("Typing started for conversationId: " + conversationId);
                    result.success("started");
                } else {
                    // ✅ หยุดพิมพ์ (Twilio จะหมดอายุ typing เองภายใน ~5 วินาที)
                    // ที่นี่เราเพียงส่ง signal log หรือ event เฉย ๆ
                    System.out.println("Typing ended for conversationId: " + conversationId);
                    result.success("ended");
                }
            }

            @Override
            public void onError(ErrorInfo errorInfo) {
                System.out.println("setTypingStatus error: " + errorInfo.getMessage());
                result.success("setTypingStatus error: " + errorInfo.getMessage());
            }
        });
    }

    /// Unsubscribe To Message Update #
    public static void unSubscribeToMessageUpdate(String conversationId) {
        conversationClient.getConversation(conversationId, new CallbackListener<Conversation>() {
            @Override
            public void onSuccess(Conversation result) {
                // A10: remove only the listener that subscribeToMessageUpdate
                // installed for this conversation. The previous code called
                // removeAllListeners(), which also detached listeners owned by
                // in-flight runWhenConversationSynchronized calls (body /
                // updateMessages / sendMessageWithMedia / etc.), causing those
                // callers to time out instead of complete.
                final String sid = result.getSid();
                final ConversationListener listener = activeMessageListeners.get(sid);
                if (listener != null) {
                    try {
                        result.removeListener(listener);
                        // Symmetric with subscribeToMessageUpdate: only drop
                        // the map entry AFTER a successful removeListener.
                        // If removeListener threw, the listener is still
                        // attached on Twilio's side — keeping the map entry
                        // lets a subsequent unSubscribeToMessageUpdate (or
                        // subscribe's pre-attach cleanup) retry the removal.
                        // Dropping the entry preemptively would orphan the
                        // listener: no future call could find and remove it.
                        activeMessageListeners.remove(sid);
                    } catch (RuntimeException e) {
                        System.err.println("unSubscribeToMessageUpdate: removeListener threw for "
                                + sid + "; leaving map entry for retry: " + e);
                    }
                }
            }

            @Override
            public void onError(ErrorInfo errorInfo) {
                // System.out.println("client12-" +
                // errorInfo.getStatus()+"-"+errorInfo.getCode()+"-"+errorInfo.getMessage()+"-"+errorInfo.getDescription()+"-"+errorInfo.getReason());
                CallbackListener.super.onError(errorInfo);
            }
        });
    }

    /// Get list of conversations for logged in user #
    public static List<Map<String, Object>> getConversationsList() {
        List<Conversation> conversationList = conversationClient.getMyConversations();
        // System.out.println(conversationList.size()+"");
        List<Map<String, Object>> list = new ArrayList<>();
        for (int i = 0; i < conversationList.size(); i++) {
            Map<String, Object> conversationMap = new HashMap<>();

            conversationMap.put("sid", conversationList.get(i).getSid());
            conversationMap.put("conversationName", conversationList.get(i).getFriendlyName());
            conversationMap.put("createdBy", conversationList.get(i).getCreatedBy());
            conversationMap.put("dateCreated", conversationList.get(i).getDateCreated());
            conversationMap.put("uniqueName", conversationList.get(i).getUniqueName());
            conversationMap.put("lastReadIndex", conversationList.get(i).getLastReadMessageIndex());
            conversationMap.put("lastMessageIndex", conversationList.get(i).getLastMessageIndex());
            conversationMap.put("participantsCount", conversationList.get(i).getParticipantsList().size());
            conversationMap.put("isGroup", conversationList.get(i).getParticipantsList().size() > 2);
            if (conversationList.get(i).getLastMessageDate() != null) {
                SimpleDateFormat inputFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy", Locale.ENGLISH);
                SimpleDateFormat outputFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss Z", Locale.ENGLISH);
                outputFormat.setTimeZone(TimeZone.getTimeZone("UTC")); // Convert to UTC
                try {
                    Date date = inputFormat.parse(conversationList.get(i).getLastMessageDate().toString());
                    String outputDateStr = outputFormat.format(date);
                    conversationMap.put("lastMessageDate", outputDateStr);
                    System.out.println("lastMessageDateTime->" + outputDateStr);
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            }

            if (conversationList.get(i).getFriendlyName() != null
                    && !conversationList.get(i).getFriendlyName().trim().isEmpty()) {
                list.add(conversationMap);
            }
        }
        System.out.println("getMyConversations----->" + list);
        return list;
    }

    public static void getLastMessages(String conversationId, MethodChannel.Result result) {
        if (!isClientInitialized()) {
            result.success("Client not initialized");
            return;
        }

        // A9: Collections.synchronizedList — the entries are added on whichever
        // thread Twilio's callback fires on (which is not always the main
        // thread for cached objects), and we read it from the maybeReply path.
        List<Map<String, Object>> list = Collections.synchronizedList(new ArrayList<>());
        // A9: single-fire guard so any combination of paths (sync onError +
        // late onSuccess, double-fired upstream callback, …) replies at most
        // once — Flutter throws "Reply already submitted" otherwise.
        final AtomicBoolean replied = new AtomicBoolean(false);
        conversationClient.getConversation(conversationId, new CallbackListener<Conversation>() {
            @Override
            public void onSuccess(Conversation conversation) {
                AtomicInteger pendingCallbacks = new AtomicInteger(1); // Track pending callbacks
                Map<String, Object> conversationMap = new HashMap<>();

                runWhenConversationSynchronized(conversation,
                        () -> conversation.getLastMessages(1, new CallbackListener<List<Message>>() {
                            @Override
                            public void onSuccess(List<Message> messages) {
                                if (!messages.isEmpty()) {
                                    Message lastMessage = messages.get(0);
                                    conversationMap.put("sid", conversationId);
                                    conversationMap.put("lastMessage", lastMessage.getBody());
                                    conversationMap.put("attributes", lastMessage.getAttributes().toString());
                                    conversationMap.put("mediaCount", lastMessage.getAttachedMedia().size());
                                    conversationMap.put("participantsCount", conversation.getParticipantsList().size());
                                    conversationMap.put("isGroup", conversation.getParticipantsList().size() > 2);
                                    conversationMap.put("lastReadIndex", conversation.getLastReadMessageIndex());
                                    conversationMap.put("lastMessageIndex", conversation.getLastMessageIndex());

                                    if (conversation.getLastMessageDate() != null) {
                                        SimpleDateFormat inputFormat = new SimpleDateFormat(
                                                "EEE MMM dd HH:mm:ss z yyyy", Locale.ENGLISH);
                                        SimpleDateFormat outputFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss Z",
                                                Locale.ENGLISH);
                                        outputFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
                                        try {
                                            Date date = inputFormat.parse(conversation.getLastMessageDate().toString());
                                            String outputDateStr = outputFormat.format(date);
                                            conversationMap.put("lastMessageDate", outputDateStr);
                                        } catch (ParseException e) {
                                            e.printStackTrace();
                                        }
                                    }

                                    // A9: add conversationMap to the result list
                                    // BEFORE dispatching getAndSubscribeUser.
                                    // Previously the add happened after the
                                    // async call; if Twilio resolved the user
                                    // synchronously (cached) the callback's
                                    // decrementAndGet could reach 0 while the
                                    // list was still empty, returning [] to
                                    // Flutter.
                                    list.add(conversationMap);

                                    Participant participant = lastMessage.getParticipant();
                                    if (participant != null) { // Added null check here
                                        pendingCallbacks.incrementAndGet();
                                        participant.getAndSubscribeUser(new CallbackListener<User>() {
                                            @Override
                                            public void onSuccess(User user) {
                                                conversationMap.put("friendlyIdentity", user.getIdentity());
                                                conversationMap.put("friendlyName", user.getFriendlyName());
                                                if (pendingCallbacks.decrementAndGet() == 0
                                                        && replied.compareAndSet(false, true)) {
                                                    result.success(list);
                                                }
                                            }

                                            @Override
                                            public void onError(ErrorInfo errorInfo) {
                                                // Without this override the default CallbackListener.onError
                                                // just logs, pendingCallbacks never decrements, and the
                                                // Flutter Future hangs forever.
                                                System.err.println(
                                                        "getAndSubscribeUser failed: " + errorInfo.getMessage());
                                                if (pendingCallbacks.decrementAndGet() == 0
                                                        && replied.compareAndSet(false, true)) {
                                                    result.success(list);
                                                }
                                            }
                                        });
                                    }
                                }
                                if (pendingCallbacks.decrementAndGet() == 0
                                        && replied.compareAndSet(false, true)) {
                                    result.success(list);
                                }
                            }

                            @Override
                            public void onError(ErrorInfo errorInfo) {
                                System.out.println("Error fetching last message: " + errorInfo.getMessage());
                                // Guard list.add behind the replied CAS so a late onError
                                // arriving AFTER replied=true cannot mutate the list while
                                // Flutter's StandardMessageCodec iterates it on the main
                                // thread (would throw ConcurrentModificationException).
                                if (replied.compareAndSet(false, true)) {
                                    Map<String, Object> messagesMap = new HashMap<>();
                                    messagesMap.put("status", "failed");
                                    list.add(messagesMap);
                                    result.success(list);
                                }
                            }
                        }),
                        errMsg -> {
                            if (replied.compareAndSet(false, true)) {
                                Map<String, Object> messagesMap = new HashMap<>();
                                messagesMap.put("status", "failed");
                                list.add(messagesMap);
                                result.success(list);
                            }
                        });
            }

            @Override
            public void onError(ErrorInfo errorInfo) {
                System.out.println("Error fetching conversation: " + errorInfo.getMessage());
                if (replied.compareAndSet(false, true)) {
                    Map<String, Object> messagesMap = new HashMap<>();
                    messagesMap.put("status", "failed");
                    list.add(messagesMap);
                    result.success(list);
                }
            }
        });
    }

    public static void getUnReadMsgCount(String conversationId, MethodChannel.Result result) {
        if (!isClientInitialized()) {
            result.success("Client not initialized");
            return;
        }

        List<Map<String, Object>> list = new ArrayList<>();
        conversationClient.getConversation(conversationId, new CallbackListener<Conversation>() {
            @Override
            public void onSuccess(Conversation conversation) {
                Map<String, Object> conversationMap = new HashMap<>();
                conversation.getUnreadMessagesCount(new CallbackListener<Long>() {
                    @Override
                    public void onSuccess(Long data) {

                        System.out.println("Success fetching getUnreadMessagesCount: " + data);
                        conversationMap.put("sid", conversationId);
                        conversationMap.put("unReadCount", data);
                        list.add(conversationMap);

                        result.success(list);
                    }

                    @Override
                    public void onError(ErrorInfo errorInfo) {
                        CallbackListener.super.onError(errorInfo);
                        System.out.println("Error fetching getUnreadMessagesCount: " + errorInfo.getMessage());
                        Map<String, Object> errorMap = new HashMap<>();
                        errorMap.put("status", Strings.failed);
                        list.add(errorMap);
                        result.success(list);
                    }
                });
            }

            @Override
            public void onError(ErrorInfo errorInfo) {
                System.out.println("Error fetching conversation: " + errorInfo.getMessage());
                List<Map<String, Object>> list = new ArrayList<>();
                Map<String, Object> messagesMap = new HashMap<>();
                messagesMap.put("status", Strings.failed);
                list.add(messagesMap);
                result.success(list);
            }
        });
        System.out.println("getUnReadMsgCount----->" + list);
    }

    /// Get messages from the specific conversation #
    public static void getAllMessages(String conversationId, Integer messageCount, MethodChannel.Result result) {
        if (!isClientInitialized()) {
            result.success("Client not initialized");
            return;
        }

        // List + reply guard hoisted to the outermost scope so EVERY terminal
        // branch (onSuccess, the three onError branches, the sync-failed
        // lambda, and the outer getConversation.onError) shares the same
        // single-fire AtomicBoolean. Previously `replied` was local to the
        // inner onSuccess only and the error branches called result.success
        // directly — if Twilio fired onSuccess then a late onError, Flutter
        // threw "Reply already submitted" and the error reply was swallowed
        // (MainThreadResult catches RuntimeException so no crash, but Dart
        // could see either response depending on dispatch order).
        final List<Map<String, Object>> list =
                Collections.synchronizedList(new ArrayList<>());
        final AtomicBoolean replied = new AtomicBoolean(false);
        final Runnable replyWithList = () -> {
            if (replied.compareAndSet(false, true)) {
                // Defensive DEEP snapshot. A shallow new ArrayList<>(list)
                // copies the outer list, but each entry's `attachMedia` is the
                // SAME synchronizedList instance the worker callbacks mutate.
                // If Twilio mis-fires a media callback after pendingMediaCount
                // already hit 0, the late mediaList.add races with the codec
                // iterating the same mediaList on the platform thread →
                // ConcurrentModificationException. Deep-copy each entry's
                // mediaList into a plain ArrayList so the codec sees a stable
                // structure even if a worker mutates the original.
                List<Map<String, Object>> snapshot;
                synchronized (list) {
                    snapshot = new ArrayList<>(list.size());
                    for (Map<String, Object> entry : list) {
                        Map<String, Object> entryCopy = new HashMap<>(entry);
                        Object media = entryCopy.get("attachMedia");
                        if (media instanceof List<?>) {
                            List<?> liveMedia = (List<?>) media;
                            List<Object> mediaCopy;
                            synchronized (liveMedia) {
                                mediaCopy = new ArrayList<>(liveMedia);
                            }
                            entryCopy.put("attachMedia", mediaCopy);
                        }
                        snapshot.add(entryCopy);
                    }
                }
                result.success(snapshot);
            }
        };
        final Runnable replyFailed = () -> {
            if (replied.compareAndSet(false, true)) {
                List<Map<String, Object>> failed = new ArrayList<>();
                Map<String, Object> messagesMap = new HashMap<>();
                messagesMap.put("status", Strings.failed);
                failed.add(messagesMap);
                result.success(failed);
            }
        };

        conversationClient.getConversation(conversationId, new CallbackListener<Conversation>() {
            @Override
            public void onSuccess(Conversation conversation) {
                runWhenConversationSynchronized(conversation,
                        () -> conversation.getLastMessages((messageCount != null) ? messageCount : 1000,
                                new CallbackListener<List<Message>>() {
                                    @Override
                                    public void onSuccess(List<Message> messagesList) {
                                        // A7: pre-arm the counter at 1 so any
                                        // synchronous callback during the loop
                                        // can decrement freely without reaching
                                        // 0 prematurely. We balance the +1 with
                                        // an explicit decrement after the loop.
                                        final int[] pendingMediaCount = { 1 };

                                        for (Message message : messagesList) {
                                            final Map<String, Object> messagesMap = new HashMap<>();
                                            messagesMap.put("sid", message.getSid());
                                            messagesMap.put("author", message.getAuthor());
                                            messagesMap.put("body", message.getBody());
                                            messagesMap.put("attributes", message.getAttributes().toString());
                                            messagesMap.put("dateCreated", message.getDateCreated());
                                            messagesMap.put("conversationSid", conversationId);

                                            // A20-analog: Collections.synchronizedList so
                                            // concurrent media-URL callbacks can safely add.
                                            final List<Map<String, Object>> mediaList =
                                                    Collections.synchronizedList(new ArrayList<>());

                                            for (Media media : message.getAttachedMedia()) {
                                                final Map<String, Object> mediaMap = new HashMap<>();
                                                mediaMap.put("sid", media.getSid());
                                                mediaMap.put("contentType", media.getContentType());
                                                mediaMap.put("filename", media.getFilename());

                                                // Increment the pending media counter
                                                synchronized (pendingMediaCount) {
                                                    pendingMediaCount[0]++;
                                                }

                                                media.getTemporaryContentUrl(new CallbackListener<String>() {
                                                    @Override
                                                    public void onSuccess(String mediaUrl) {
                                                        mediaMap.put("mediaUrl", mediaUrl);

                                                        // A20: mediaList.add belongs INSIDE the
                                                        // same critical section as the counter
                                                        // decrement so the visibility of mediaUrl
                                                        // (worker thread put) is published before
                                                        // any replyWithList reads mediaList.
                                                        // Move the reply OUTSIDE the lock so we
                                                        // don't hold pendingMediaCount while the
                                                        // codec / Handler.post takes its own locks.
                                                        boolean shouldReply;
                                                        synchronized (pendingMediaCount) {
                                                            mediaList.add(mediaMap);
                                                            pendingMediaCount[0]--;
                                                            shouldReply = pendingMediaCount[0] == 0;
                                                        }
                                                        if (shouldReply) replyWithList.run();
                                                    }

                                                    @Override
                                                    public void onError(ErrorInfo errorInfo) {
                                                        System.err.println("Error retrieving media URL: "
                                                                + errorInfo.getMessage());
                                                        boolean shouldReply;
                                                        synchronized (pendingMediaCount) {
                                                            pendingMediaCount[0]--;
                                                            shouldReply = pendingMediaCount[0] == 0;
                                                        }
                                                        if (shouldReply) replyWithList.run();
                                                    }
                                                });
                                            }

                                            messagesMap.put("attachMedia", mediaList);
                                            list.add(messagesMap);
                                        }

                                        // A8: setLastReadMessageIndex was previously
                                        // invoked once per message inside the loop —
                                        // N round-trips to Twilio for the same final
                                        // value. Issue it exactly once with the last
                                        // message index after the loop completes.
                                        if (!list.isEmpty()) {
                                            try {
                                                conversation.setLastReadMessageIndex(
                                                        conversation.getLastMessageIndex(),
                                                        new CallbackListener<Long>() {
                                                            @Override
                                                            public void onSuccess(Long result) {
                                                            }

                                                            @Override
                                                            public void onError(ErrorInfo errorInfo) {
                                                                System.err.println(
                                                                        "getAllMessages: setLastReadMessageIndex failed: "
                                                                                + errorInfo.getMessage());
                                                            }
                                                        });
                                            } catch (RuntimeException e) {
                                                System.err.println(
                                                        "getAllMessages: setLastReadMessageIndex threw: " + e);
                                            }
                                        }

                                        // Release the +1 from pre-arm outside the
                                        // synchronized block (same reason as the
                                        // per-media callbacks above).
                                        boolean shouldReply;
                                        synchronized (pendingMediaCount) {
                                            pendingMediaCount[0]--;
                                            shouldReply = pendingMediaCount[0] == 0;
                                        }
                                        if (shouldReply) replyWithList.run();
                                    }

                                    @Override
                                    public void onError(ErrorInfo errorInfo) {
                                        System.err.println("Error retrieving get messages: " + errorInfo.getMessage());
                                        replyFailed.run();
                                    }
                                }),
                                errMsg -> replyFailed.run());
            }

            @Override
            public void onError(ErrorInfo errorInfo) {
                System.err.println("Error retrieving conversation: " + errorInfo.getMessage());
                replyFailed.run();
            }
        });
    }

    public static void deleteConversation(String conversationId, MethodChannel.Result result) {
        conversationClient.getConversation(conversationId, new CallbackListener<Conversation>() {
            @Override
            public void onSuccess(Conversation conversation) {
                conversation.destroy(new StatusListener() {
                    @Override
                    public void onSuccess() {
                        System.err.println("Conversation Delete Success");
                        result.success(Strings.success);
                    }

                    @Override
                    public void onError(ErrorInfo errorInfo) {
                        System.err.println("Conversation Delete Failed: " + errorInfo.getMessage());
                        result.success(Strings.failed);
                    }
                });
            }

            @Override
            public void onError(ErrorInfo errorInfo) {
                System.err.println("Conversation Delete Failed");
                result.success(Strings.failed);
            }
        });
    }

    public static void deleteMessage(String conversationId, long index, MethodChannel.Result result) {
        if (!isClientInitialized()) {
            result.success("Client not initialized");
            return;
        }
        System.err.println("Index - " + index);
        conversationClient.getConversation(conversationId, new CallbackListener<Conversation>() {
            @Override
            public void onSuccess(Conversation conversation) {
                System.err.println("Conversation retrieved successfully.");

                runWhenConversationSynchronized(conversation,
                        () -> conversation.getMessageByIndex(index, new CallbackListener<Message>() {
                            @Override
                            public void onSuccess(Message message) {
                                System.err.println("Message retrieved successfully. Message: " + message + " Body: "
                                        + message.getBody());

                                // new Handler(Looper.getMainLooper()).postDelayed(() -> {
                                conversation.removeMessage(message, new StatusListener() {
                                    @Override
                                    public void onSuccess() {
                                        System.err.println("Message deleted successfully.");
                                        result.success(Strings.success);
                                    }

                                    @Override
                                    public void onError(ErrorInfo errorInfo) {
                                        System.err
                                                .println("Failed to delete message. Error: " + errorInfo.getMessage());
                                        result.success(Strings.failed);
                                    }
                                });
                                // }, 2000); // Delay of 2 seconds (2000 milliseconds)
                            }

                            @Override
                            public void onError(ErrorInfo errorInfo) {
                                System.err.println(
                                        "Failed to retrieve message by index. Error: " + errorInfo.getMessage());
                                result.success(Strings.failed);
                            }
                        }),
                        errMsg -> result.success(Strings.failed));
            }

            @Override
            public void onError(ErrorInfo errorInfo) {
                System.err.println("Failed to retrieve conversation. Error: " + errorInfo.getMessage());
                result.success(Strings.failed);
            }
        });
    }

    /// Delete message by sid #
    // Ported from ALAlliancetek fork (v0.4.0). Adapted to our fork:
    //   - isClientInitialized guard added (matches every other public entry).
    //   - getLastMessages call wrapped with runWhenConversationSynchronized
    //     so it inherits PR #1's IllegalStateException protection — ALAT's
    //     version called the SDK directly and would crash if the conversation
    //     hadn't reached SynchronizationStatus.ALL yet.
    //   - findMessageBySid helper inlined; replies use result.success(string)
    //     instead of result.error(...) to match the rest of this fork (Dart
    //     callers treat the response as a status string).
    public static void deleteMessageWithSid(String conversationId, String messageSid, Integer messageCount,
            MethodChannel.Result result) {
        if (!isClientInitialized()) {
            result.success("Client not initialized");
            return;
        }
        if (messageSid == null || messageSid.isEmpty()) {
            result.success(Strings.failed + ": messageSid is required");
            return;
        }
        final int searchCount = (messageCount != null && messageCount > 0) ? messageCount : 1000;
        conversationClient.getConversation(conversationId, new CallbackListener<Conversation>() {
            @Override
            public void onSuccess(Conversation conversation) {
                runWhenConversationSynchronized(conversation,
                        () -> conversation.getLastMessages(searchCount, new CallbackListener<List<Message>>() {
                            @Override
                            public void onSuccess(List<Message> messages) {
                                Message found = null;
                                if (messages != null) {
                                    for (Message msg : messages) {
                                        if (messageSid.equals(msg.getSid())) {
                                            found = msg;
                                            break;
                                        }
                                    }
                                }
                                if (found == null) {
                                    result.success("msg_not_found: SID not in last " + searchCount + " messages");
                                    return;
                                }
                                conversation.removeMessage(found, new StatusListener() {
                                    @Override
                                    public void onSuccess() {
                                        result.success(Strings.success);
                                    }

                                    @Override
                                    public void onError(ErrorInfo errorInfo) {
                                        StatusListener.super.onError(errorInfo);
                                        result.success("delete_failed: " + errorInfo.getMessage());
                                    }
                                });
                            }

                            @Override
                            public void onError(ErrorInfo errorInfo) {
                                CallbackListener.super.onError(errorInfo);
                                result.success("getLastMessages error: " + errorInfo.getMessage());
                            }
                        }),
                        errMsg -> result.success("Sync error: " + errMsg));
            }

            @Override
            public void onError(ErrorInfo errorInfo) {
                CallbackListener.super.onError(errorInfo);
                result.success("conv_failed: " + errorInfo.getMessage());
            }
        });
    }

    public static void initializeConversationClient(String accessToken, MethodChannel.Result result,
            ClientInterface clientInterface) {
        // A18: shut down the previous client (if any) before creating a fresh
        // one. Otherwise its ConversationsClientListener stays attached and
        // continues to fire onTokenExpired / onClientSynchronization on the
        // stale connection, producing duplicate events to Dart.
        if (conversationClient != null) {
            // Remove our listener first so its closure (which captures the
            // previous call's clientInterface) becomes eligible for GC. Doing
            // this BEFORE shutdown() also stops late callbacks fired during
            // the async shutdown from reaching a dead engine.
            ConversationsClient prevClient = conversationClient;
            ConversationsClientListener prevListener = clientListener;
            if (prevListener != null) {
                try {
                    prevClient.removeListener(prevListener);
                } catch (RuntimeException e) {
                    System.err.println(
                            "initializeConversationClient: previous removeListener threw: " + e);
                }
                clientListener = null;
            }
            try {
                prevClient.shutdown();
            } catch (RuntimeException e) {
                System.err.println("initializeConversationClient: previous shutdown threw: " + e);
            }
            // Drop the per-conversation listener map. Twilio's shutdown()
            // (called just above on prevClient) stops dispatching from any
            // listener attached to that client's Conversations, so we don't
            // need to explicitly removeListener on each entry — clearing the
            // map releases the plugin's hold on the listener instances.
            detachAllMessageListeners();
            conversationClient = null;
            currentSynchronizationStatus = null;
            currentConnectionState = null;
        }

        // Capture binding reference once. flutterPluginBinding may be nulled
        // concurrently by detachPluginFromClient on the platform thread; we
        // need a stable non-null reference for the SDK call.
        FlutterPlugin.FlutterPluginBinding binding = flutterPluginBinding;
        if (binding == null) {
            System.err.println(
                    "initializeConversationClient: flutterPluginBinding is null (engine detached?)");
            result.success(Strings.authenticationFailed);
            return;
        }

        ConversationsClient.Properties props = ConversationsClient.Properties.newBuilder().createProperties();
        ConversationsClient.create(binding.getApplicationContext(), accessToken, props,
                new CallbackListener<ConversationsClient>() {
                    @Override
                    public void onSuccess(ConversationsClient client) {
                        // If the engine detached while create() was in flight,
                        // do NOT attach a listener whose closure captures the
                        // now-stale clientInterface (the detached plugin). Just
                        // shutdown the freshly-created client and bail. We use
                        // the same flutterPluginBinding-null check that
                        // detachPluginFromClient performs.
                        if (flutterPluginBinding == null) {
                            System.err.println(
                                    "initializeConversationClient: engine detached during create — shutting down new client");
                            try {
                                client.shutdown();
                            } catch (RuntimeException e) {
                                System.err.println(
                                        "initializeConversationClient: detach-during-create shutdown threw: " + e);
                            }
                            result.success(Strings.authenticationFailed);
                            return;
                        }
                        ConversationsClientListener listener = new ConversationsClientListener() {

                            @Override
                            public void onConversationAdded(Conversation conversation) {
                                // System.out.println("onConversationAdded");
                            }

                            @Override
                            public void onConversationUpdated(Conversation conversation,
                                    Conversation.UpdateReason reason) {

                            }

                            @Override
                            public void onConversationDeleted(Conversation conversation) {

                            }

                            @Override
                            public void onConversationSynchronizationChange(Conversation conversation) {

                            }

                            @Override
                            public void onError(ErrorInfo errorInfo) {

                            }

                            @Override
                            public void onUserUpdated(User user, User.UpdateReason reason) {

                            }

                            @Override
                            public void onUserSubscribed(User user) {

                            }

                            @Override
                            public void onUserUnsubscribed(User user) {

                            }

                            @Override
                            public void onClientSynchronization(
                                    ConversationsClient.SynchronizationStatus synchronizationStatus) {
                                System.out.println("onClientSynchronization synchronizationStatus->"
                                        + synchronizationStatus.getValue());
                                currentSynchronizationStatus = synchronizationStatus;
                                if (synchronizationStatus == ConversationsClient.SynchronizationStatus.COMPLETED) {
                                    System.out.println("Client Synchronized");
                                }
                                if (clientInterface != null) {
                                    System.out.println("Passed to Flutter");
                                    Map<String, Object> syncMap = new HashMap<>();
                                    syncMap.put("status", synchronizationStatus.getValue());
                                    clientInterface.onClientSynchronizationChanged(syncMap);
                                }
                            }

                            @Override
                            public void onNewMessageNotification(String conversationSid, String messageSid,
                                    long messageIndex) {

                            }

                            @Override
                            public void onAddedToConversationNotification(String conversationSid) {

                            }

                            @Override
                            public void onRemovedFromConversationNotification(String conversationSid) {

                            }

                            @Override
                            public void onNotificationSubscribed() {

                            }

                            @Override
                            public void onNotificationFailed(ErrorInfo errorInfo) {

                            }

                            @Override
                            public void onConnectionStateChange(ConversationsClient.ConnectionState state) {
                                System.out.println("ConnectionState:" + state.getValue());
                                currentConnectionState = state;
                            }

                            @Override
                            public void onTokenExpired() {
                                System.out.println("onTokenExpired");
                                Map<String, Object> tokenStatusMap = new HashMap<>();
                                tokenStatusMap.put("statusCode", 401);
                                tokenStatusMap.put("message", Strings.accessTokenExpired);
                                onTokenStatusChange(tokenStatusMap);
                            }

                            @Override
                            public void onTokenAboutToExpire() {
                                // System.out.println("onTokenAboutToExpire");
                                Map<String, Object> tokenStatusMap = new HashMap<>();
                                tokenStatusMap.put("statusCode", 200);
                                tokenStatusMap.put("message", Strings.accessTokenWillExpire);
                                onTokenStatusChange(tokenStatusMap);
                            }
                        };
                        // Publish in a safe order: attach the listener to the
                        // new client FIRST, then publish the field references.
                        // A concurrent reader (e.g. shutdownClient on the
                        // platform thread) that observes the new conversationClient
                        // will also observe a fully-installed listener — never
                        // a half-attached one. Publishing client BEFORE
                        // addListener would let a teardown find a non-null
                        // client whose listener is not yet attached, then
                        // miss the upcoming addListener completely and leak it.
                        client.addListener(listener);
                        clientListener = listener;
                        conversationClient = client;
                        result.success(Strings.authenticationSuccessful);
                    }

                    @Override
                    public void onError(ErrorInfo errorInfo) {
                        System.out.println("Error " + errorInfo);
                        result.success(Strings.authenticationFailed);
                    }
                });
    }

    /// Get participants from the specific conversation #
    public static void getParticipants(String conversationId, MethodChannel.Result result) {
        if (!isClientInitialized()) {
            result.success("Client not initialized");
            return;
        }

        conversationClient.getConversation(conversationId, new CallbackListener<Conversation>() {
            @Override
            public void onSuccess(Conversation conversation) {
                List<Participant> participantList = conversation.getParticipantsList();
                List<Map<String, Object>> participants = new ArrayList<>();
                for (int i = 0; i < participantList.size(); i++) {
                    Map<String, Object> participantMap = new HashMap<>();
                    participantMap.put("identity", participantList.get(i).getIdentity());
                    participantMap.put("sid", participantList.get(i).getSid());
                    participantMap.put("conversationSid", participantList.get(i).getConversation().getSid());
                    participantMap.put("conversationCreatedBy",
                            participantList.get(i).getConversation().getCreatedBy());
                    participantMap.put("dateCreated", participantList.get(i).getConversation().getDateCreated());
                    participantMap.put("isAdmin",
                            Objects.equals(participantList.get(i).getConversation().getCreatedBy(),
                                    participantList.get(i).getIdentity()));
                    participantMap.put("attributes", participantList.get(i).getAttributes().toString());
                    participants.add(participantMap);
                    // System.out.println("participantMap->" + participantMap);
                }
                result.success(participants);
            }

            @Override
            public void onError(ErrorInfo errorInfo) {
                CallbackListener.super.onError(errorInfo);
                List<Participant> participantList = new ArrayList<>();
                result.success(participantList);
            }
        });
    }

    public static void getParticipantsWithName(String conversationId, MethodChannel.Result result) {
        if (!isClientInitialized()) {
            result.success("Client not initialized");
            return;
        }

        conversationClient.getConversation(conversationId, new CallbackListener<Conversation>() {
            @Override
            public void onSuccess(Conversation conversation) {
                List<Participant> participantList = conversation.getParticipantsList();
                List<Map<String, Object>> participants = new ArrayList<>();
                AtomicInteger pendingCallbacks = new AtomicInteger(participantList.size());

                if (participantList.isEmpty()) {
                    result.success(participants);
                    return;
                }

                for (Participant participant : participantList) {
                    Map<String, Object> participantMap = new HashMap<>();
                    participant.getAndSubscribeUser(new CallbackListener<User>() {
                        @Override
                        public void onSuccess(User user) {
                            participantMap.put("friendlyIdentity", user.getIdentity());
                            participantMap.put("friendlyName", user.getFriendlyName());
                            fillParticipantDetails(participant, participantMap);
                            participants.add(participantMap);
                            if (pendingCallbacks.decrementAndGet() == 0) {
                                result.success(participants);
                            }
                        }

                        @Override
                        public void onError(ErrorInfo errorInfo) {
                            fillParticipantDetails(participant, participantMap);
                            participants.add(participantMap);
                            if (pendingCallbacks.decrementAndGet() == 0) {
                                result.success(participants);
                            }
                        }
                    });
                }
            }

            @Override
            public void onError(ErrorInfo errorInfo) {
                result.success(new ArrayList<>());
            }
        });
    }

    private static void fillParticipantDetails(Participant participant, Map<String, Object> participantMap) {
        participantMap.put("identity", participant.getIdentity());
        participantMap.put("sid", participant.getSid());
        participantMap.put("conversationSid", participant.getConversation().getSid());
        participantMap.put("conversationCreatedBy", participant.getConversation().getCreatedBy());
        participantMap.put("dateCreated", participant.getConversation().getDateCreated());
        participantMap.put("isAdmin",
                Objects.equals(participant.getConversation().getCreatedBy(), participant.getIdentity()));
        participantMap.put("attributes", participant.getAttributes().toString());
    }

    public static void updateAccessToken(String accessToken, MethodChannel.Result result) {
        Map<String, Object> tokenStatus = new HashMap<>();
        conversationClient.updateToken(accessToken, new StatusListener() {
            @Override
            public void onSuccess() {
                System.out.println("Refreshed access token.");
                tokenStatus.put("statusCode", 200);
                tokenStatus.put("message", Strings.accessTokenRefreshed);
                result.success(tokenStatus);
            }

            @Override
            public void onError(ErrorInfo errorInfo) {
                StatusListener.super.onError(errorInfo);
                tokenStatus.put("statusCode", 500);
                tokenStatus.put("message", errorInfo.getMessage());
                result.success(tokenStatus);
            }
        });
    }

    public void setListener(MessageInterface listener) {
        messageInterface = listener;
    }

    public void setTokenListener(AccessTokenInterface listener) {
        accessTokenInterface = listener;
    }

    public static void triggerEvent(Map message) {
        // Pass the result through the messageInterface
        if (messageInterface != null) {
            messageInterface.onMessageUpdate(message);
        }
    }

    public static void onTokenStatusChange(Map status) {
        // Pass the result through the messageInterface
        // System.out.println("accessTokenInterface->" +
        // accessTokenInterface.toString());
        if (accessTokenInterface != null) {
            accessTokenInterface.onTokenStatusChange(status);
        }
    }

    /**
     * A19: drop the previous plugin's hold on the Twilio SDK on engine detach
     * (Flutter hot-restart, plugin teardown) WITHOUT shutting down the
     * client. Hot-restart detaches and reattaches the engine without the user
     * intending to log out, so killing the client here would force the user
     * to re-authenticate every restart — exactly what the calling-site
     * comment in TwilioConversationSdkPlugin.onDetachedFromEngine forbids.
     *
     * <p>What this does:
     * <ul>
     *   <li>Removes our ConversationsClientListener (which captures the
     *       previous plugin instance as {@code clientInterface}) so the old
     *       plugin can be garbage collected.</li>
     *   <li>Clears the {@link MessageInterface} / {@link AccessTokenInterface}
     *       reference IFF it still points at the detaching plugin — a new
     *       onAttachedToEngine on a fresh plugin instance will overwrite
     *       these via {@link #setListener} / {@link #setTokenListener}, but
     *       if the new plugin hasn't attached yet we don't want a stale
     *       worker-thread callback firing into the dead one.</li>
     *   <li>Drops the FlutterPluginBinding reference so the old engine's
     *       Application context is not pinned indefinitely.</li>
     * </ul>
     *
     * <p>Authoritative client teardown remains the Dart-driven
     * {@link #shutdownClient} path.
     *
     * @param detachingPlugin the plugin instance being detached; statics that
     *                        still point at it are nulled, statics that
     *                        already point at a newer instance are left alone
     */
    public static void detachPluginFromClient(
            Object detachingPlugin, FlutterPlugin.FlutterPluginBinding detachingBinding) {
        ConversationsClientListener listener = clientListener;
        if (listener != null) {
            // Try to detach from the live client if there is one — if not,
            // there's no SDK reference to remove, but we still need to null
            // the static so the listener's closure (which captures the old
            // plugin via clientInterface) becomes GC-eligible.
            if (conversationClient != null) {
                try {
                    conversationClient.removeListener(listener);
                } catch (RuntimeException e) {
                    System.err.println("detachPluginFromClient: removeListener threw: " + e);
                }
            }
            clientListener = null;
        }

        // Identity-compare so a hot-restart where the new plugin has already
        // re-registered itself doesn't get its interfaces nulled out from
        // under it. setListener / setTokenListener / onAttachedToEngine fire
        // synchronously on the platform thread but the order of detach-vs-
        // attach across two engines is not contractually guaranteed.
        if (messageInterface == detachingPlugin) {
            messageInterface = null;
        }
        if (accessTokenInterface == detachingPlugin) {
            accessTokenInterface = null;
        }
        // Same identity check on the binding — previously this was nulled
        // unconditionally, which would wipe a new engine's binding if it
        // attached before the old engine detached, causing the next
        // initializeConversationClient call to fail authentication.
        if (detachingBinding != null && flutterPluginBinding == detachingBinding) {
            flutterPluginBinding = null;
        }
    }

    /**
     * Shutdown and clean up the Twilio Conversations Client
     * This will properly dispose of the client and free up resources
     */
    public static void shutdownClient(MethodChannel.Result result) {
        try {
            if (conversationClient != null) {
                // Remove our ConversationsClientListener before shutdown so its
                // captured clientInterface (plugin instance) is GC-able and so
                // any late callbacks fired during the async shutdown don't
                // reach a now-disconnected Dart-side listener.
                ConversationsClientListener listener = clientListener;
                if (listener != null) {
                    try {
                        conversationClient.removeListener(listener);
                    } catch (RuntimeException e) {
                        System.err.println("shutdownClient: removeListener threw: " + e);
                    }
                    clientListener = null;
                }
                // Drop the per-conversation listener map. shutdown() below
                // stops Twilio from dispatching any further events on this
                // client's Conversations, so we just need to release the
                // plugin's references to the listener instances.
                detachAllMessageListeners();
                // Shutdown the client
                conversationClient.shutdown();
                conversationClient = null;

                // Reset synchronization status
                currentSynchronizationStatus = null;
                currentConnectionState = null;

                System.out.println("Twilio Conversations Client shutdown successfully");
                result.success("Client shutdown successfully");
            } else {
                result.success("Client already shutdown or not initialized");
            }
        } catch (Exception e) {
            System.err.println("Error shutting down client: " + e.getMessage());
            result.error("SHUTDOWN_ERROR", "Failed to shutdown client: " + e.getMessage(), null);
        }
    }
}