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
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.TimeZone;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import io.flutter.embedding.engine.plugins.FlutterPlugin;
import io.flutter.plugin.common.MethodChannel;

public class ConversationHandler {
    /// Entry point for the Conversations SDK.
    public static ConversationsClient conversationClient;
    public static FlutterPlugin.FlutterPluginBinding flutterPluginBinding;
    private static MessageInterface messageInterface;
    private static AccessTokenInterface accessTokenInterface;
    private static ConversationsClient.SynchronizationStatus currentSynchronizationStatus = null;
    private static ConversationsClient.ConnectionState currentConnectionState = null;

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
        // on the platform thread.
        final SyncErrorCallback deliverFailed = msg -> mainHandler.post(() -> onFailed.accept(msg));

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
                listenerHolder.set(null);
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
        }
    }

    /// Generate token and authenticate user #
    public static String generateAccessToken(String accountSid, String apiKey, String apiSecret, String identity,
            String serviceSid, String pushSid) {
        // Create an AccessToken builder
        System.out.println("admin-" + Arrays.toString(apiSecret.getBytes()));
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
                result.success(Strings.fcmFail);
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
            public void onSuccess(Conversation conversations) {

                addParticipant(identity, conversationName, result);
                result.success(conversations.getSid());
            }

            @Override
            public void onError(ErrorInfo errorInfo) {
                if (errorInfo.getMessage().equals(Strings.conversationExists)) {
                    result.success(Strings.conversationExists);
                } else {
                    result.success(Strings.createConversationFailure);
                }
                CallbackListener.super.onError(errorInfo);
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
            }
        });
    }

    /// Join the existing conversation #
    public static String joinConversation(String conversationId) {
        conversationClient.getConversation(conversationId, new CallbackListener<Conversation>() {
            @Override
            public void onSuccess(Conversation result) {
                // Retrieve the conversation object using the conversation SID
                result.join(new StatusListener() {
                    @Override
                    public void onSuccess() {
                    }

                    @Override
                    public void onError(ErrorInfo errorInfo) {
                        StatusListener.super.onError(errorInfo);
                    }
                });
            }

            @Override
            public void onError(ErrorInfo errorInfo) {
                CallbackListener.super.onError(errorInfo);
            }
        });
        return conversationId;
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
                JSONObject jsonObject;
                jsonObject = new JSONObject(attribute);

                Attributes attributes = new Attributes(jsonObject);
                conversation.prepareMessage().setAttributes(attributes).setBody(enteredMessage)
                        .buildAndSend(new CallbackListener() {
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
                                List<String> successList = new ArrayList<>();
                                List<String> errorList = new ArrayList<>();

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

                                            // ✅ อัปเดต body ก่อน
                                            targetMessage.updateBody(newBody, new StatusListener() {
                                                @Override
                                                public void onSuccess() {
                                                    // ✅ จากนั้นอัปเดต attributes ต่อ (ถ้ามี)
                                                    if (attributesToSet != null) {
                                                        targetMessage.setAttributes(attributesToSet,
                                                                new StatusListener() {
                                                                    @Override
                                                                    public void onSuccess() {
                                                                        successList.add(msgId);
                                                                        latch.countDown();
                                                                    }

                                                                    @Override
                                                                    public void onError(ErrorInfo errorInfo) {
                                                                        errorList.add(msgId + ": setAttributes error - "
                                                                                + errorInfo.getMessage());
                                                                        latch.countDown();
                                                                    }
                                                                });
                                                    } else {
                                                        // ไม่มี attributes ให้ update, ถือว่าสำเร็จ
                                                        successList.add(msgId);
                                                        latch.countDown();
                                                    }
                                                }

                                                @Override
                                                public void onError(ErrorInfo errorInfo) {
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

                                // รอให้ทุก message อัปเดตเสร็จ
                                new Thread(() -> {
                                    try {
                                        latch.await();
                                        // สร้าง response
                                        Map<String, Object> responseMap = new HashMap<>();
                                        responseMap.put("success", successList);
                                        responseMap.put("errors", errorList);
                                        responseMap.put("totalSuccess", successList.size());
                                        responseMap.put("totalErrors", errorList.size());

                                        // ส่งผลลัพธ์กลับไปยัง Flutter
                                        new Handler(Looper.getMainLooper()).post(() -> {
                                            result.success(responseMap);
                                        });
                                    } catch (InterruptedException e) {
                                        new Handler(Looper.getMainLooper()).post(() -> {
                                            Map<String, Object> errorResponse = new HashMap<>();
                                            errorResponse.put("error", "Thread interrupted: " + e.getMessage());
                                            result.success(errorResponse);
                                        });
                                    }
                                }).start();
                            }

                            @Override
                            public void onError(ErrorInfo errorInfo) {
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
                                    JSONObject jsonObject = new JSONObject(attribute);
                                    final Attributes finalAttributes = new Attributes(jsonObject); // ✅ ต้องเป็น final

                                    // ✅ อัปเดต body ก่อน
                                    targetMessage.updateBody(enteredMessage, new StatusListener() {
                                        @Override
                                        public void onSuccess() {
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
        // Fetch the conversation using the conversationId
        conversationClient.getConversation(conversationId, new CallbackListener<Conversation>() {
            @Override
            public void onSuccess(Conversation conversation) {
                try {
                    System.out.println("enteredMessage:" + enteredMessage);
                    System.out.println("conversationId:" + conversationId);
                    System.out.println("MediaFile:" + mediaFilePath);
                    System.out.println("MediaType" + mimeType);
                    System.out.println("MediaName" + fileName);
                    // Prepare the message with media
                    JSONObject jsonObject;
                    jsonObject = new JSONObject(attribute);

                    Attributes attributes = new Attributes(jsonObject);
                    InputStream fileInputStream = null;
                    if (mediaFilePath != null) {
                        fileInputStream = openInputStream(new File(mediaFilePath));
                    }
                    // try (InputStream inputStream = new FileInputStream(file)) {
                    assert fileInputStream != null;
                    conversation.prepareMessage().setAttributes(attributes).setBody(enteredMessage)
                            .addMedia(fileInputStream, mimeType, fileName, new MediaUploadListener() {
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
                                    HashMap<String, Object> progressData = new HashMap<>();
                                    progressData.put("mediaStatus", "Completed");
                                    triggerEvent(progressData);
                                }

                                @Override
                                public void onFailed(@NonNull ErrorInfo errorInfo) {
                                    // Handle media upload failure
                                    System.err.println("Media upload failed:" + errorInfo.getMessage());
                                    HashMap<String, Object> progressData = new HashMap<>();
                                    progressData.put("mediaStatus", Strings.failed);
                                    triggerEvent(progressData);
                                }
                            }).buildAndSend(new CallbackListener() {
                                @Override
                                public void onSuccess(Object data) {
                                    // Message sent successfully
                                    System.out.println("Message sent successfully!");
                                    result.success("send");
                                }

                                @Override
                                public void onError(ErrorInfo errorInfo) {
                                    // Handle message send error
                                    System.err.println("Error sending message: " + errorInfo.getMessage());
                                    // result.success("SendMessageError", errorInfo.getMessage(), null);
                                    HashMap<String, Object> progressData = new HashMap<>();
                                    progressData.put("messageStatus", Strings.failed);
                                    triggerEvent(progressData);
                                }
                            });
                } catch (Exception e) {
                    // Handle exceptions (e.g., JSONException, FileNotFoundException)
                    System.err.println("Error preparing message: " + e.getMessage());
                    HashMap<String, Object> progressData = new HashMap<>();
                    progressData.put("messageStatus", Strings.failed);
                    triggerEvent(progressData);
                    // result.error("PrepareMessageError", e.getMessage(), null);
                }
            }

            @Override
            public void onError(ErrorInfo errorInfo) {
                // Handle error in fetching conversation
                System.err.println("Error fetching conversation: " + errorInfo.getMessage());
                // result.error("ConversationFetchError", errorInfo.getMessage(), null);
                HashMap<String, Object> progressData = new HashMap<>();
                progressData.put("messageStatus", Strings.failed);
                triggerEvent(progressData);
            }
        });
    }

    /// Subscribe To Message Update #
    public static void subscribeToMessageUpdate(String conversationId) {
        conversationClient.getConversation(conversationId, new CallbackListener<Conversation>() {
            @Override
            public void onSuccess(Conversation result) {
                // Join the conversation with the given participant identity
                result.addListener(new ConversationListener() {
                    @Override
                    public void onMessageAdded(Message message) {
                        // new code for attach media check
                        try {
                            Map<String, Object> messageMap = new HashMap<>();
                            messageMap.put("sid", message.getSid());
                            messageMap.put("author", message.getAuthor());
                            messageMap.put("body", message.getBody());
                            messageMap.put("attributes", message.getAttributes().toString());
                            messageMap.put("dateCreated", message.getDateCreated());
                            messageMap.put("conversationSid", result.getSid());

                            List<Map<String, Object>> mediaList = new ArrayList<>();
                            int[] pendingMediaCount = { 0 }; // Counter to track pending URL fetches

                            for (Media media : message.getAttachedMedia()) {
                                Map<String, Object> mediaMap = new HashMap<>();
                                mediaMap.put("sid", media.getSid());
                                mediaMap.put("contentType", media.getContentType());
                                mediaMap.put("filename", media.getFilename());

                                // Increment pending media count
                                synchronized (pendingMediaCount) {
                                    pendingMediaCount[0]++;
                                }

                                media.getTemporaryContentUrl(new CallbackListener<String>() {
                                    @Override
                                    public void onSuccess(String mediaUrl) {
                                        mediaMap.put("mediaUrl", mediaUrl);
                                        mediaList.add(mediaMap);

                                        // Decrement pending media count and check completion
                                        synchronized (pendingMediaCount) {
                                            pendingMediaCount[0]--;
                                            if (pendingMediaCount[0] == 0) {
                                                messageMap.put("attachMedia", mediaList);
                                                triggerEvent(messageMap); // Trigger event when all URLs are fetched
                                            }
                                        }
                                    }

                                    @Override
                                    public void onError(ErrorInfo errorInfo) {
                                        System.err.println("Error retrieving media URL: " + errorInfo.getMessage());

                                        // Decrement pending media count and check completion
                                        synchronized (pendingMediaCount) {
                                            pendingMediaCount[0]--;
                                            if (pendingMediaCount[0] == 0) {
                                                messageMap.put("attachMedia", mediaList);
                                                triggerEvent(messageMap); // Trigger event even if there are errors
                                            }
                                        }
                                    }
                                });
                            }

                            // If no media to fetch, trigger the event immediately
                            synchronized (pendingMediaCount) {
                                if (pendingMediaCount[0] == 0) {
                                    messageMap.put("attachMedia", mediaList);
                                    triggerEvent(messageMap);
                                }
                            }

                            // Update the last read message index
                            result.setLastReadMessageIndex(result.getLastMessageIndex() + 1,
                                    new CallbackListener<Long>() {
                                        @Override
                                        public void onSuccess(Long result) {
                                            System.out.println("LastReadMessageIndex- " + result);
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
                            Map<String, Object> messageMap = new HashMap<>();
                            messageMap.put("sid", message.getSid());
                            messageMap.put("author", message.getAuthor());
                            messageMap.put("body", message.getBody());
                            messageMap.put("attributes", message.getAttributes().toString());
                            messageMap.put("dateCreated", message.getDateCreated());
                            messageMap.put("conversationSid", result.getSid());

                            List<Map<String, Object>> mediaList = new ArrayList<>();
                            int[] pendingMediaCount = { 0 }; // Counter to track pending URL fetches

                            for (Media media : message.getAttachedMedia()) {
                                Map<String, Object> mediaMap = new HashMap<>();
                                mediaMap.put("sid", media.getSid());
                                mediaMap.put("contentType", media.getContentType());
                                mediaMap.put("filename", media.getFilename());

                                // Increment pending media count
                                synchronized (pendingMediaCount) {
                                    pendingMediaCount[0]++;
                                }

                                media.getTemporaryContentUrl(new CallbackListener<String>() {
                                    @Override
                                    public void onSuccess(String mediaUrl) {
                                        mediaMap.put("mediaUrl", mediaUrl);
                                        mediaList.add(mediaMap);

                                        // Decrement pending media count and check completion
                                        synchronized (pendingMediaCount) {
                                            pendingMediaCount[0]--;
                                            if (pendingMediaCount[0] == 0) {
                                                messageMap.put("attachMedia", mediaList);
                                                triggerEvent(messageMap); // Trigger event when all URLs are fetched
                                            }
                                        }
                                    }

                                    @Override
                                    public void onError(ErrorInfo errorInfo) {
                                        System.err.println("Error retrieving media URL: " + errorInfo.getMessage());

                                        // Decrement pending media count and check completion
                                        synchronized (pendingMediaCount) {
                                            pendingMediaCount[0]--;
                                            if (pendingMediaCount[0] == 0) {
                                                messageMap.put("attachMedia", mediaList);
                                                triggerEvent(messageMap); // Trigger event even if there are errors
                                            }
                                        }
                                    }
                                });
                            }

                            // If no media to fetch, trigger the event immediately
                            synchronized (pendingMediaCount) {
                                if (pendingMediaCount[0] == 0) {
                                    messageMap.put("attachMedia", mediaList);
                                    triggerEvent(messageMap);
                                }
                            }

                            // Update the last read message index
                            result.setLastReadMessageIndex(result.getLastMessageIndex() + 1,
                                    new CallbackListener<Long>() {
                                        @Override
                                        public void onSuccess(Long result) {
                                            System.out.println("LastReadMessageIndex- " + result);
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
                });
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
                /// Retrieve the conversation object using the conversation SID
                result.removeAllListeners();
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

        List<Map<String, Object>> list = new ArrayList<>();
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
                                    Participant participant = lastMessage.getParticipant();
                                    if (participant != null) { // Added null check here
                                        pendingCallbacks.incrementAndGet();
                                        participant.getAndSubscribeUser(new CallbackListener<User>() {
                                            @Override
                                            public void onSuccess(User user) {
                                                conversationMap.put("friendlyIdentity", user.getIdentity());
                                                conversationMap.put("friendlyName", user.getFriendlyName());
                                                if (pendingCallbacks.decrementAndGet() == 0) {
                                                    result.success(list);
                                                }
                                            }
                                        });
                                    }

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

                                    list.add(conversationMap);
                                }
                                if (pendingCallbacks.decrementAndGet() == 0) {
                                    result.success(list);
                                }
                            }

                            @Override
                            public void onError(ErrorInfo errorInfo) {
                                System.out.println("Error fetching last message: " + errorInfo.getMessage());
                                Map<String, Object> messagesMap = new HashMap<>();
                                messagesMap.put("status", "failed");
                                list.add(messagesMap);
                                result.success(list);
                            }
                        }),
                        errMsg -> {
                            Map<String, Object> messagesMap = new HashMap<>();
                            messagesMap.put("status", "failed");
                            list.add(messagesMap);
                            result.success(list);
                        });
            }

            @Override
            public void onError(ErrorInfo errorInfo) {
                System.out.println("Error fetching conversation: " + errorInfo.getMessage());
                Map<String, Object> messagesMap = new HashMap<>();
                messagesMap.put("status", "failed");
                list.add(messagesMap);
                result.success(list);
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

        List<Map<String, Object>> list = new ArrayList<>();
        conversationClient.getConversation(conversationId, new CallbackListener<Conversation>() {
            @Override
            public void onSuccess(Conversation conversation) {
                runWhenConversationSynchronized(conversation,
                        () -> conversation.getLastMessages((messageCount != null) ? messageCount : 1000,
                                new CallbackListener<List<Message>>() {
                                    @Override
                                    public void onSuccess(List<Message> messagesList) {
                                        int[] pendingMediaCount = { 0 }; // Counter for pending media URL fetches

                                        for (Message message : messagesList) {
                                            Map<String, Object> messagesMap = new HashMap<>();
                                            messagesMap.put("sid", message.getSid());
                                            messagesMap.put("author", message.getAuthor());
                                            messagesMap.put("body", message.getBody());
                                            messagesMap.put("attributes", message.getAttributes().toString());
                                            messagesMap.put("dateCreated", message.getDateCreated());
                                            messagesMap.put("conversationSid", conversationId);

                                            List<Map<String, Object>> mediaList = new ArrayList<>();

                                            for (Media media : message.getAttachedMedia()) {
                                                Map<String, Object> mediaMap = new HashMap<>();
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

                                                        // Decrement the pending media counter
                                                        synchronized (pendingMediaCount) {
                                                            pendingMediaCount[0]--;
                                                            if (pendingMediaCount[0] == 0) {
                                                                result.success(list); // All media URLs fetched
                                                            }
                                                        }
                                                    }

                                                    @Override
                                                    public void onError(ErrorInfo errorInfo) {
                                                        System.err.println("Error retrieving media URL: "
                                                                + errorInfo.getMessage());

                                                        // Decrement the pending media counter
                                                        synchronized (pendingMediaCount) {
                                                            pendingMediaCount[0]--;
                                                            if (pendingMediaCount[0] == 0) {
                                                                result.success(list); // All media URLs fetched
                                                            }
                                                        }
                                                    }
                                                });

                                                mediaList.add(mediaMap);
                                            }

                                            messagesMap.put("attachMedia", mediaList);
                                            list.add(messagesMap);
                                            if (!list.isEmpty()) {
                                                conversation.setLastReadMessageIndex(conversation.getLastMessageIndex(),
                                                        new CallbackListener<Long>() {
                                                            @Override
                                                            public void onSuccess(Long result) {

                                                            }
                                                        });
                                            }
                                        }

                                        // Check if there are no pending media URLs
                                        synchronized (pendingMediaCount) {
                                            if (pendingMediaCount[0] == 0) {
                                                result.success(list);
                                            }
                                        }
                                    }

                                    @Override
                                    public void onError(ErrorInfo errorInfo) {
                                        System.err.println("Error retrieving get messages: " + errorInfo.getMessage());
                                        List<Map<String, Object>> list = new ArrayList<>();
                                        Map<String, Object> messagesMap = new HashMap<>();
                                        messagesMap.put("status", Strings.failed);
                                        list.add(messagesMap);
                                        result.success(list);
                                        // result.error("MESSAGE_RETRIEVAL_ERROR", errorInfo.getMessage(), null);
                                    }
                                }),
                                errMsg -> {
                                    List<Map<String, Object>> errList = new ArrayList<>();
                                    Map<String, Object> messagesMap = new HashMap<>();
                                    messagesMap.put("status", Strings.failed);
                                    errList.add(messagesMap);
                                    result.success(errList);
                                });
            }

            @Override
            public void onError(ErrorInfo errorInfo) {
                System.err.println("Error retrieving conversation: " + errorInfo.getMessage());
                List<Map<String, Object>> list = new ArrayList<>();
                Map<String, Object> messagesMap = new HashMap<>();
                messagesMap.put("status", Strings.failed);
                list.add(messagesMap);
                result.success(list);
                // result.error("CONVERSATION_RETRIEVAL_ERROR", errorInfo.getMessage(), null);
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

    public static void deleteMessage(String conversationId, int index, MethodChannel.Result result) {
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

    public static void initializeConversationClient(String accessToken, MethodChannel.Result result,
            ClientInterface clientInterface) {
        ConversationsClient.Properties props = ConversationsClient.Properties.newBuilder().createProperties();
        ConversationsClient.create(flutterPluginBinding.getApplicationContext(), accessToken, props,
                new CallbackListener<ConversationsClient>() {
                    @Override
                    public void onSuccess(ConversationsClient client) {
                        conversationClient = client;
                        conversationClient.addListener(new ConversationsClientListener() {

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
                        });
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
     * Shutdown and clean up the Twilio Conversations Client
     * This will properly dispose of the client and free up resources
     */
    public static void shutdownClient(MethodChannel.Result result) {
        try {
            if (conversationClient != null) {
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