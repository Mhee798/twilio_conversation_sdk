package com.at.twilio_conversation_sdk;

import android.os.Handler;
import android.os.Looper;

import androidx.annotation.NonNull;

import java.util.concurrent.atomic.AtomicBoolean;


import com.at.twilio_conversation_sdk.app_interface.AccessTokenInterface;
import com.at.twilio_conversation_sdk.app_interface.ClientInterface;
import com.at.twilio_conversation_sdk.app_interface.MessageInterface;
import com.at.twilio_conversation_sdk.conversation.ConversationHandler;
import com.at.twilio_conversation_sdk.utility.Methods;
import com.at.twilio_conversation_sdk.utility.Strings;
import com.twilio.conversations.Conversation;

import java.util.List;
import java.util.Map;

import io.flutter.embedding.engine.plugins.FlutterPlugin;
import io.flutter.plugin.common.EventChannel;
import io.flutter.plugin.common.MethodCall;
import io.flutter.plugin.common.MethodChannel;
import io.flutter.plugin.common.MethodChannel.MethodCallHandler;
import io.flutter.plugin.common.MethodChannel.Result;
import io.flutter.plugin.common.EventChannel.EventSink;
import io.flutter.plugin.common.EventChannel.StreamHandler;


/**
 * TwilioChatConversationSdkPlugin
 */
public class TwilioConversationSdkPlugin implements FlutterPlugin, MethodCallHandler, MessageInterface, AccessTokenInterface, ClientInterface {
    /// The MethodChannel that will the communication between Flutter and native Android
    /// This local reference serves to register the plugin with the Flutter Engine and unregister it
    /// when the Flutter Engine is detached from the Activity
    private MethodChannel channel;
    private EventChannel eventChannel;
    private EventChannel eventSyncChannel;
    private EventChannel tokenEventChannel;
    private EventChannel clientEventChannel;
    /**
     * Captured at onAttachedToEngine and passed back to
     * {@link ConversationHandler#detachPluginFromClient} so the static
     * {@code flutterPluginBinding} is only nulled if it still points at THIS
     * plugin's binding. Without this, a hot-restart where the new engine
     * attaches before the old engine detaches would have the old detach
     * unconditionally wipe the new binding.
     */
    private FlutterPluginBinding ownBinding;
    // A11: per-channel sinks were previously assigned in a single onListen() that
    // received the events arg from whichever channel happened to register last.
    // Now each EventChannel has its own StreamHandler so events stay on their
    // intended stream (e.g. token events no longer leak into client-sync stream).
    //
    // volatile because writers (per-channel onListen / onCancel on the platform
    // thread) and readers (the onMessageUpdate / onSynchronizationChanged /
    // onTokenStatusChange / onClientSynchronizationChanged paths that Twilio
    // invokes from worker threads) have no other happens-before edge — without
    // volatile a worker can see a stale value of the field and either drop a
    // valid event or invoke a sink whose channel has already been torn down.
    private volatile EventChannel.EventSink eventSink;
    private volatile EventChannel.EventSink eventSyncSink;
    private volatile EventChannel.EventSink tokenEventSink;
    private volatile EventChannel.EventSink clientEventSink;

    // Initialised once at class load via a null-tolerant factory so class
    // loading does NOT throw ExceptionInInitializerError in environments
    // without a prepared main Looper (Robolectric, plain JVM unit tests,
    // some IDE indexers). Final because no code re-assigns it; volatile
    // would be misleading.
    private static final Handler MAIN_HANDLER = createMainHandler();

    private static Handler createMainHandler() {
        try {
            Looper main = Looper.getMainLooper();
            return main != null ? new Handler(main) : null;
        } catch (RuntimeException e) {
            // Defensive — getMainLooper() shouldn't throw, but if it does we
            // refuse to load the class on its account.
            return null;
        }
    }

    /**
     * Post {@code r} to the main looper. Returns true when successfully
     * enqueued; false when MAIN_HANDLER is null (test env) or post refused
     * (looper quitting).
     *
     * <p>Behaviour when post fails:
     * <ul>
     *   <li>MAIN_HANDLER null (test env): run the runnable inline on the
     *       calling thread. Tests are expected to provide platform-thread
     *       semantics through their harness, or to use a no-op Result.</li>
     *   <li>post refused (looper quit): log and DO NOT execute inline. Off-
     *       platform-thread invocation of Flutter MethodChannel.Result /
     *       EventSink.success would either throw (and be swallowed by the
     *       caller's try/catch) or quietly fail — either way the Dart-side
     *       Future or Stream cannot be reached. Logging makes the drop
     *       observable; pretending to deliver hides it.</li>
     * </ul>
     */
    private static boolean postToMainOrLog(@NonNull Runnable r, @NonNull String label) {
        Handler h = MAIN_HANDLER;
        if (h == null) {
            // No main looper (test env). Run inline.
            try {
                r.run();
            } catch (Throwable t) {
                System.err.println(label + ": inline run threw: " + t);
            }
            return false;
        }
        if (h.post(r)) {
            return true;
        }
        // Looper is quitting / queue has shut down. Off-thread inline
        // execution of the Runnable would invoke EventSink.success or
        // MethodChannel.Result.success on the wrong thread — Flutter throws
        // IllegalStateException and the catch wrappers swallow it, with
        // identical user-visible effect to dropping. Drop loudly instead.
        System.err.println(label
                + ": MAIN_HANDLER.post refused (looper quitting); reply will not be delivered");
        return false;
    }

    /**
     * A12: Decorator that forwards every MethodChannel.Result reply via the main
     * looper. Flutter requires platform-thread responses; many of our paths
     * complete inside Twilio CallbackListener/StatusListener callbacks which
     * Twilio dispatches off the main thread. Wrapping at the Plugin layer
     * means individual handler methods don't each need a Handler.post.
     *
     * <p>Calls that are already on the main thread still queue one tick — that
     * is the expected behaviour for platform channels and avoids re-entrancy
     * issues if a handler completes synchronously inside onMethodCall.
     */
    private static final class MainThreadResult implements MethodChannel.Result {
        private final MethodChannel.Result inner;
        // True once ANY of success/error/notImplemented has CLAIMED the
        // right to reply. CAS'd at the entry of each method so a second
        // call (e.g. an SDK that fires both onSuccess and onError on the
        // same listener) is dropped without scheduling a second inner.*
        // invocation. Note this is "claimed", not "delivered" — if the
        // inner call subsequently throws inside the codec we still attempt
        // a best-effort fallback inner.error, but the CAS prevents a
        // second caller from ALSO trying.
        private final AtomicBoolean replied = new AtomicBoolean(false);

        MainThreadResult(MethodChannel.Result inner) {
            this.inner = inner;
        }

        @Override
        public void success(Object value) {
            if (!replied.compareAndSet(false, true)) {
                System.err.println("MainThreadResult.success: dropped double-call");
                return;
            }
            postToMainOrLog(() -> {
                // Throwable catch: in addition to IllegalStateException
                // (already replied / engine torn down) and
                // IllegalArgumentException (StandardMethodCodec rejects an
                // unencodable type — Date, custom POJOs, etc.), the codec
                // can throw StackOverflowError on deeply-nested maps and
                // OutOfMemoryError on huge payloads. Anything escaping the
                // runnable would crash Looper.loop().
                try {
                    inner.success(value);
                } catch (Throwable t) {
                    System.err.println("MainThreadResult.success threw: " + t);
                    // Best-effort fallback: try to fail the Future
                    // explicitly so Dart does not hang waiting for a reply
                    // that the codec failed to encode. Flutter may reject
                    // this if the channel already marked itself as replied
                    // mid-encode — purely a safety net, the Dart Future
                    // can still hang under pathological codec failures.
                    try {
                        inner.error("REPLY_FAILED",
                                "result.success threw: " + t.getClass().getSimpleName(),
                                null);
                    } catch (Throwable t2) {
                        System.err.println(
                                "MainThreadResult.success fallback error also threw: " + t2);
                    }
                }
            }, "MainThreadResult.success");
        }

        @Override
        public void error(@NonNull String code, String message, Object details) {
            if (!replied.compareAndSet(false, true)) {
                System.err.println("MainThreadResult.error: dropped double-call (code=" + code + ")");
                return;
            }
            postToMainOrLog(() -> {
                try {
                    inner.error(code, message, details);
                } catch (Throwable t) {
                    System.err.println("MainThreadResult.error threw: " + t);
                    try {
                        inner.error("REPLY_FAILED",
                                "result.error threw: " + t.getClass().getSimpleName(),
                                null);
                    } catch (Throwable t2) {
                        System.err.println(
                                "MainThreadResult.error fallback also threw: " + t2);
                    }
                }
            }, "MainThreadResult.error");
        }

        @Override
        public void notImplemented() {
            if (!replied.compareAndSet(false, true)) {
                System.err.println("MainThreadResult.notImplemented: dropped double-call");
                return;
            }
            postToMainOrLog(() -> {
                try {
                    inner.notImplemented();
                } catch (Throwable t) {
                    System.err.println("MainThreadResult.notImplemented threw: " + t);
                }
            }, "MainThreadResult.notImplemented");
        }
    }

    @Override
    public void onAttachedToEngine(@NonNull FlutterPluginBinding flutterPluginBinding) {
        ownBinding = flutterPluginBinding;
        channel = new MethodChannel(flutterPluginBinding.getBinaryMessenger(), "twilio_conversation_sdk");
        channel.setMethodCallHandler(this);
        eventChannel = new EventChannel(flutterPluginBinding.getBinaryMessenger(), "twilio_conversation_sdk/onMessageUpdated");
        eventChannel.setStreamHandler(messageStreamHandler);
        eventSyncChannel = new EventChannel(flutterPluginBinding.getBinaryMessenger(), "twilio_conversation_sdk/onSynchronizationChanged");
        eventSyncChannel.setStreamHandler(syncStreamHandler);
        tokenEventChannel = new EventChannel(flutterPluginBinding.getBinaryMessenger(), "twilio_conversation_sdk/onTokenStatusChange");
        tokenEventChannel.setStreamHandler(tokenStreamHandler);
        clientEventChannel = new EventChannel(flutterPluginBinding.getBinaryMessenger(), "twilio_conversation_sdk/onClientSynchronizationChanged");
        clientEventChannel.setStreamHandler(clientStreamHandler);

        // Wire static SDK state to THIS plugin instance. setListener /
        // setTokenListener mutate static fields directly (see ConversationHandler
        // — the instance receiver is a vestige). Pass `this` as the owner so
        // detachPluginFromClient can identity-check before clearing.
        ConversationHandler.flutterPluginBinding = flutterPluginBinding;
        ConversationHandler handler = new ConversationHandler();
        handler.setListener(this);
        handler.setTokenListener(this);
    }

    @Override
    public void onMethodCall(@NonNull MethodCall call, @NonNull Result rawResult) {
        // A12: wrap once at the entry so every result.success / result.error
        // inside ConversationHandler dispatches on the platform thread.
        final MethodChannel.Result result = new MainThreadResult(rawResult);
        System.out.println("call.method->" + call.method);
        switch (call.method) {
            // To generate twilio access token #
            case Methods.generateToken: //Generate token and authenticate user
                String accessToken = ConversationHandler.generateAccessToken(call.argument("accountSid"), call.argument("apiKey"), call.argument("apiSecret"), call.argument("identity"), call.argument("serviceSid"), call.argument("pushSid"));
                System.out.println("accessToken generated->" + accessToken);
                result.success(accessToken);
//        ConversationHandler.initializeConversationClient(accessToken,result);
                break;

            case Methods.initializeConversationClient: //Generate token and authenticate user
                ConversationHandler.initializeConversationClient(call.argument("accessToken"), result, this);
                break;
            //Register FCM Token#
            case Methods.registerFCMToken:
                ConversationHandler.registerFCMToken(call.argument("fcmToken"), result);
                break;
            //UnRegister FCM Token#
            case Methods.unregisterFCMToken:
                ConversationHandler.unregisterFCMToken(call.argument("fcmToken"), result);
                break;
            // Create new conversation #
            case Methods.createConversation:
                ConversationHandler.createConversation(call.argument("conversationName"), call.argument("identity"), result);
                break;
            // Get list of conversations for logged in user #
            case Methods.getConversations:
                List<Map<String, Object>> conversationList = ConversationHandler.getConversationsList();
                result.success(conversationList);
                break;
            // Get list of conversations for logged in user last message#
            case Methods.getLastMessages:
                ///List<Map<String, Object>> getLastMessagesList =
                ConversationHandler.getLastMessages(call.argument("conversationId"), result);
                //result.success(getLastMessagesList);
                break;
            // Get list of conversations for logged in user last message#
            case Methods.getUnReadMsgCount:
                ///List<Map<String, Object>> getLastMessagesList =
                ConversationHandler.getUnReadMsgCount(call.argument("conversationId"), result);
                //result.success(getLastMessagesList);
                break;
            // Get messages from the specific conversation #
            case Methods.getMessages:
                ConversationHandler.getAllMessages(call.argument("conversationId"), call.argument("messageCount"), result);
                break;
            //Join the existing conversation #
            case Methods.joinConversation:
                String joinStatus = ConversationHandler.joinConversation(call.argument("conversationId"));
                result.success(joinStatus);
                break;
            // Send message #
            case Methods.sendMessage:
                ConversationHandler.sendMessages(call.argument("message"), call.argument("conversationId"), call.argument("attribute"), result);
                break;
            case Methods.updateMessage:
                ConversationHandler.body(call.argument("message"), call.argument("conversationId"), call.argument("msgId"), call.argument("attribute"), result);
                break;
            case Methods.updateMessages:
                ConversationHandler.updateMessages(call.argument("conversationId"), call.argument("messages"), result);
                break;
            // Send message with media #
            case Methods.sendMessageWithMedia:
                ConversationHandler.sendMessageWithMedia(
                        call.argument("message"),
                        call.argument("conversationId"),
                        call.argument("attribute"),
                        call.argument("mediaFilePath"),
                        call.argument("mimeType"),
                        call.argument("fileName"),
                        result
                );
                break;
            // Add participant in a conversation #
            case Methods.addParticipant:
                ConversationHandler.addParticipant(call.argument("participantName"), call.argument("conversationId"), result);
                break;
            case Methods.removeParticipant:
                ConversationHandler.removeParticipant(call.argument("participantName"), call.argument("conversationId"), result);
                break;
            // Get & Listen messages from the specific conversation #
            case Methods.receiveMessages:
            case Methods.subscribeToMessageUpdate:
                ConversationHandler.subscribeToMessageUpdate(call.argument("conversationId"));
                // The handler is fire-and-forget (subscription is async on
                // Twilio's worker pool and surfaces results through the
                // EventChannel, not this MethodChannel). Reply immediately so
                // the Dart-side Future doesn't hang and the MainThreadResult
                // wrapping rawResult is released.
                //
                // CAVEAT: the immediate success reply does NOT indicate the
                // listener was actually attached. If Twilio's getConversation
                // fails, or addListener throws, the subscription silently
                // doesn't install — Dart sees a successful method call but
                // never receives onMessageUpdated events. Failures are logged
                // to stderr only. A future API improvement would either reply
                // from inside the async callback or surface the failure via
                // an EventChannel event so Dart can re-subscribe.
                result.success(null);
                break;
            // Get participants from the specific conversation #
            case Methods.getParticipants:
                ConversationHandler.getParticipants(call.argument("conversationId"), result);
                break;
            // Get participants with name from the specific conversation #
            case Methods.getParticipantsWithName:
                ConversationHandler.getParticipantsWithName(call.argument("conversationId"), result);
                break;
            case Methods.unSubscribeToMessageUpdate:
                ConversationHandler.unSubscribeToMessageUpdate(call.argument("conversationId"));
                // Same fire-and-forget contract as subscribeToMessageUpdate —
                // reply immediately so the Dart Future doesn't hang.
                result.success(null);
                break;
            case Methods.updateAccessToken:
                ConversationHandler.updateAccessToken(call.argument("accessToken"), result);
                break;
            case Methods.deleteConversation:
                ConversationHandler.deleteConversation(call.argument("conversationId"), result);
                break;
            case Methods.deleteMessage: {
                // Dart `int` arrives as Integer for small values, Long for
                // large ones — coerce via Number to avoid ClassCastException
                // when the conversation has indexed past Integer.MAX_VALUE.
                Number idx = call.argument("index");
                if (idx == null) {
                    result.success(Strings.failed);
                    break;
                }
                ConversationHandler.deleteMessage(call.argument("conversationId"), idx.longValue(), result);
                break;
            }
            case Methods.deleteMessageWithSid: {
                // Same Dart-int bridging concern as deleteMessage: coerce
                // via Number to accept both Integer and Long arrivals.
                Number count = call.argument("messageCount");
                Integer messageCount = (count != null) ? count.intValue() : null;
                ConversationHandler.deleteMessageWithSid(
                        call.argument("conversationId"),
                        call.argument("messageSid"),
                        messageCount,
                        result);
                break;
            }
            case Methods.setTypingStatus:
                ConversationHandler.setTypingStatus(call.argument("conversationId"), call.argument("isTyping"), result);
                break;
            case Methods.isClientInitialized:
                boolean isInitialized = ConversationHandler.isClientInitialized();
                result.success(isInitialized);
                break;
            case Methods.shutdownClient:
                ConversationHandler.shutdownClient(result);
                break;
            default:
                break;
        }
    }

    @Override
    public void onDetachedFromEngine(@NonNull FlutterPluginBinding binding) {
        channel.setMethodCallHandler(null);
        eventChannel.setStreamHandler(null);
        eventSyncChannel.setStreamHandler(null);
        tokenEventChannel.setStreamHandler(null);
        clientEventChannel.setStreamHandler(null);

        // A19: drop sinks held by this plugin instance.
        eventSink = null;
        eventSyncSink = null;
        tokenEventSink = null;
        clientEventSink = null;

        // A19: detach the plugin from the static SDK state so the now-discarded
        // engine doesn't keep receiving Twilio callbacks via captured plugin
        // references — but do NOT shut down the Twilio client itself. On a
        // Flutter hot-restart the engine detaches and reattaches without the
        // app intending to log out; previously we shut the client down here
        // and the user had to re-authenticate every restart, contradicting the
        // intent in this comment. The Dart-driven shutdownClient method is the
        // only authoritative teardown path.
        //
        // detachPluginFromClient() also removes the ConversationsClientListener
        // that captures `clientInterface` (this plugin), so the old plugin
        // instance can be garbage collected once the new engine attaches.
        //
        // Pass our captured ownBinding so the static flutterPluginBinding is
        // only cleared if it still refers to THIS plugin's binding — a new
        // engine that attached before this detach must not have its binding
        // wiped.
        ConversationHandler.detachPluginFromClient(this, ownBinding);
        ownBinding = null;
    }

    // A11: dedicated per-EventChannel StreamHandler instances. Each one only
    // touches its own sink field; previously all four channels shared `this`
    // and clobbered each other's sinks inside a single onListen() so events
    // could be delivered on the wrong stream.
    private final StreamHandler messageStreamHandler = new StreamHandler() {
        @Override
        public void onListen(Object arguments, EventSink events) {
            eventSink = events;
        }

        @Override
        public void onCancel(Object arguments) {
            eventSink = null;
        }
    };

    private final StreamHandler syncStreamHandler = new StreamHandler() {
        @Override
        public void onListen(Object arguments, EventSink events) {
            eventSyncSink = events;
        }

        @Override
        public void onCancel(Object arguments) {
            eventSyncSink = null;
        }
    };

    private final StreamHandler tokenStreamHandler = new StreamHandler() {
        @Override
        public void onListen(Object arguments, EventSink events) {
            tokenEventSink = events;
        }

        @Override
        public void onCancel(Object arguments) {
            tokenEventSink = null;
        }
    };

    private final StreamHandler clientStreamHandler = new StreamHandler() {
        @Override
        public void onListen(Object arguments, EventSink events) {
            clientEventSink = events;
        }

        @Override
        public void onCancel(Object arguments) {
            clientEventSink = null;
        }
    };

    @Override
    public void onMessageUpdate(Map message) {
        // EventSink.success must be invoked on the platform thread.
        postToMainOrLog(() -> {
            EventChannel.EventSink sink = this.eventSink;
            if (sink != null) {
                try {
                    sink.success(message);
                } catch (RuntimeException e) {
                    System.err.println("onMessageUpdate sink.success swallowed: " + e);
                }
            }
        }, "onMessageUpdate");
    }

    @Override
    public void onSynchronizationChanged(Map status) {
        // Forward the conversation-sync event to BOTH the dedicated sync sink
        // (the architecturally correct destination) AND the message sink (the
        // de-facto destination Dart consumers listen to via the
        // `{"status": <int>}` filter in lib/twilio_conversation_sdk.dart line
        // 366 and example/lib/main.dart line 144). Pre-A11 the buggy single-
        // onListen fan-out aliased these two sinks, so Dart received the
        // events on whichever stream it subscribed to. The A11 split made the
        // routing architecturally clean but left no Dart subscriber on the
        // dedicated sync channel — silently dropping events. Until the Dart
        // side adds a sync-channel listener we publish to both.
        postToMainOrLog(() -> {
            EventChannel.EventSink syncSink = this.eventSyncSink;
            if (syncSink != null) {
                try {
                    syncSink.success(status);
                } catch (RuntimeException e) {
                    System.err.println("onSynchronizationChanged syncSink.success swallowed: " + e);
                }
            }
            EventChannel.EventSink msgSink = this.eventSink;
            if (msgSink != null) {
                try {
                    msgSink.success(status);
                } catch (RuntimeException e) {
                    System.err.println("onSynchronizationChanged msgSink.success swallowed: " + e);
                }
            }
        }, "onSynchronizationChanged");
    }

    @Override
    public void onTokenStatusChange(Map message) {
        postToMainOrLog(() -> {
            EventChannel.EventSink sink = this.tokenEventSink;
            if (sink != null) {
                try {
                    sink.success(message);
                } catch (RuntimeException e) {
                    System.err.println("onTokenStatusChange sink.success swallowed: " + e);
                }
            }
        }, "onTokenStatusChange");
    }

    @Override
    public void onClientSynchronizationChanged(Map status) {
        System.out.println("onClientSynchronizationChanged SDK Plugin");
        postToMainOrLog(() -> {
            EventChannel.EventSink sink = this.clientEventSink;
            if (sink != null) {
                System.out.println("onClientSynchronizationChanged SDK Plugin Not null");
                try {
                    sink.success(status);
                } catch (RuntimeException e) {
                    System.err.println("onClientSynchronizationChanged sink.success swallowed: " + e);
                }
            }
        }, "onClientSynchronizationChanged");
    }
}
