import UIKit
import TwilioConversationsClient
import Flutter
import Foundation

class ConversationsHandler: NSObject, TwilioConversationsClientDelegate {
    
    
    
    // MARK: Conversations variables
    private var client: TwilioConversationsClient?
    var lastReadIndex: NSNumber?
    weak var messageDelegate: MessageDelegate?
    weak var clientDelegate: ClientDelegate?
    var isSubscribe:Bool?
    var conversationId : String?
    public var messageSubscriptionId: String = ""
    var tokenEventSink: FlutterEventSink?

    // MARK: Sync-wait helper (I2 — Swift port of Android's
    // runWhenConversationSynchronized). Wait until a TCHConversation reaches
    // .all synchronization status before invoking a closure that calls into
    // Twilio's message APIs. Without this guard, getLastMessages /
    // prepareMessage / setLastReadMessageIndex etc. invoked too early would
    // either no-op silently or return empty data, and the caller's Future
    // would never resolve.
    //
    // Both onReady and onFailed are guaranteed to fire exactly once on the
    // main queue. onFailed surfaces sync .failed, the 30-second timeout, or
    // a missing sid.
    //
    // Scope of the watchdog: the 30s timeout only covers the sync-status
    // step. Once onReady fires, the SDK calls that run inside it
    // (getLastMessages, prepareMessage()...buildAndSend, getUnreadMessagesCount,
    // updateBody, setAttributes, conversation.remove,
    // media.getTemporaryContentUrl) are NOT individually watchdogged — if any
    // of them never callback the Dart Future can still hang. Closing that
    // remaining surface is Phase 5 watchdog work, not addressed here.
    private static let conversationSyncTimeoutSeconds: TimeInterval = 30
    private var syncWaiters: [String: [SyncWaiter]] = [:]

    // Per-sid pending read-index. Lets messageAdded coalesce bursts: rapid
    // messages all overwrite the latest computedIndex; a single in-flight
    // flush waiter then writes that final value once. Prevents the N-message
    // burst → N concurrent setLastReadMessageIndex calls racing on the
    // server. Touched only on .main.
    private var pendingReadIndex: [String: NSNumber] = [:]
    private var pendingReadFlush: Set<String> = []

    // Registry of currently-armed OneShots. Used by shutdownClient to
    // invalidate every in-flight watchdog so its onTimeout doesn't fire
    // against a torn-down FlutterResult ~30s after teardown. The registry
    // also tracks a `drained` flag so a OneShot constructed off-main, racing
    // an in-flight drainAll(), can be detected at arm-time and fired NOW
    // instead of installing a timer that would survive shutdown.
    fileprivate let oneShotRegistry = OneShotRegistry()

    func isClientInitialized() -> Bool {
        guard let client = client else {
            return false
        }
        return client.synchronizationStatus == .completed
    }

    /// Run `onReady` once the conversation reports
    /// `TCHConversationSynchronizationStatus.all`. On `.failed`, a missing
    /// sid, or a 30-second timeout, run `onFailed` instead. Exactly one of
    /// the two runs, always on the main queue.
    func runWhenConversationSynchronized(
        _ conversation: TCHConversation,
        onReady: @escaping () -> Void,
        onFailed: @escaping (String) -> Void
    ) {
        let body = { [weak self] in
            guard let self = self else { return }
            let status = conversation.synchronizationStatus
            if status == .all {
                onReady()
                return
            }
            if status == .failed {
                let sidStr = conversation.sid ?? "?"
                print("Conversation synchronization already FAILED for \(sidStr)")
                onFailed("Conversation synchronization FAILED for \(sidStr)")
                return
            }
            guard let sid = conversation.sid else {
                // No sid = nothing to key this waiter on. (Android's helper
                // registers a listener directly on the Conversation object
                // and doesn't need a sid, so it has no equivalent case;
                // iOS's dispatch is sid-keyed so we have to short-circuit.)
                // A sid-less conversation is typically a transient state
                // right after createConversation, and failing here would
                // intermittently break the first sendMessage after
                // creation. Let the SDK call inside onReady surface its
                // own error if the conversation truly isn't usable.
                print("runWhenConversationSynchronized: conversation has no sid; running onReady without wait")
                onReady()
                return
            }
            let waiter = SyncWaiter(onReady: onReady, onFailed: onFailed)
            // Schedule the timeout BEFORE registering — defends against the
            // SDK never firing the delegate callback (Phase 5 watchdog).
            let timer = DispatchSource.makeTimerSource(queue: .main)
            timer.schedule(deadline: .now() + ConversationsHandler.conversationSyncTimeoutSeconds)
            timer.setEventHandler { [weak self, weak waiter] in
                guard let self = self, let waiter = waiter else { return }
                if waiter.fireFailed("Conversation synchronization timed out for \(sid)") {
                    self.removeWaiter(waiter, sid: sid)
                }
            }
            // Append BEFORE resume() so the dispatch path can never observe
            // an empty list while a timer for an un-tracked waiter is alive.
            self.syncWaiters[sid, default: []].append(waiter)
            waiter.timer = timer
            timer.resume()

            // Defensive recheck — status may have flipped to terminal in
            // the gap between the fast-path check and the append above.
            // Deferred via main.async so any onReady the recheck triggers
            // runs on a fresh main-queue tick instead of synchronously
            // inside this caller's stack frame (preserves the "onReady is
            // async" contract that other call sites rely on, and prevents
            // reentrant FlutterResult delivery during a switch handler).
            let recheck = conversation.synchronizationStatus
            if recheck == .all || recheck == .failed {
                DispatchQueue.main.async { [weak self] in
                    self?.dispatchSyncWaiters(for: conversation)
                }
            }
        }
        if Thread.isMainThread {
            body()
        } else {
            DispatchQueue.main.async(execute: body)
        }
    }

    /// Wake any waiters registered for this conversation if its sync status is
    /// now terminal. Called from the TwilioConversationsClient delegate
    /// callback for synchronization status updates. Safe to call on any
    /// status — non-terminal updates are no-ops.
    ///
    /// Must run on .main: syncWaiters and SyncWaiter.handled are written
    /// from both this path and the main-queue helper / timer eventHandler,
    /// and the dict / Bool are not thread-safe. dispatchPrecondition makes
    /// the invariant explicit in debug builds.
    fileprivate func dispatchSyncWaiters(for conversation: TCHConversation) {
        dispatchPrecondition(condition: .onQueue(.main))
        guard let sid = conversation.sid,
              let waiters = syncWaiters[sid],
              !waiters.isEmpty else { return }
        switch conversation.synchronizationStatus {
        case .all:
            syncWaiters.removeValue(forKey: sid)
            for waiter in waiters {
                _ = waiter.fireReady()
            }
        case .failed:
            syncWaiters.removeValue(forKey: sid)
            for waiter in waiters {
                _ = waiter.fireFailed("Conversation synchronization FAILED for \(sid)")
            }
        default:
            // Not yet terminal — keep waiting.
            break
        }
    }

    private func removeWaiter(_ waiter: SyncWaiter, sid: String) {
        guard var list = syncWaiters[sid] else { return }
        list.removeAll { $0 === waiter }
        if list.isEmpty {
            syncWaiters.removeValue(forKey: sid)
        } else {
            syncWaiters[sid] = list
        }
    }

    /// Coalesce a messageAdded burst into a single in-flight
    /// setLastReadMessageIndex per sid. Must run on .main.
    /// - Always updates pendingReadIndex[sid] to the latest computedIndex.
    /// - If pendingReadFlush already contains sid, an in-flight writeFlush
    ///   will pick up the new value on its next iteration — return.
    /// - Otherwise mark sid as in-flight and dispatch performReadIndexFlush
    ///   onto the next main tick so a burst within the same tick coalesces.
    fileprivate func scheduleReadIndexFlush(sid: String,
                                            computedIndex: NSNumber,
                                            conversation: TCHConversation) {
        dispatchPrecondition(condition: .onQueue(.main))
        let prev = self.pendingReadIndex[sid]?.intValue ?? 0
        self.pendingReadIndex[sid] = NSNumber(value: max(prev, computedIndex.intValue))
        if self.pendingReadFlush.contains(sid) { return }
        self.pendingReadFlush.insert(sid)
        DispatchQueue.main.async { [weak self] in
            self?.performReadIndexFlush(sid: sid, conversation: conversation)
        }
    }

    /// Issue a single setLastReadMessageIndex write for sid using the latest
    /// pendingReadIndex value. Keep pendingReadFlush[sid] set throughout
    /// the in-flight SDK call so concurrent scheduleReadIndexFlush calls
    /// short-circuit instead of issuing parallel writes. On completion,
    /// re-schedule if a fresher index landed during the flight; otherwise
    /// clear the in-flight marker. Must run on .main.
    ///
    /// Implemented as a method (rather than a self-referential local
    /// closure) so the success-path re-schedule does NOT introduce a
    /// retain cycle between the closure and its captured `var` box that
    /// would leak the captured conversation / sid per delivery.
    fileprivate func performReadIndexFlush(sid: String,
                                           conversation: TCHConversation) {
        dispatchPrecondition(condition: .onQueue(.main))
        guard let indexToWrite = self.pendingReadIndex.removeValue(forKey: sid) else {
            // Nothing to flush — clear the in-flight marker so a future
            // messageAdded can schedule again.
            self.pendingReadFlush.remove(sid)
            return
        }
        self.runWhenConversationSynchronized(conversation, onReady: { [weak self] in
            conversation.setLastReadMessageIndex(indexToWrite) { result, index in
                print("setLastReadMessageIndex \(result.description)")
                // Twilio delivers this callback off-main on some paths;
                // hop to .main before touching pendingReadIndex /
                // pendingReadFlush.
                let finish: () -> Void = { [weak self] in
                    guard let self = self else { return }
                    if self.pendingReadIndex[sid] != nil {
                        // Newer index landed during flight — push it next.
                        DispatchQueue.main.async { [weak self] in
                            self?.performReadIndexFlush(sid: sid, conversation: conversation)
                        }
                    } else {
                        self.pendingReadFlush.remove(sid)
                    }
                }
                if Thread.isMainThread {
                    finish()
                } else {
                    DispatchQueue.main.async(execute: finish)
                }
            }
        }, onFailed: { [weak self] msg in
            // Sync failed — restore the just-removed index so the next
            // messageAdded picks up from at least this point. Drop the
            // in-flight marker so a future messageAdded can re-schedule;
            // we deliberately do NOT auto-retry here (matches the prior
            // behaviour of waiting for a real signal rather than busy-
            // looping on a sync-failed conversation).
            if let self = self {
                let restored = max(indexToWrite.intValue,
                                   (self.pendingReadIndex[sid]?.intValue ?? 0))
                self.pendingReadIndex[sid] = NSNumber(value: restored)
                self.pendingReadFlush.remove(sid)
            }
            print("messageAdded.setLastReadMessageIndex: sync wait failed: \(msg)")
        })
    }



    //    MARK: raw
    func conversationsClient(_ client: TwilioConversationsClient, conversation: TCHConversation,
                             messageAdded message: TCHMessage) {

        guard isClientInitialized() else {
            return
        }

        self.getMessageInDictionary(message, conversationSid: conversation.sid) { [self] messageDictionary in
            if let messageDict = messageDictionary {
                var updatedMessage: [String: Any] = [:]
                updatedMessage["conversationId"] = conversation.sid ?? ""
                updatedMessage["message"] = messageDict
                //MARK: Update Index
                let computedIndex: NSNumber = {
                    if let lastMessageIndex = conversation.lastMessageIndex {
                        // Extract the value of lastMessageIndex and add 1
                        return NSNumber(value: lastMessageIndex.intValue + 1)
                    } else {
                        return 1
                    }
                }()

                // Media attachments are populated inside `getMessageInDictionary`
                // (DispatchGroup + per-media OneShot watchdog, see line ~1265),
                // which writes `attachMedia` into `messageDict` already. The
                // plugin forwards `messageDict` (not `updatedMessage`) to Dart,
                // so duplicating the loop here would only re-emit broken media
                // entries (value-type append before async, raw URL instead of
                // absoluteString) that no consumer reads.

                if (isSubscribe ?? false && conversationId == conversation.sid ),
                    let sid = conversation.sid {
                    // Coalesce: bursts of messageAdded must NOT register N
                    // parallel sync-wait waiters that race N independent
                    // setLastReadMessageIndex server writes (final stored
                    // index then non-deterministic). Instead, track the
                    // latest computedIndex per sid; have at most ONE flush
                    // waiter in-flight per sid; on flush, read the latest
                    // pending index and write it once. Hop work to .main so
                    // pendingReadIndex / pendingReadFlush are touched on a
                    // single queue regardless of which delegate queue
                    // Twilio used to deliver messageAdded.
                    // Coalesce on .main: track latest index per sid, keep at
                    // most ONE writeFlush in-flight per sid (the
                    // pendingReadFlush set acts as the in-flight marker).
                    // Implemented via methods on the handler rather than
                    // self-referential local closures to avoid the retain
                    // cycle that would otherwise leak the captured
                    // `conversation` / `sid` / `computedIndex` per delivery.
                    let scheduleFlush: () -> Void = { [weak self] in
                        guard let self = self else { return }
                        self.scheduleReadIndexFlush(sid: sid,
                                                    computedIndex: computedIndex,
                                                    conversation: conversation)
                    }
                    if Thread.isMainThread {
                        scheduleFlush()
                    } else {
                        DispatchQueue.main.async(execute: scheduleFlush)
                    }
                }


                self.messageDelegate?.onMessageUpdate(message: updatedMessage, messageSubscriptionId: self.messageSubscriptionId)

                //                print("lastReadIndex \(conversation.lastMessageIndex)")

            }
        }
    }

    // MARK: - Message Updated Listener
    // func conversationsClient(_ client: TwilioConversationsClient, conversation: TCHConversation,
    //                          messageUpdated message: TCHMessage) {

    //     print("onMessageUpdated -> \(message)")

    //     var attachedMedia: [[String: Any]] = []
    //     guard client.synchronizationStatus == .completed else {
    //         return
    //     }

    //     self.getMessageInDictionary(message) { [self] messageDictionary in
    //         if let messageDict = messageDictionary {
    //             var updatedMessage: [String: Any] = [:]
    //             updatedMessage["conversationId"] = conversation.sid ?? ""
    //             updatedMessage["message"] = messageDict

    //             // ดึง media ถ้ามี
    //             for media in message.getMedia(by: Set([MediaCategory.media])) {
    //                 var mediaMap: [String: Any] = [:]

    //                 mediaMap["sid"] = media.sid
    //                 mediaMap["contentType"] = media.contentType
    //                 mediaMap["filename"] = media.filename

    //                 media.getTemporaryContentUrl { result, tempUrl in
    //                     mediaMap["mediaUrl"] = tempUrl?.absoluteString ?? ""
    //                     print("TempURL >>> \(tempUrl?.absoluteString ?? "")")
    //                 }
    //                 attachedMedia.append(mediaMap)
    //             }

    //             if !attachedMedia.isEmpty {
    //                 updatedMessage["attachMedia"] = attachedMedia
    //             }

    //             // Update last read index if subscribed
    //             if (isSubscribe ?? false && conversationId == conversation.sid) {
    //                 let computedIndex: NSNumber = {
    //                     if let lastMessageIndex = conversation.lastMessageIndex {
    //                         return NSNumber(value: lastMessageIndex.intValue + 1)
    //                     } else {
    //                         return 1
    //                     }
    //                 }()

    //                 conversation.setLastReadMessageIndex(computedIndex) { result, index in
    //                     print("setLastReadMessageIndex \(result.description)")
    //                 }
    //             }

    //             // ส่ง event กลับไปยัง Flutter
    //             self.messageDelegate?.onMessageUpdate(message: updatedMessage, messageSubscriptionId: self.messageSubscriptionId)
    //         }
    //     }
    // }

    // MARK: - Typing Started Listener
    func conversationsClient(_ client: TwilioConversationsClient,
                             typingStartedOn conversation: TCHConversation,
                             participant: TCHParticipant) {
        print("onTypingStarted -> participant: \(participant.identity ?? "unknown"), conversation: \(conversation.sid ?? "unknown")")

        guard isClientInitialized() else {
            return
        }

        var typingMap: [String: Any] = [:]
        typingMap["typingStatus"] = true
        typingMap["identity"] = participant.identity ?? ""
        typingMap["conversationSid"] = conversation.sid ?? ""

        self.messageDelegate?.onMessageUpdate(message: typingMap, messageSubscriptionId: self.messageSubscriptionId)
    }

    // MARK: - Typing Ended Listener
    func conversationsClient(_ client: TwilioConversationsClient,
                             typingEndedOn conversation: TCHConversation,
                             participant: TCHParticipant) {
        print("onTypingEnded -> participant: \(participant.identity ?? "unknown"), conversation: \(conversation.sid ?? "unknown")")

        guard isClientInitialized() else {
            return
        }

        var typingMap: [String: Any] = [:]
        typingMap["typingStatus"] = false
        typingMap["identity"] = participant.identity ?? ""
        typingMap["conversationSid"] = conversation.sid ?? ""

        self.messageDelegate?.onMessageUpdate(message: typingMap, messageSubscriptionId: self.messageSubscriptionId)
    }

    func registerFCMToken(token: String,completion: @escaping (_ success : Bool) -> Void){

        let data = token.hexToData
        //        print(data) // Output: 5 bytes

        guard let client = self.client else {
            print("FCM register: client not initialized")
            completion(false)
            return
        }
        client.register(withNotificationToken: data ?? Data(), completion: { result in
            completion(result.isSuccessful)
            print("Twilio Notification Token Set: \(result) with token \(token)")
            print("Device push token registration was\(result.isSuccessful ? "" : " not") successful")
        })
    }

    func unregisterFCMToken(token: String,completion: @escaping (_ success : Bool) -> Void){

        let data = token.hexToData
        //        print(data) // Output: 5 bytes

        guard let client = self.client else {
            print("FCM unregister: client not initialized")
            completion(false)
            return
        }
        client.deregister(withNotificationToken: data ?? Data(), completion: { result in
            completion(result.isSuccessful)
            print("Twilio Notification Token deregister: \(result) with token \(token)")
            print("Device push token deregister \(result.isSuccessful ? "" : " not") successful")
        })
    }


    func conversationsClient(_ client: TwilioConversationsClient, conversation: TCHConversation, synchronizationStatusUpdated status: TCHConversationSynchronizationStatus) {
        // Wake any pending sync-wait registrations for this conversation
        // before forwarding the event so callers waiting on runWhen...
        // unblock as early as possible. Funnel through main: Twilio iOS SDK
        // does not contractually guarantee delegate dispatch queue and
        // dispatchSyncWaiters mutates state shared with the main-queue
        // helper / timer eventHandler.
        let dispatch = { [weak self] in
            self?.dispatchSyncWaiters(for: conversation)
            self?.messageDelegate?.onSynchronizationChanged(status: ["status" : conversation.synchronizationStatus.rawValue])
            print("StatusConversations \(conversation.synchronizationStatus.rawValue) ")
        }
        if Thread.isMainThread {
            dispatch()
        } else {
            DispatchQueue.main.async(execute: dispatch)
        }
    }

    func conversationsClientTokenWillExpire(_ client: TwilioConversationsClient) {
        print("Access token will expire.->\(String(describing: tokenEventSink))")
        var tokenStatusMap: [String: Any] = [:]
        tokenStatusMap["statusCode"] = 200
        tokenStatusMap["message"] = Strings.accessTokenWillExpire
        tokenEventSink?(tokenStatusMap)
    }


    func conversationsClient(_ client: TwilioConversationsClient, synchronizationStatusUpdated status: TCHClientSynchronizationStatus) {

        print("statusclient->\(status.hashValue)--\(client.synchronizationStatus)")

        guard status == .completed else {
            return
        }
        self.clientDelegate?.onClientSynchronizationChanged(status: ["status":client.synchronizationStatus.rawValue])
        print("StatusClient \(client.synchronizationStatus.rawValue) ")

        //            checkConversationCreation { (_, conversation) in
        //               if let conversation = conversation {
        //                   self.joinConversation(conversation)
        //               } else {
        //                   self.createConversation { (success, conversation) in
        //                       if success, let conversation = conversation {
        //                           self.joinConversation(conversation)
        //                       }
        //                   }
        //               }
        //            }
    }



    func conversationsClientTokenExpired(_ client: TwilioConversationsClient) {
        print("Access token expired.\(String(describing: tokenEventSink))")
        var tokenStatusMap: [String: Any] = [:]
        tokenStatusMap["statusCode"] = 401
        tokenStatusMap["message"] = Strings.accessTokenExpired
        tokenEventSink?(tokenStatusMap)
    }

    public func updateAccessToken(accessToken:String,completion: @escaping (TCHResult?) -> Void) {
        self.client?.updateToken(accessToken, completion: { tchResult in
            completion(tchResult)
        })
    }



    func sendMessage(conversationId: String,
                     messageText: String,
                     attributes: [String: Any]?,
                     completion: @escaping (TCHResult, String?, String?) -> Void) {
        // Third tuple slot `failureReason` surfaces the iOS-side error
        // message (sync timeout, "Conversation not found", etc.) to the
        // plugin handler. TCHResult is read-only, so its resultText cannot
        // carry our diagnostic; without this channel a sync timeout would
        // arrive at Dart as `FlutterError(SEND_FAILED, "Conversation not
        // found")`, indistinguishable from a genuinely missing conversation.
        self.getConversationFromId(conversationId: conversationId) { [weak self] conversation in
            guard let self = self else { return }
            guard let conversation = conversation else {
                print("Conversation not found for id: \(conversationId)")
                completion(TCHResult(), nil, "Conversation not found for id: \(conversationId)")
                return
            }

            // Wait for sync — prepareMessage on an un-synced conversation can
            // produce a builder that buildAndSend rejects.
            self.runWhenConversationSynchronized(conversation, onReady: {
                let builder = conversation.prepareMessage().setBody(messageText)
                if let attributes = attributes {
                    var attrError: NSError?
                    let attributesObject = TCHJsonAttributes(dictionary: attributes)
                    _ = builder.setAttributes(attributesObject, error: &attrError)
                    if let attrError = attrError {
                        print("sendMessage: setAttributes rejected: \(attrError.localizedDescription)")
                    }
                }
                // Bound buildAndSend so a dropped SDK callback can't hang
                // the Dart Future. anyvet retries with a 4s Dart timeout
                // anyway, but resolving natively saves the retry budget
                // for a real network round-trip.
                let shot = OneShot(registry: self.oneShotRegistry)
                shot.arm(seconds: 30) {
                    completion(TCHResult(), nil, "buildAndSend timed out after 30s")
                }
                builder.buildAndSend(completion: { tchResult, tchMessages in
                    DispatchQueue.main.async {
                        guard shot.complete() else { return }
                        if tchResult.isSuccessful, let messageSid = tchMessages?.sid {
                            completion(tchResult, messageSid, nil)
                        } else {
                            completion(tchResult, nil, tchResult.resultText)
                        }
                    }
                })
            }, onFailed: { msg in
                print("sendMessage: sync wait failed: \(msg)")
                completion(TCHResult(), nil, "Sync error: \(msg)")
            })
        }
    }

    func body(
        conversationId: String,
        msgId: String,
        messageText: String,
        attributes: [String: Any]?,
        completion: @escaping (TCHResult, TCHMessage?, String?) -> Void
    ) {
        // See sendMessage for the `failureReason` rationale.
        self.getConversationFromId(conversationId: conversationId) { [weak self] conversation in
            guard let self = self else { return }
            guard let conversation = conversation else {
                print("Conversation not found for id: \(conversationId)")
                completion(TCHResult(), nil, "Conversation not found for id: \(conversationId)")
                return
            }
            // Wait for sync before reading the message list — getLastMessages
            // returns empty / errors when called before .all.
            self.runWhenConversationSynchronized(conversation, onReady: {
                // Outer watchdog protects the prerequisite getLastMessages
                // call. Without it a dropped lookup hangs the Dart Future.
                let lookupShot = OneShot(registry: self.oneShotRegistry)
                lookupShot.arm(seconds: 30) {
                    completion(TCHResult(), nil, "Sync error: body getLastMessages timed out after 30s")
                }
                conversation.getLastMessages(withCount: 1000) { result, messages in
                    DispatchQueue.main.async {
                        guard lookupShot.complete() else { return }
                        guard result.isSuccessful, let messages = messages as? [TCHMessage] else {
                            let err = result.resultText ?? "Unknown error"
                            print("Failed to load messages: \(err)")
                            completion(result, nil, "getLastMessages: \(err)")
                            return
                        }
                        guard let targetMessage = messages.first(where: { $0.sid == msgId }) else {
                            print("Message not found for sid: \(msgId)")
                            completion(TCHResult(), nil, "msg_not_found: \(msgId)")
                            return
                        }
                        // Stage 1: updateBody. Per-call watchdog protects
                        // against a dropped updateBody callback.
                        let stage1 = OneShot(registry: self.oneShotRegistry)
                        stage1.arm(seconds: 30) {
                            completion(TCHResult(), nil, "Sync error: body updateBody timed out after 30s")
                        }
                        targetMessage.updateBody(messageText) { updateResult in
                            DispatchQueue.main.async {
                                guard stage1.complete() else { return }
                                guard updateResult.isSuccessful else {
                                    completion(updateResult, nil, updateResult.resultText ?? "updateBody failed")
                                    return
                                }
                                // Skip setAttributes when the Dart caller
                                // didn't supply attributes (matches Android
                                // body() and avoids overwriting with {}).
                                guard let attributes = attributes else {
                                    completion(updateResult, targetMessage, nil)
                                    return
                                }
                                // Stage 2: setAttributes. Separate watchdog
                                // so a slow attrs write doesn't share the
                                // stage1 budget.
                                let stage2 = OneShot(registry: self.oneShotRegistry)
                                stage2.arm(seconds: 30) {
                                    completion(TCHResult(), targetMessage, "Sync error: body setAttributes timed out after 30s")
                                }
                                let attributesObject = TCHJsonAttributes(dictionary: attributes)
                                targetMessage.setAttributes(attributesObject, completion: { attrResult in
                                    DispatchQueue.main.async {
                                        guard stage2.complete() else { return }
                                        if attrResult.isSuccessful {
                                            completion(attrResult, targetMessage, nil)
                                        } else {
                                            completion(attrResult, nil, attrResult.resultText ?? "setAttributes failed")
                                        }
                                    }
                                })
                            }
                        }
                    }
                }
            }, onFailed: { msg in
                print("body(): sync wait failed: \(msg)")
                completion(TCHResult(), nil, "Sync error: \(msg)")
            })
        }
    }

    // MARK: - Update Multiple Messages
    func updateMessages(
        conversationId: String,
        messages: [[String: Any]],
        completion: @escaping ([String: Any]) -> Void
    ) {
        guard !messages.isEmpty else {
            let responseMap: [String: Any] = [
                "success": [],
                "errors": [],
                "totalSuccess": 0,
                "totalErrors": 0
            ]
            completion(responseMap)
            return
        }

        self.getConversationFromId(conversationId: conversationId) { [weak self] conversation in
            guard let self = self else { return }
            guard let conversation = conversation else {
                let errorResponse: [String: Any] = [
                    "error": "Conversation not found for id: \(conversationId)"
                ]
                completion(errorResponse)
                return
            }
            self.runWhenConversationSynchronized(conversation, onReady: {
            // Outer watchdog: protect the prerequisite getLastMessages
            // call. If it drops, we never enter the per-message loop
            // and no per-message OneShot is ever armed, so the Dart
            // Future would hang forever without this guard.
            let outerShot = OneShot(registry: self.oneShotRegistry)
            outerShot.arm(seconds: 30) {
                completion(["error": "Sync error: updateMessages getLastMessages timed out after 30s"])
            }
            conversation.getLastMessages(withCount: 1000) { result, messagesList in
                DispatchQueue.main.async {
                guard result.isSuccessful, let messagesList = messagesList as? [TCHMessage] else {
                    guard outerShot.complete() else { return }
                    let errorResponse: [String: Any] = [
                        "error": "Failed to load messages: \(result.resultText ?? "Unknown error")"
                    ]
                    completion(errorResponse)
                    return
                }
                guard outerShot.complete() else { return }

                var successList: [String] = []
                var errorList: [String] = []
                let dispatchGroup = DispatchGroup()

                // วนลูปผ่านแต่ละ message ที่ต้องการอัปเดต
                for messageData in messages {
                    guard let msgId = messageData["msgId"] as? String,
                          let newBody = messageData["message"] as? String else {
                        errorList.append("Invalid data: msgId or message is null")
                        continue
                    }

                    // 🔍 หา message ที่มี sid ตรงกับ msgId
                    guard let targetMessage = messagesList.first(where: { $0.sid == msgId }) else {
                        errorList.append("\(msgId): Message not found")
                        continue
                    }

                    dispatchGroup.enter()

                    // สร้าง attributes (ถ้ามี)
                    var attributesObject: TCHJsonAttributes?
                    if let newAttribute = messageData["attribute"] as? [String: Any] {
                        attributesObject = TCHJsonAttributes(dictionary: newAttribute)
                    }

                    // Per-message watchdog: if either updateBody or
                    // setAttributes silently drops its callback, the group
                    // would never reach .notify and updateMessages would
                    // hang the Dart Future. Cap each message at 30s and
                    // log a timeout error so the rest of the batch can
                    // still resolve.
                    let perMessageShot = OneShot(registry: self.oneShotRegistry)
                    perMessageShot.arm(seconds: 30) {
                        errorList.append("\(msgId): timed out after 30s")
                        dispatchGroup.leave()
                    }
                    targetMessage.updateBody(newBody) { updateResult in
                        DispatchQueue.main.async {
                            if updateResult.isSuccessful {
                                if let attributes = attributesObject {
                                    // Gate the setAttributes launch on the
                                    // perMessageShot still being live, so a
                                    // late updateBody callback that arrives
                                    // after the timeout fired doesn't issue
                                    // an extra SDK write that Dart never
                                    // hears about. If we won the race, the
                                    // perMessageShot is already consumed;
                                    // arm stage2 for setAttributes.
                                    guard perMessageShot.complete() else {
                                        // Timer already fired and left the
                                        // group; discard this late callback.
                                        return
                                    }
                                    let stage2 = OneShot(registry: self.oneShotRegistry)
                                    stage2.arm(seconds: 30) {
                                        errorList.append("\(msgId): setAttributes timed out after 30s")
                                        dispatchGroup.leave()
                                    }
                                    targetMessage.setAttributes(attributes) { attrResult in
                                        DispatchQueue.main.async {
                                            guard stage2.complete() else { return }
                                            if attrResult.isSuccessful {
                                                successList.append(msgId)
                                            } else {
                                                errorList.append("\(msgId): setAttributes error - \(attrResult.resultText ?? "Unknown error")")
                                            }
                                            dispatchGroup.leave()
                                        }
                                    }
                                } else {
                                    guard perMessageShot.complete() else { return }
                                    successList.append(msgId)
                                    dispatchGroup.leave()
                                }
                            } else {
                                guard perMessageShot.complete() else { return }
                                errorList.append("\(msgId): updateBody error - \(updateResult.resultText ?? "Unknown error")")
                                dispatchGroup.leave()
                            }
                        }
                    }
                }

                // รอให้ทุก message อัปเดตเสร็จ
                dispatchGroup.notify(queue: .main) {
                    let responseMap: [String: Any] = [
                        "success": successList,
                        "errors": errorList,
                        "totalSuccess": successList.count,
                        "totalErrors": errorList.count
                    ]
                    completion(responseMap)
                }
                } // close DispatchQueue.main.async
            } // close getLastMessages closure
            }, onFailed: { msg in
                print("updateMessages(): sync wait failed: \(msg)")
                completion(["error": "Sync error: \(msg)"])
            })
        }
    }

    // MARK: - Set Typing Status
    func setTypingStatus(
        conversationId: String,
        isTyping: Bool,
        completion: @escaping (String) -> Void
    ) {
        self.getConversationFromId(conversationId: conversationId) { conversation in
            guard let conversation = conversation else {
                print("Conversation not found for id: \(conversationId)")
                // Preserve the legacy contract: typing is fire-and-forget,
                // and anyvet's Dart bridge string-matches "started"/"ended"
                // to drive the local indicator. Returning "Conversation not
                // found" or "Sync error: ..." silently desyncs that
                // indicator. Always treat the off-path as "ended" so a
                // stuck-on typing dot self-clears — returning "started"
                // here would arm an indicator that never gets cleared
                // because there's no conversation to deliver a future
                // typingEnded event.
                completion("ended")
                return
            }
            // typing() is a fire-and-forget SDK call with no completion
            // and no documented IllegalStateException-equivalent on iOS;
            // calling it on an un-synced conversation is at worst a no-op.
            // Funneling through runWhenConversationSynchronized would
            // either (a) block per-keystroke typing for up to 30 s, or
            // (b) return "Sync error" which anyvet's string-match doesn't
            // recognise — both regressions. Call directly.
            if isTyping {
                conversation.typing()
                print("Typing started for conversationId: \(conversationId)")
                completion("started")
            } else {
                print("Typing ended for conversationId: \(conversationId)")
                completion("ended")
            }
        }
    }


    func sendMessageWithMedia(conversationId: String,
                              messageText: String,
                              attributes: [String: Any]?,
                              mediaFilePath : String,
                              mimeType : String,
                              fileName :String ,
                              completion: @escaping (TCHResult, TCHMessage?, String?) -> Void) {
        // See sendMessage for the `failureReason` rationale.
        self.getConversationFromId(conversationId: conversationId) { [weak self] conversation in
            guard let self = self else { return }
            guard let conversation = conversation else {
                print("Conversation not found for id: \(conversationId)")
                completion(TCHResult(), nil, "Conversation not found for id: \(conversationId)")
                return
            }

            // Check file existence/readability BEFORE waiting for sync so a
            // missing/unreadable file fails immediately instead of being
            // parked behind the 30s sync watcher.
            // (`InputStream(fileAtPath:)` itself only allocates the stream
            // and does not detect missing files — the underlying open
            // happens lazily during upload, so the check above is what
            // actually catches the fail-fast case.)
            guard FileManager.default.isReadableFile(atPath: mediaFilePath) else {
                print("Media file not readable at path: \(mediaFilePath)")
                completion(TCHResult(), nil, "Media file not readable at path: \(mediaFilePath)")
                return
            }

            self.runWhenConversationSynchronized(conversation, onReady: {
                // Open the stream lazily inside onReady so we don't hold the
                // stream object alive across the 30s sync wait window.
                guard let fileInputStream = InputStream(fileAtPath: mediaFilePath) else {
                    print("Error opening media file at path: \(mediaFilePath)")
                    completion(TCHResult(), nil, "Error opening media file at path: \(mediaFilePath)")
                    return
                }
                let builder = conversation.prepareMessage().setBody(messageText)
                if let attributes = attributes {
                    var attrError: NSError?
                    let attributesObject = TCHJsonAttributes(dictionary: attributes)
                    _ = builder.setAttributes(attributesObject, error: &attrError)
                    if let attrError = attrError {
                        print("sendMessageWithMedia: setAttributes rejected: \(attrError.localizedDescription)")
                    }
                }
                // Larger media uploads can legitimately exceed the 30s
                // text-send timeout. Use 5 minutes here as the outer bound
                // for "the SDK silently dropped the buildAndSend callback"
                // — long enough that real slow-network uploads complete,
                // short enough that a stuck send eventually resolves.
                let shot = OneShot(registry: self.oneShotRegistry)
                shot.arm(seconds: 300) {
                    completion(TCHResult(), nil, "sendMessageWithMedia timed out after 300s")
                }
                builder.addMedia(inputStream: fileInputStream, contentType: mimeType, filename: fileName, listener: MediaMessageListener(
                        onStarted: {
                            print("Media upload started.")
                        },
                        onProgress: { bytesSent in
                            print("Media upload progress: \(bytesSent) bytes sent.")
                        },
                        onCompleted: { mediaSid in
                            print("Media uploaded successfully with SID: \(mediaSid)")
                        },
                        onFailed: { error in
                            print("Media upload failed: \(error.localizedDescription )")
                        }
                    ))
                    .buildAndSend(completion: { tchResult, tchMessages in
                        DispatchQueue.main.async {
                            guard shot.complete() else { return }
                            if tchResult.isSuccessful {
                                completion(tchResult, tchMessages, nil)
                            } else {
                                completion(tchResult, tchMessages, tchResult.resultText)
                            }
                        }
                    })
            }, onFailed: { msg in
                print("sendMessageWithMedia: sync wait failed: \(msg)")
                completion(TCHResult(), nil, "Sync error: \(msg)")
            })
        }
    }


    func loginWithAccessToken(_ token: String, completion: @escaping (TCHResult?) -> Void) {
        // Re-arm the OneShot registry on .main BEFORE the client comes back —
        // shutdownClient leaves it in a drained state, and the plugin reuses
        // a single handler instance across logout/login. Without this, every
        // SDK call in the new session would short-circuit to its timeout
        // fallback because OneShot.arm's register() would still return false.
        let resetRegistry = { [weak self] in
            self?.oneShotRegistry.reset()
        }
        if Thread.isMainThread {
            resetRegistry()
        } else {
            DispatchQueue.main.sync(execute: resetRegistry)
        }
        // Set up Twilio Conversations client
        TwilioConversationsClient.conversationsClient(withToken: token,
                                                      properties: nil,
                                                      delegate: self) { (result, client) in
            self.client = client
            self.clientDelegate?.onClientSynchronizationChanged(status: ["status" : client?.synchronizationStatus.rawValue ?? -1])
            print("\(client?.synchronizationStatus.rawValue ?? -1)")
            //            self.client?.delegate?.conversationsClient?(<#T##client: TwilioConversationsClient##TwilioConversationsClient#>, synchronizationStatusUpdated: TCHClientSynchronizationStatus)
            completion(result)
        }
    }

    func shutdown() {
        if let client = client {
            client.delegate = nil
            client.shutdown()
            self.client = nil
        }
    }

    func createConversation(uniqueConversationName:String,_ completion: @escaping (Bool, TCHConversation?,String) -> Void) {
        guard isClientInitialized(), let client = client else {
            completion(false, nil, Strings.clientNotInitialized)
            return
        }
        // Create the conversation if it hasn't been created yet
        let options: [String: Any] = [
            TCHConversationOptionUniqueName: uniqueConversationName,
            TCHConversationOptionFriendlyName: uniqueConversationName,
        ]
        client.createConversation(options: options) { (result, conversation) in
            if result.isSuccessful {
                completion(result.isSuccessful, conversation,result.resultText ?? "Conversation created.")
            } else {
                completion(false, conversation,result.error?.localizedDescription ?? "Conversation NOT created.")
            }
        }
    }

    func getConversations(_ completion: @escaping([TCHConversation]) -> Void) {
        guard isClientInitialized(), let client = client else {
            completion([])
            return
        }

        completion(client.myConversations() ?? [])
    }

    func getParticipants(conversationId:String,_ completion: @escaping([TCHParticipant]) -> Void) {
        self.getConversationFromId(conversationId: conversationId) { conversation in
            completion(conversation?.participants() ?? [])
        }
    }

    func addParticipants(conversationId:String,participantName:String,_ completion: @escaping(TCHResult?, String?) -> Void) {
        // Second tuple slot carries a distinct failure reason — timeout or
        // sync-wait failure must not be confused with a nil status from
        // an actually-missing conversation. Plugin handler picks the
        // reason over the nil-result fallback.
        self.getConversationFromId(conversationId: conversationId) { [weak self] conversation in
            guard let self = self else {
                completion(nil, "handler released")
                return
            }
            guard let conversation = conversation else {
                print("Conversation not found for id: \(conversationId)")
                completion(nil, "Conversation not found for id: \(conversationId)")
                return
            }
            self.runWhenConversationSynchronized(conversation, onReady: {
                let shot = OneShot(registry: self.oneShotRegistry)
                shot.arm(seconds: 30) {
                    print("addParticipants: timed out after 30s")
                    completion(nil, "addParticipant timed out after 30s")
                }
                conversation.addParticipant(byIdentity: participantName, attributes: nil, completion: { status in
                    DispatchQueue.main.async {
                        guard shot.complete() else { return }
                        completion(status, nil)
                    }
                })
            }, onFailed: { msg in
                print("addParticipants: sync wait failed: \(msg)")
                completion(nil, "Sync error: \(msg)")
            })
        }
    }

    func removeParticipants(conversationId:String,participantName:String,_ completion: @escaping(TCHResult?, String?) -> Void) {
        self.getConversationFromId(conversationId: conversationId) { [weak self] conversation in
            guard let self = self else {
                completion(nil, "handler released")
                return
            }
            guard let conversation = conversation else {
                print("Conversation not found for id: \(conversationId)")
                completion(nil, "Conversation not found for id: \(conversationId)")
                return
            }
            self.runWhenConversationSynchronized(conversation, onReady: {
                let shot = OneShot(registry: self.oneShotRegistry)
                shot.arm(seconds: 30) {
                    print("removeParticipants: timed out after 30s")
                    completion(nil, "removeParticipant timed out after 30s")
                }
                conversation.removeParticipant(byIdentity: participantName, completion: { status in
                    DispatchQueue.main.async {
                        guard shot.complete() else { return }
                        print("status->\(status)")
                        completion(status, nil)
                    }
                })
            }, onFailed: { msg in
                print("removeParticipants: sync wait failed: \(msg)")
                completion(nil, "Sync error: \(msg)")
            })
        }
    }


    func joinConversation(_ conversation: TCHConversation,_ completion: @escaping(String?) -> Void) {
        if conversation.status == .joined {
            completion(conversation.sid)
        } else {
            conversation.join(completion: { result in
                if result.isSuccessful {
                    completion(conversation.sid)
                } else {
                    completion(nil)
                }
            })
        }
    }

    func getConversationFromId(conversationId:String,_ completion: @escaping(TCHConversation?) -> Void){
        guard isClientInitialized(), let client = client else {
            print("getConversationFromId: client not initialized for id: \(conversationId)")
            completion(nil)
            return
        }
        // Watchdog: client.conversation(withSidOrUniqueName:) is itself
        // a Twilio SDK callback that can silently drop (transport
        // blackhole / mid-shutdown). Without this guard, every protected
        // entry point that routes through getConversationFromId would
        // hang at the gateway no matter how well its inner SDK calls
        // are wrapped. 30s budget — same as the per-call watchdogs
        // downstream.
        let shot = OneShot(registry: self.oneShotRegistry)
        shot.arm(seconds: 30) {
            print("getConversationFromId: Twilio lookup timed out after 30s for id: \(conversationId)")
            completion(nil)
        }
        client.conversation(withSidOrUniqueName: conversationId) { (result, conversation) in
            DispatchQueue.main.async {
                guard shot.complete() else { return }
                if conversation == nil {
                    print("getConversationFromId: Twilio lookup failed for id: \(conversationId) — isSuccessful=\(result.isSuccessful), resultText=\(result.resultText ?? "nil"), error=\(result.error?.localizedDescription ?? "nil")")
                }
                completion(conversation)
            }
        }
    }

    func loadPreviousMessages(_ conversation: TCHConversation,_ messageCount: UInt?,_ completion: @escaping([[String: Any]]?) -> Void) {
        print("synchronizationStatus->\(isClientInitialized())")
        guard isClientInitialized() else {
            completion([])
            return
        }
        runWhenConversationSynchronized(conversation, onReady: { [weak self] in
            guard let self = self else { return }
            // Watchdog: getLastMessages can silently drop its callback on
            // transport blackhole / mid-shutdown. Without this, Dart's
            // loadPreviousMessages Future hangs indefinitely.
            let shot = OneShot(registry: self.oneShotRegistry)
            shot.arm(seconds: 30) {
                print("loadPreviousMessages: getLastMessages timed out after 30s")
                completion([])
            }
            conversation.getLastMessages(withCount: messageCount ?? 1000) { (result, messages) in
                DispatchQueue.main.async {
                    guard shot.complete() else { return }
                    guard let messagesList = messages else {
                        completion([])
                        return
                    }
                    self.processMessagesSequentially(messagesList: messagesList, conversationSid: conversation.sid) { result in
                        completion(result)
                    }
                }
            }
        }, onFailed: { msg in
            print("loadPreviousMessages: sync wait failed: \(msg)")
            completion([])
        })
    }

    func processMessagesSequentially(
        messagesList: [TCHMessage],
        conversationSid: String? = nil,
        listOfMessagess: [[String: Any]] = [],
        completion: @escaping ([[String: Any]]) -> Void
    ) {
        var listOfMessagess = listOfMessagess // Create a local copy to modify

        var index = 0 // Start index

        func processNextMessage() {
            if index < messagesList.count { // Ensure we're within bounds
                self.getMessageInDictionary(messagesList[index], conversationSid: conversationSid) { messageDictionary in
                    if let messageDict = messageDictionary {
                        listOfMessagess.append(messageDict)
                    }
                    index += 1 // Increment the index

                    // Dispatch recursively to avoid stack overflow
                    DispatchQueue.main.async {
                        processNextMessage() // Process the next message
                    }
                }
            } else {
                // All messages have been processed
                completion(listOfMessagess) // Return the final list
            }
        }

        processNextMessage() // Start processing
    }


    func processMessagesSequentiallyForParticipants(_ conversation: TCHConversation,
                                                    messagesList: [TCHMessage],
                                                    listOfMessagess: [[String: Any]] = [],
                                                    completion: @escaping ([[String: Any]]) -> Void
    ) {
        var listOfMessagess = listOfMessagess // Create a local copy to modify

        var index = 0 // Start index

        func processNextMessage() {
            if index < messagesList.count { // Ensure we're within bounds
                self.getMessageInDictionaryWithMsg(conversation,messagesList[index]) { messageDictionary in
                    if let messageDict = messageDictionary {
                        listOfMessagess.append(messageDict)
                    }
                    index += 1 // Increment the index

                    // Dispatch recursively to avoid stack overflow
                    DispatchQueue.main.async {
                        processNextMessage() // Process the next message
                    }
                }
            } else {
                // All messages have been processed
                completion(listOfMessagess) // Return the final list
            }
        }

        processNextMessage() // Start processing
    }


    func getLastMessage(_ conversation: TCHConversation,_ messageCount: UInt?,_ completion: @escaping([[String: Any]]?) -> Void) {
        print("synchronizationStatus->\(isClientInitialized())")
        guard isClientInitialized() else {
            completion([])
            return
        }
        runWhenConversationSynchronized(conversation, onReady: { [weak self] in
            guard let self = self else { return }
            // Watchdog: same drop-risk as loadPreviousMessages above.
            let shot = OneShot(registry: self.oneShotRegistry)
            shot.arm(seconds: 30) {
                print("getLastMessage: getLastMessages timed out after 30s")
                completion([])
            }
            conversation.getLastMessages(withCount: messageCount ?? 1) { (result, messages) in
                DispatchQueue.main.async {
                    guard shot.complete() else { return }
                    guard let messagesList = messages else {
                        completion([])
                        return
                    }
                    self.processMessagesSequentiallyForParticipants(conversation, messagesList: messagesList) { result in
                        completion(result)
                    }
                }
            }
        }, onFailed: { msg in
            print("getLastMessage: sync wait failed: \(msg)")
            completion([])
        })
    }


    func getUnReadMsgCount(conversationId: String, _ completion: @escaping ([[String: Any]]?) -> Void) {
        var list: [[String: Any]] = []

        self.getConversationFromId(conversationId: conversationId) { [weak self] conversation in
            guard let self = self else { return }
            var dictionary: [String: Any] = [:]
            guard let conversation = conversation else {
                print("Conversation not found for id: \(conversationId)")
                dictionary["sid"] = conversationId
                dictionary["unReadCount"] = 0
                list.append(dictionary)
                completion(list)
                return
            }
            self.runWhenConversationSynchronized(conversation, onReady: { [weak self] in
                guard let self = self else { return }
                // Watchdog: getUnreadMessagesCount can silently drop on
                // transport blackhole / mid-shutdown — Dart's
                // getUnReadMsgCount Future would otherwise hang forever.
                // On timeout we surface a zero count rather than nothing
                // so the caller's UI badge resolves to a safe default.
                let shot = OneShot(registry: self.oneShotRegistry)
                shot.arm(seconds: 30) {
                    print("getUnReadMsgCount: getUnreadMessagesCount timed out after 30s")
                    var errorDict: [String: Any] = [:]
                    errorDict["sid"] = conversationId
                    errorDict["unReadCount"] = 0
                    list.removeAll()
                    list.append(errorDict)
                    completion(list)
                }
                conversation.getUnreadMessagesCount(completion: { result, count in
                    DispatchQueue.main.async {
                        guard shot.complete() else { return }
                        if result.isSuccessful {
                            list.removeAll()
                            print("Total Unread Count \(count)")
                            dictionary["sid"] = conversationId
                            dictionary["unReadCount"] = count
                            list.append(dictionary)
                            completion(list)
                        }
                        else{
                            print("No Unread Count")
                            dictionary["sid"] = conversationId
                            dictionary["unReadCount"] = 0
                            list.append(dictionary)
                            completion(list)
                        }
                    }
                })
            }, onFailed: { msg in
                print("getUnReadMsgCount: sync wait failed: \(msg)")
                var errorDict: [String: Any] = [:]
                errorDict["sid"] = conversationId
                errorDict["unReadCount"] = 0
                list.removeAll()
                list.append(errorDict)
                completion(list)
            })
        }
    }



    func getMessageInDictionary(_ message: TCHMessage, conversationSid: String? = nil, _ completion: @escaping ([String: Any]?) -> Void) {
        var dictionary: [String: Any] = [:]
        var attachedMedia: [[String: Any]] = []

        dictionary["sid"] = message.sid
        dictionary["author"] = message.author
        dictionary["body"] = message.body
        if let convSid = conversationSid {
            dictionary["conversationSid"] = convSid
        }

        do {
            let attributes = message.attributes()?.dictionary ?? [:]
            let jsonData = try JSONSerialization.data(withJSONObject: attributes, options: .prettyPrinted)
            if let jsonString = String(data: jsonData, encoding: .utf8) {
                dictionary["attributes"] = jsonString
            }
        } catch {
            print("Error converting dictionary to string: \(error.localizedDescription)")
            dictionary["attributes"] = ""
        }


        // dictionary["lastMessageDate"] = formatLastMessageDateISO8601(lastMessageDateString: message.dateUpdated?.description ?? "")
        dictionary["dateCreated"] = message.dateCreated?.description ?? ""
        dictionary["lastMessage"] = message.body

        // Fetch media details
        let mediaItems = message.getMedia(by: Set([MediaCategory.media]))
        if mediaItems.isEmpty {
            completion(dictionary) // No media, complete immediately
            return
        }

        let mediaDispatchGroup = DispatchGroup()

        for media in mediaItems {
            mediaDispatchGroup.enter()

            var mediaMap: [String: Any] = [:]
            mediaMap["sid"] = media.sid
            mediaMap["contentType"] = media.contentType
            mediaMap["filename"] = media.filename

            // Per-media watchdog: if Twilio's getTemporaryContentUrl callback
            // never fires (transport blackhole / mid-shutdown), the
            // mediaDispatchGroup would never leave and the surrounding
            // loadPreviousMessages / getLastMessage would hang the Dart
            // Future indefinitely. Cap at 30s with an empty URL fallback.
            let shot = OneShot(registry: self.oneShotRegistry)
            shot.arm(seconds: 30) {
                mediaMap["mediaUrl"] = ""
                attachedMedia.append(mediaMap)
                mediaDispatchGroup.leave()
            }
            media.getTemporaryContentUrl { result, tempUrl in
                DispatchQueue.main.async {
                    guard shot.complete() else { return }
                    mediaMap["mediaUrl"] = tempUrl?.absoluteString ?? ""
                    attachedMedia.append(mediaMap)
                    mediaDispatchGroup.leave()
                }
            }
        }

        mediaDispatchGroup.notify(queue: .main) {
            dictionary["attachMedia"] = attachedMedia
            completion(dictionary) // Complete after all media details are processed
        }
    }

    func getMessageInDictionaryWithMsg(_ conversation: TCHConversation,_ message: TCHMessage, _ completion: @escaping ([String: Any]?) -> Void) {
        var dictionary: [String: Any] = [:]
        var attachedParticipantsData: [[String: Any]] = []

        dictionary["sid"] = message.participantSid
        dictionary["author"] = message.author
        dictionary["body"] = message.body

        do {
            let attributes = message.attributes()?.dictionary ?? [:]
            let jsonData = try JSONSerialization.data(withJSONObject: attributes, options: .prettyPrinted)
            if let jsonString = String(data: jsonData, encoding: .utf8) {
                dictionary["attributes"] = jsonString
            }
        } catch {
            print("Error converting dictionary to string: \(error.localizedDescription)")
            dictionary["attributes"] = ""
        }

        let participantsDispatchGroup = DispatchGroup()

        dictionary["lastMessageDate"] = formatLastMessageDateISO8601(lastMessageDateString: message.dateUpdated?.description ?? "")
        dictionary["dateCreated"] = message.dateCreated?.description ?? ""
        dictionary["lastMessage"] = message.body
        dictionary["mediaCount"] = message.attachedMedia.count
        dictionary["participantsCount"] = conversation.participants().count
        dictionary["isGroup"] = conversation.participants().count > 2
        dictionary["lastReadIndex"] = conversation.lastReadMessageIndex
        dictionary["lastMessageIndex"] = conversation.lastMessageIndex

        var friendlyIdentity = ""
        var friendlyName = ""

        guard let participant = message.participant else {
            completion(dictionary)
            return// Complete after all media details are processed
        }
      
        
        participantsDispatchGroup.enter()

        // Watchdog: subscribedUser is a Twilio async lookup that can drop
        // its callback (transport blackhole / mid-shutdown). Without this
        // guard, the surrounding getLastMessage would never resolve. On
        // timeout, surface a distinct sentinel so downstream UI can show
        // "Unknown sender (lookup failed)" rather than silently rendering
        // a blank author.
        var friendlyLookupFailed = false
        let shot = OneShot(registry: self.oneShotRegistry)
        shot.arm(seconds: 30) {
            friendlyLookupFailed = true
            participantsDispatchGroup.leave()
        }
        participant.subscribedUser { result, users in
            DispatchQueue.main.async {
                guard shot.complete() else { return }
                friendlyIdentity = users?.identity ?? ""
                friendlyName = users?.friendlyName ?? ""
                participantsDispatchGroup.leave()
            }
        }

        participantsDispatchGroup.notify(queue: .main) {
            dictionary["friendlyIdentity"] = friendlyIdentity
            dictionary["friendlyName"] = friendlyName
            // Surface lookup failure as a flag so Dart consumers can
            // distinguish "user exists but has no friendly name" from
            // "lookup never returned". Avoids silently rendering messages
            // from blank authors.
            if friendlyLookupFailed {
                dictionary["friendlyLookupFailed"] = true
            }
            print(dictionary)
            completion(dictionary) // Complete after all media details are processed
        }

    }




    func formatLastMessageDateISO8601(lastMessageDateString: String?) -> String? {
        // Create an ISO8601 date formatter for the input
        let inputFormatter = ISO8601DateFormatter()
        inputFormatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]

        // Create a standard date formatter for the desired output
        let outputFormatter = DateFormatter()
        outputFormatter.dateFormat = "yyyy-MM-dd HH:mm:ss Z"
        outputFormatter.timeZone = TimeZone(abbreviation: "UTC") // Convert to UTC

        // Parse the input date and format it to the desired output
        if let date = inputFormatter.date(from: lastMessageDateString ?? "") {
            let outputDateString = outputFormatter.string(from: date)
            print("lastMessageDateTime->\(outputDateString)")
            return outputDateString
        } else {
            print("Failed to parse date string")
            return nil
        }
    }

    // MARK: - Shutdown Client
    /// Shuts down and cleans up the Twilio Conversations Client
    /// Delete an entire conversation. Ported from ALAlliancetek fork
    /// (v0.4.0). Adapted to our completion-discipline patterns from PR #3:
    /// every path invokes completion exactly once, errors are surfaced
    /// rather than swallowed.
    func deleteConversation(conversationId: String, completion: @escaping (String) -> Void) {
        guard isClientInitialized() else {
            completion(Strings.clientNotInitialized)
            return
        }
        getConversationFromId(conversationId: conversationId) { [weak self] conversation in
            guard let self = self else {
                completion("conv_failed: handler released")
                return
            }
            guard let conversation = conversation else {
                completion("conv_failed: Conversation not found")
                return
            }
            // Watchdog: destroy() is a destructive SDK call; if its
            // callback silently drops (mid-shutdown / transport blackhole),
            // the Dart Future hangs forever and the user is left staring
            // at a delete spinner. Matches the deleteShot pattern in
            // deleteMessageWithSid below.
            let shot = OneShot(registry: self.oneShotRegistry)
            shot.arm(seconds: 30) {
                completion("delete_failed: destroy timed out after 30s")
            }
            conversation.destroy { result in
                DispatchQueue.main.async {
                    guard shot.complete() else { return }
                    if result.isSuccessful {
                        completion("success")
                    } else {
                        print("deleteConversation: \(result.error?.localizedDescription ?? "unknown")")
                        completion("delete_failed: \(result.error?.localizedDescription ?? "unknown")")
                    }
                }
            }
        }
    }

    /// Delete a single message by sid. Ported from ALAlliancetek fork
    /// (v0.4.1). Adapted to our fork:
    ///   - Client-init guard added (matches every other public entry).
    ///   - Empty messageSid rejected up front.
    ///   - All paths invoke completion exactly once (PR #3 discipline).
    ///   - getLastMessages call is now wrapped in
    ///     `runWhenConversationSynchronized` so the lookup waits for
    ///     `.all` sync status rather than reading an empty list.
    func deleteMessageWithSid(conversationId: String,
                              messageSid: String,
                              messageCount: Int,
                              completion: @escaping (String) -> Void) {
        guard isClientInitialized() else {
            completion(Strings.clientNotInitialized)
            return
        }
        guard !messageSid.isEmpty else {
            completion("failed: messageSid is required")
            return
        }
        let searchCount = max(messageCount, 1)
        getConversationFromId(conversationId: conversationId) { [weak self] conversation in
            guard let self = self else { return }
            guard let conversation = conversation else {
                completion("conv_failed: Conversation not found")
                return
            }
            self.runWhenConversationSynchronized(conversation, onReady: {
                // Two watchdogs in sequence: getLastMessages (30s) then
                // conversation.remove (30s). A single watchdog spanning
                // both would let a slow getLastMessages consume the
                // budget and racing the remove timer would risk
                // reporting "timed out" for a message that was actually
                // deleted (audit Finding 5).
                let lookupShot = OneShot(registry: self.oneShotRegistry)
                lookupShot.arm(seconds: 30) {
                    completion("Sync error: deleteMessageWithSid getLastMessages timed out after 30s")
                }
                conversation.getLastMessages(withCount: UInt(searchCount)) { result, messages in
                    DispatchQueue.main.async {
                        guard lookupShot.complete() else { return }
                        guard result.isSuccessful, let messages = messages else {
                            let err = result.error?.localizedDescription ?? "unknown"
                            print("deleteMessageWithSid: getLastMessages failed: \(err)")
                            completion("getLastMessages error: \(err)")
                            return
                        }
                        guard let message = messages.first(where: { $0.sid == messageSid }) else {
                            completion("msg_not_found: SID not in last \(searchCount) messages")
                            return
                        }
                        let deleteShot = OneShot(registry: self.oneShotRegistry)
                        deleteShot.arm(seconds: 30) {
                            completion("Sync error: deleteMessageWithSid remove timed out after 30s")
                        }
                        conversation.remove(message) { deleteResult in
                            DispatchQueue.main.async {
                                guard deleteShot.complete() else { return }
                                if deleteResult.isSuccessful {
                                    completion("success")
                                } else {
                                    let err = deleteResult.error?.localizedDescription ?? "unknown"
                                    print("deleteMessageWithSid: remove failed: \(err)")
                                    completion("delete_failed: \(err)")
                                }
                            }
                        }
                    }
                }
            }, onFailed: { msg in
                print("deleteMessageWithSid: sync wait failed: \(msg)")
                completion("Sync error: \(msg)")
            })
        }
    }

    func shutdownClient(completion: @escaping (String) -> Void) {
        // Always drain pending sync-wait registrations + watchdog OneShots
        // BEFORE nilling the client — even on the "already shutdown" branch
        // — so any in-flight Dart Future resolves now (with a defined error)
        // instead of waiting up to 30 s for its timer to fire after the
        // channel has been torn down (which would surface as 'Reply already
        // submitted' or a stale completion on a dead FlutterResult).
        drainSyncWaiters(reason: "client shutdown")
        drainOneShots()

        if let client = self.client {
            // Shutdown the client
            client.shutdown()
            self.client = nil

            // Clear delegates and subscriptions
            self.messageDelegate = nil
            self.clientDelegate = nil
            self.isSubscribe = nil
            self.conversationId = nil
            self.messageSubscriptionId = ""

            print("Twilio Conversations Client shutdown successfully")
            completion("Client shutdown successfully")
        } else {
            completion("Client already shutdown or not initialized")
        }
    }

    /// Invalidate every in-flight OneShot watchdog so its captured
    /// onTimeout cannot fire later against a torn-down FlutterResult.
    /// invalidate() is a no-op for already-resolved shots, so this is
    /// safe to call even after drainSyncWaiters has resolved the inner
    /// completion handlers some shots refer to. Also flips the registry's
    /// `drained` flag so any OneShot whose async arm-setup runs AFTER this
    /// drain (off-main construction racing shutdown) fires its onTimeout
    /// immediately instead of installing a post-shutdown timer.
    private func drainOneShots() {
        let drain = { [weak self] in
            guard let self = self else { return }
            self.oneShotRegistry.drainAll()
        }
        if Thread.isMainThread {
            drain()
        } else {
            DispatchQueue.main.async(execute: drain)
        }
    }

    /// Fail every in-flight SyncWaiter with the given reason and clear the
    /// registry + any pending timers. Must run on .main.
    ///
    /// We snapshot `syncWaiters` and clear it BEFORE iterating so that any
    /// user onFailed closure that re-enters the handler (registers a new
    /// sync-wait, calls shutdownClient again, etc.) operates against a clean
    /// dict rather than corrupting the iteration we're inside.
    private func drainSyncWaiters(reason: String) {
        let drain = { [weak self] in
            guard let self = self else { return }
            let pending = self.syncWaiters
            self.syncWaiters.removeAll()
            for waiters in pending.values {
                for waiter in waiters {
                    _ = waiter.fireFailed(reason)
                }
            }
        }
        if Thread.isMainThread {
            drain()
        } else {
            DispatchQueue.main.async(execute: drain)
        }
    }
}

/// Internal record of a single in-flight sync-wait registration. Owned by
/// `ConversationsHandler.syncWaiters` and the `DispatchSourceTimer` it
/// schedules. `handled` guards against the timer + the delegate callback
/// racing to deliver completion twice.
fileprivate final class SyncWaiter {
    let onReady: () -> Void
    let onFailed: (String) -> Void
    var handled: Bool = false
    var timer: DispatchSourceTimer?

    init(onReady: @escaping () -> Void, onFailed: @escaping (String) -> Void) {
        self.onReady = onReady
        self.onFailed = onFailed
    }

    /// Always invoked on .main. Returns true iff this call actually fired
    /// the completion (i.e. the waiter wasn't already handled by a peer).
    @discardableResult
    func fireReady() -> Bool {
        guard !handled else { return false }
        handled = true
        timer?.cancel()
        timer = nil
        onReady()
        return true
    }

    @discardableResult
    func fireFailed(_ message: String) -> Bool {
        guard !handled else { return false }
        handled = true
        timer?.cancel()
        timer = nil
        onFailed(message)
        return true
    }
}

/// Main-queue-only registry of in-flight OneShots. Wraps a weak NSHashTable
/// with a `drained` flag so OneShot.arm can atomically (a) register itself
/// and arm its timer, or (b) detect the registry has been drained since
/// construction and fire its onTimeout immediately.
///
/// All methods must be called on the main queue — the registry deliberately
/// does NOT do its own dispatch hop so callers that need to combine
/// registration with other main-queue mutations stay in one atomic step.
fileprivate final class OneShotRegistry {
    private let shots = NSHashTable<OneShot>.weakObjects()
    private var drained = false

    /// Register a shot. Returns false if drainAll() has already run,
    /// meaning the caller should fire its onTimeout NOW instead of arming.
    func register(_ shot: OneShot) -> Bool {
        dispatchPrecondition(condition: .onQueue(.main))
        guard !drained else { return false }
        shots.add(shot)
        return true
    }

    /// Mark the registry drained and invalidate every currently-armed shot.
    /// Subsequent register() calls return false until the registry is
    /// re-armed via reset(). The plugin keeps a single long-lived handler
    /// across logout/login, so reset() MUST be called when a new client
    /// session begins (see loginWithAccessToken); otherwise every
    /// post-shutdown SDK call would short-circuit to its timeout fallback.
    func drainAll() {
        dispatchPrecondition(condition: .onQueue(.main))
        drained = true
        let snapshot = shots.allObjects
        shots.removeAllObjects()
        for shot in snapshot {
            shot.invalidate()
        }
    }

    /// Re-arm the registry for a fresh client session. Clears the `drained`
    /// flag so subsequent OneShot.arm calls install timers normally.
    /// Called from loginWithAccessToken to support logout → login on the
    /// same handler instance (the plugin reuses one ConversationsHandler
    /// for the app lifetime).
    func reset() {
        dispatchPrecondition(condition: .onQueue(.main))
        drained = false
        shots.removeAllObjects()
    }
}

/// One-shot guard for a single SDK callback. Pair `arm` with `complete`
/// (or rely on the timer to fire `onTimeout` if `complete` never arrives).
///
/// Why: Twilio iOS callbacks occasionally drop on the floor (transport
/// blackhole, mid-shutdown race, deinit ordering). Without a watchdog,
/// any caller that awaits the completion hangs forever. Pair every
/// "the SDK promised to call back once" path with `OneShot` so that
/// either the SDK's callback or our timeout resolves the Dart Future
/// — never neither.
///
/// All state mutation is funneled through .main: `arm` and `complete`
/// hop the work onto the main queue, so callers may invoke them from
/// any thread (Twilio delivers some callbacks off-main).
fileprivate final class OneShot {
    private var handled = false
    private var timer: DispatchSourceTimer?
    private var pendingOnTimeout: (() -> Void)?
    private let registry: OneShotRegistry?

    /// Stash a reference to the drain registry. Registration is DEFERRED
    /// to arm() so that registration, drained-check, pendingOnTimeout
    /// install, and timer-resume all happen atomically inside one main-
    /// queue setup block. This eliminates the previous race where init's
    /// async `table.add` could be split from arm's async `setup` by a
    /// drainOneShots running on main in between, leaving an armed timer
    /// that fires post-shutdown.
    init(registry: OneShotRegistry? = nil) {
        self.registry = registry
    }

    /// Resolve the OneShot by firing onTimeout NOW (with the same
    /// "watchdog tripped" semantics). Used by shutdownClient so every
    /// in-flight watchdog resolves its Dart Future immediately rather
    /// than waiting up to 30 s for the timer to fire — and to break
    /// the gateway-hang case where an outer Dart Future (sendMessage,
    /// body, etc.) depends on getConversationFromId's onTimeout
    /// completion as its only fallback. A late SDK callback still
    /// guards on `shot.complete()` and is silently dropped, so we
    /// don't double-fire.
    func invalidate() {
        let run = { [weak self] in
            guard let self = self else { return }
            guard !self.handled else { return }
            self.handled = true
            self.timer?.cancel()
            self.timer = nil
            let toFire = self.pendingOnTimeout
            self.pendingOnTimeout = nil
            toFire?()
        }
        if Thread.isMainThread {
            run()
        } else {
            DispatchQueue.main.async(execute: run)
        }
    }

    deinit {
        // Defensive: libdispatch traps with "BUG IN CLIENT OF
        // LIBDISPATCH" if a resumed dispatch source is released without
        // cancel(). In normal flow either complete() (success path) or
        // the timer's eventHandler (timeout path) has already cancelled
        // and nil-ed the timer. This catches any edge path that
        // released OneShot without going through either.
        if let t = timer {
            t.cancel()
            timer = nil
        }
    }

    /// Schedule `onTimeout` to fire on .main after `seconds` unless
    /// `complete()` is called first. May only be armed once. Double-arm
    /// is logged but does NOT trap — a debug-build crash from a
    /// drainOneShots-vs-arm race during shutdown would be worse than
    /// the silent loss of watchdog coverage in the very narrow window
    /// where this can happen.
    func arm(seconds: TimeInterval, onTimeout: @escaping () -> Void) {
        let setup = { [weak self] in
            guard let self = self else { return }
            if self.timer != nil || self.handled {
                print("OneShot.arm() called twice or after complete()/invalidate() — second arm is a silent no-op; caller has lost watchdog coverage")
                return
            }
            // If a drainOneShots has already run since this OneShot was
            // constructed (off-main caller raced shutdown), skip arming
            // the timer entirely and fire onTimeout NOW so the caller's
            // leave()/completion runs immediately instead of 30 s post-
            // shutdown. The "registration + timer arm" pair is the
            // atomic critical section that closes the prior init→arm
            // race window.
            if let registry = self.registry, !registry.register(self) {
                self.handled = true
                onTimeout()
                return
            }
            // Store onTimeout on self so invalidate() can fire it from
            // outside the timer's eventHandler closure (used by
            // drainOneShots on shutdown to resolve the Dart Future now
            // rather than waiting up to 30 s).
            self.pendingOnTimeout = onTimeout
            let t = DispatchSource.makeTimerSource(queue: .main)
            t.schedule(deadline: .now() + seconds)
            // STRONG self capture is required for correctness — the
            // timer must keep OneShot alive for the full timeout window
            // even when the SDK silently drops its completion closure
            // (the exact failure mode this watchdog defends against).
            // With [weak self], the SDK callback closure was the only
            // strong holder of OneShot; on drop, OneShot deinits, the
            // timer fires with weak-self == nil, and onTimeout never
            // runs — defeating the entire watchdog.
            //
            // Cycle: self → timer → eventHandler → self. Broken in two
            // places: (1) completeOnMain() calls `timer?.cancel()` then
            // sets `timer = nil`; (2) the eventHandler below nils
            // self.timer before invoking onTimeout. Either path drops
            // the timer's strong ref to the eventHandler closure (and
            // its captured self), letting OneShot dealloc. The order
            // (cancel then nil) matters: cancelling a resumed source
            // before releasing the last strong ref avoids the libdispatch
            // "Release of a suspended object" trap.
            t.setEventHandler {
                guard !self.handled else { return }
                self.handled = true
                let firedTimer = self.timer
                self.timer = nil
                firedTimer?.cancel()
                let toFire = self.pendingOnTimeout
                self.pendingOnTimeout = nil
                toFire?()
            }
            self.timer = t
            t.resume()
        }
        if Thread.isMainThread {
            setup()
        } else {
            DispatchQueue.main.async(execute: setup)
        }
    }

    /// Returns true on .main if this is the winning resolution
    /// (the timeout had not already fired). The caller's success block
    /// should be wrapped in `if shot.complete() { ... }`.
    ///
    /// Always runs synchronously on .main so the caller's branch
    /// executes before any other main-queue work re-enters.
    @discardableResult
    func complete() -> Bool {
        if Thread.isMainThread {
            return self.completeOnMain()
        }
        var won = false
        DispatchQueue.main.sync {
            won = self.completeOnMain()
        }
        return won
    }

    private func completeOnMain() -> Bool {
        dispatchPrecondition(condition: .onQueue(.main))
        guard !handled else { return false }
        handled = true
        timer?.cancel()
        timer = nil
        pendingOnTimeout = nil
        return true
    }
}

extension String {
    var hexToData: Data? {
        // Ensure the string contains a valid hex format and even number of characters
        guard self.count % 2 == 0,
              self.range(of: "^[0-9a-fA-F]+$", options: .regularExpression) != nil else {
            return nil
        }

        // Convert the hex string to `Data`
        var data = Data()
        var index = startIndex
        while index < endIndex {
            let nextIndex = self.index(index, offsetBy: 2)
            if let byte = UInt8(self[index..<nextIndex], radix: 16) {
                data.append(byte)
            } else {
                return nil // Return nil if conversion fails
            }
            index = nextIndex
        }
        return data
    }
}
