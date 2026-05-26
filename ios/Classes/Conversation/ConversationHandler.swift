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



    //    MARK: raw
    func conversationsClient(_ client: TwilioConversationsClient, conversation: TCHConversation,
                             messageAdded message: TCHMessage) {

        var attachedMedia: [[String: Any]] = []
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

                for media in message.getMedia(by: Set([MediaCategory.media])) {
                    var mediaMap: [String: Any] = [:]

                    mediaMap["sid"] = media.sid
                    mediaMap["contentType"] = media.contentType
                    mediaMap["filename"] = media.filename

                    media.getTemporaryContentUrl { result, tempUrl in
                        mediaMap["mediaUrl"] = tempUrl
                        print("TempURL >>> \(tempUrl)")
                    }
                    attachedMedia.append(mediaMap)
                    updatedMessage["attachMedia"] = attachedMedia
                }

                if (isSubscribe ?? false && conversationId == conversation.sid ){
                    conversation.setLastReadMessageIndex(computedIndex) { result, index in
                        print("setLastReadMessageIndex \(result.description)")
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
                builder.buildAndSend(completion: { tchResult, tchMessages in
                    if tchResult.isSuccessful, let messageSid = tchMessages?.sid {
                        completion(tchResult, messageSid, nil)
                    } else {
                        completion(tchResult, nil, tchResult.resultText)
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
                conversation.getLastMessages(withCount: 1000) { result, messages in
                    guard result.isSuccessful, let messages = messages as? [TCHMessage] else {
                        let err = result.resultText ?? "Unknown error"
                        print("Failed to load messages: \(err)")
                        completion(result, nil, "getLastMessages: \(err)")
                        return
                    }
                    guard let targetMessage = messages.first(where: { $0.sid == msgId }) else {
                        print("Message not found for sid: \(msgId)")
                        // result.isSuccessful was true here — return a fresh
                        // TCHResult() so plugin.updateMessage hits the error
                        // branch instead of silently reporting "success".
                        completion(TCHResult(), nil, "msg_not_found: \(msgId)")
                        return
                    }
                    targetMessage.updateBody(messageText) { updateResult in
                        if updateResult.isSuccessful {
                            // Skip setAttributes when the Dart caller didn't
                            // supply attributes — matches Android's body()
                            // logic and avoids overwriting existing message
                            // attributes with an empty {} blob.
                            if let attributes = attributes {
                                let attributesObject = TCHJsonAttributes(dictionary: attributes)
                                targetMessage.setAttributes(attributesObject, completion: { attrResult in
                                    if attrResult.isSuccessful {
                                        completion(attrResult, targetMessage, nil)
                                    } else {
                                        completion(attrResult, nil, attrResult.resultText ?? "setAttributes failed")
                                    }
                                })
                            } else {
                                completion(updateResult, targetMessage, nil)
                            }
                        } else {
                            completion(updateResult, nil, updateResult.resultText ?? "updateBody failed")
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
            conversation.getLastMessages(withCount: 1000) { result, messagesList in
                guard result.isSuccessful, let messagesList = messagesList as? [TCHMessage] else {
                    let errorResponse: [String: Any] = [
                        "error": "Failed to load messages: \(result.resultText ?? "Unknown error")"
                    ]
                    completion(errorResponse)
                    return
                }

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

                    // ✅ อัปเดต body ก่อน
                    targetMessage.updateBody(newBody) { updateResult in
                        if updateResult.isSuccessful {
                            // ✅ จากนั้นอัปเดต attributes ต่อ (ถ้ามี)
                            if let attributes = attributesObject {
                                targetMessage.setAttributes(attributes) { attrResult in
                                    if attrResult.isSuccessful {
                                        successList.append(msgId)
                                    } else {
                                        errorList.append("\(msgId): setAttributes error - \(attrResult.resultText ?? "Unknown error")")
                                    }
                                    dispatchGroup.leave()
                                }
                            } else {
                                // ไม่มี attributes ให้ update, ถือว่าสำเร็จ
                                successList.append(msgId)
                                dispatchGroup.leave()
                            }
                        } else {
                            errorList.append("\(msgId): updateBody error - \(updateResult.resultText ?? "Unknown error")")
                            dispatchGroup.leave()
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
            }
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
                completion("Conversation not found")
                return
            }

            if isTyping {
                // ✅ เริ่มพิมพ์
                conversation.typing()
                print("Typing started for conversationId: \(conversationId)")
                completion("started")
            } else {
                // ✅ หยุดพิมพ์ (Twilio จะหมดอายุ typing เองภายใน ~5 วินาที)
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
                        if tchResult.isSuccessful {
                            completion(tchResult, tchMessages, nil)
                        } else {
                            completion(tchResult, tchMessages, tchResult.resultText)
                        }
                    })
            }, onFailed: { msg in
                print("sendMessageWithMedia: sync wait failed: \(msg)")
                completion(TCHResult(), nil, "Sync error: \(msg)")
            })
        }
    }


    func loginWithAccessToken(_ token: String, completion: @escaping (TCHResult?) -> Void) {
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

    func addParticipants(conversationId:String,participantName:String,_ completion: @escaping(TCHResult?) -> Void) {
        self.getConversationFromId(conversationId: conversationId) { conversation in
            guard let conversation = conversation else {
                print("Conversation not found for id: \(conversationId)")
                completion(nil)
                return
            }
            conversation.addParticipant(byIdentity: participantName, attributes: nil,completion: { status in
                completion(status)
            })
        }
    }

    func removeParticipants(conversationId:String,participantName:String,_ completion: @escaping(TCHResult?) -> Void) {
        self.getConversationFromId(conversationId: conversationId) { conversation in
            guard let conversation = conversation else {
                print("Conversation not found for id: \(conversationId)")
                completion(nil)
                return
            }
            conversation.removeParticipant(byIdentity: participantName,completion: { status in
                print("status->\(status)")
                completion(status)
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
        client.conversation(withSidOrUniqueName: conversationId) { (result, conversation) in
            if conversation == nil {
                print("getConversationFromId: Twilio lookup failed for id: \(conversationId) — isSuccessful=\(result.isSuccessful), resultText=\(result.resultText ?? "nil"), error=\(result.error?.localizedDescription ?? "nil")")
            }
            completion(conversation)
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
            conversation.getLastMessages(withCount: messageCount ?? 1000) { (result, messages) in
                guard let messagesList = messages else {
                    completion([])
                    return
                }
                self.processMessagesSequentially(messagesList: messagesList, conversationSid: conversation.sid) { result in
                    completion(result)
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
            conversation.getLastMessages(withCount: messageCount ?? 1) { (result, messages) in
                guard let messagesList = messages else {
                    completion([])
                    return
                }
                self.processMessagesSequentiallyForParticipants(conversation, messagesList: messagesList) { result in
                    completion(result)
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
            self.runWhenConversationSynchronized(conversation, onReady: {
                conversation.getUnreadMessagesCount(completion: { result, count in
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

            media.getTemporaryContentUrl { result, tempUrl in
                mediaMap["mediaUrl"] = tempUrl?.absoluteString ?? ""
                attachedMedia.append(mediaMap)
                mediaDispatchGroup.leave()
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

        
        participant.subscribedUser { result, users in
            friendlyIdentity = users?.identity ?? ""
            friendlyName = users?.friendlyName ?? ""
                participantsDispatchGroup.leave()
            }

        participantsDispatchGroup.notify(queue: .main) {
            dictionary["friendlyIdentity"] = friendlyIdentity
            dictionary["friendlyName"] = friendlyName

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
            guard self != nil else {
                completion("conv_failed: handler released")
                return
            }
            guard let conversation = conversation else {
                completion("conv_failed: Conversation not found")
                return
            }
            conversation.destroy { result in
                if result.isSuccessful {
                    completion("success")
                } else {
                    print("deleteConversation: \(result.error?.localizedDescription ?? "unknown")")
                    completion("delete_failed: \(result.error?.localizedDescription ?? "unknown")")
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
            conversation.getLastMessages(withCount: UInt(searchCount)) { result, messages in
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
                conversation.remove(message) { deleteResult in
                    if deleteResult.isSuccessful {
                        completion("success")
                    } else {
                        let err = deleteResult.error?.localizedDescription ?? "unknown"
                        print("deleteMessageWithSid: remove failed: \(err)")
                        completion("delete_failed: \(err)")
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
        // Always drain pending sync-wait registrations BEFORE nilling the
        // client — even on the "already shutdown" branch — so any in-flight
        // Dart Future resolves now (with a defined error) instead of waiting
        // up to 30 s for its timer to fire after the channel has been torn
        // down (which would surface as 'Reply already submitted' or a stale
        // completion on a dead FlutterResult).
        drainSyncWaiters(reason: "client shutdown")

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
