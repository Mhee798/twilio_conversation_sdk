import Flutter
import UIKit
import Foundation
import TwilioConversationsClient

// Per-channel stream handler so each EventChannel keeps its OWN sink. iOS
// previously made the plugin the single StreamHandler for ALL channels with one
// shared `eventSink`; whichever channel's onListen fired last won the sink, so
// live message events were delivered to the client-sync stream and the message
// listener never received them (receiving appeared dead on iOS). Mirrors the
// Android "A11" per-channel-sink fix.
class ChannelStreamHandler: NSObject, FlutterStreamHandler {
    private let onSink: (FlutterEventSink?) -> Void
    init(_ onSink: @escaping (FlutterEventSink?) -> Void) { self.onSink = onSink }
    public func onListen(withArguments arguments: Any?, eventSink events: @escaping FlutterEventSink) -> FlutterError? {
        onSink(events)
        return nil
    }
    public func onCancel(withArguments arguments: Any?) -> FlutterError? {
        onSink(nil)
        return nil
    }
}

public class TwilioConversationSdkPlugin: NSObject, FlutterPlugin {
    var conversationsHandler = ConversationsHandler()
    // Separate sink per channel (no more shared eventSink across channels).
    var messageEventSink: FlutterEventSink?
    var clientEventSink: FlutterEventSink?
    var syncEventSink: FlutterEventSink?
    var localConversation: TCHConversation?

    // Twilio delegate callbacks arrive on Twilio's own queues, but a
    // FlutterEventSink MUST be invoked on the platform (main) thread. Marshal
    // every emit onto main; this also serializes sink reads against onListen/
    // onCancel (which Flutter calls on main), so the plain sink fields are never
    // touched concurrently. Mirrors Android's postToMainOrLog + volatile sinks.
    private func emitMain(_ emit: @escaping () -> Void) {
        // Always hop async to main (never run inline). Besides satisfying
        // Flutter's platform-thread requirement, this preserves arrival order:
        // an event delivered on a Twilio queue is async-queued, so an event that
        // happens to arrive already on main must NOT run inline and overtake an
        // earlier emit still sitting in the main queue on the same sink.
        DispatchQueue.main.async(execute: emit)
    }
    //  public static func register(with registrar: FlutterPluginRegistrar) {
    //    let channel = FlutterMethodChannel(name: "twilio_conversation_sdk", binaryMessenger: registrar.messenger())
    //    let instance = TwilioConversationSdkPlugin()
    //    registrar.addMethodCallDelegate(instance, channel: channel)
    //  }
    
    public static func register(with registrar: FlutterPluginRegistrar) {
        let channel = FlutterMethodChannel(name: "twilio_conversation_sdk", binaryMessenger: registrar.messenger())
        let synchronizationStatusEventChannel = FlutterEventChannel(name: "twilio_conversation_sdk/synchronizationStatusEventChannel", binaryMessenger: registrar.messenger())
        let onClientSynchronizationChangedEventChannel = FlutterEventChannel(name: "twilio_conversation_sdk/onClientSynchronizationChanged", binaryMessenger: registrar.messenger())
        let messageEventChannel = FlutterEventChannel(name: "twilio_conversation_sdk/onMessageUpdated", binaryMessenger: registrar.messenger())
        let tokenEventChannel = FlutterEventChannel(name: "twilio_conversation_sdk/onTokenStatusChange", binaryMessenger: registrar.messenger())
        
        let instance = TwilioConversationSdkPlugin()
        registrar.addMethodCallDelegate(instance, channel: channel)

        // Each channel routes to its OWN sink so events never cross-route. The
        // channels (held by the binary messenger) retain these handlers, and the
        // handlers retain `instance` strongly — keeping it alive as long as any
        // channel can deliver. No retain cycle: conversationsHandler references
        // the plugin only through weak message/client delegates.
        messageEventChannel.setStreamHandler(ChannelStreamHandler { instance.messageEventSink = $0 })
        onClientSynchronizationChangedEventChannel.setStreamHandler(ChannelStreamHandler { instance.clientEventSink = $0 })
        tokenEventChannel.setStreamHandler(ChannelStreamHandler { instance.conversationsHandler.tokenEventSink = $0 })
        synchronizationStatusEventChannel.setStreamHandler(ChannelStreamHandler { instance.syncEventSink = $0 })
    }
    
    //  public func handle(_ call: FlutterMethodCall, result: @escaping FlutterResult) {
    //    switch call.method {
    //    case "getPlatformVersion":
    //      result("iOS " + UIDevice.current.systemVersion)
    //    default:
    //      result(FlutterMethodNotImplemented)
    //    }
    //  }
    
    public func handle(_ call: FlutterMethodCall, result: @escaping FlutterResult) {
        let arguments = call.arguments as? [String:Any]
        print("call->\(String(describing: call.method))")
        print("arguments->\(String(describing: arguments))")
        
        switch call.method {
        case Methods.generateToken:
            //          TwilioApi.requestTwilioAccessToken(identity:arguments?["identity"] as! String) { apiResult in
            //              switch apiResult {
            //              case .success(let accessToken):
            //                  result(accessToken)
            //              case .failure(let error):
            //                  print("Error requesting Twilio Access Token: \(error)")
            //                  result("")
            //              }
            //          }
            result(FlutterMethodNotImplemented)
            break
        case Methods.registerFCMToken:
            guard let fcmToken = arguments?["fcmToken"] as? String, !fcmToken.isEmpty else {
                result(FlutterError(code: "INVALID_ARGS", message: "registerFCMToken requires fcmToken:String", details: nil))
                break
            }
            conversationsHandler.registerFCMToken(token: fcmToken) { success in
                // I12: Twilio invokes this completion on an internal queue;
                // FlutterResult must be called on the platform thread.
                DispatchQueue.main.async {
                    if success {
                        result("Token Registerd")
                    } else {
                        result(FlutterError(code: "FCM_REGISTER_FAILED",
                                            message: "Failed to register FCM token",
                                            details: nil))
                    }
                }
            }
            break
        case Methods.unregisterFCMToken:
            guard let fcmToken = arguments?["fcmToken"] as? String, !fcmToken.isEmpty else {
                result(FlutterError(code: "INVALID_ARGS", message: "unregisterFCMToken requires fcmToken:String", details: nil))
                break
            }
            conversationsHandler.unregisterFCMToken(token: fcmToken) { success in
                // I12: dispatch the FlutterResult back onto the platform thread.
                DispatchQueue.main.async {
                    if success {
                        result("Token unregisterFCMToken")
                    } else {
                        result(FlutterError(code: "FCM_UNREGISTER_FAILED",
                                            message: "Failed to unregister FCM token",
                                            details: nil))
                    }
                }
            }
            break
        case Methods.updateAccessToken:
            guard let accessToken = arguments?["accessToken"] as? String, !accessToken.isEmpty else {
                result(FlutterError(code: "INVALID_ARGS", message: "updateAccessToken requires accessToken:String", details: nil))
                break
            }
            self.conversationsHandler.updateAccessToken(accessToken: accessToken) { tchResult in
                print("Methods.updateAccessToken->\(String(describing: tchResult))")
                var tokenStatus: [String: Any] = [:]
                if let tokenUpdateResult = tchResult {
                    if (tokenUpdateResult.resultCode == 200){
                        tokenStatus["statusCode"] = tokenUpdateResult.resultCode
                        tokenStatus["message"] = Strings.accessTokenRefreshed
                    }else {
                        tokenStatus["statusCode"] = tokenUpdateResult.resultCode
                        tokenStatus["message"] = tokenUpdateResult.resultText
                    }
                }
                result(tokenStatus)
            }
            break
        case Methods.initializeConversationClient:
            guard let accessToken = arguments?["accessToken"] as? String, !accessToken.isEmpty else {
                result(FlutterError(code: "INVALID_ARGS", message: "initializeConversationClient requires accessToken:String", details: nil))
                break
            }
            self.conversationsHandler.clientDelegate = self
            self.conversationsHandler.loginWithAccessToken(accessToken) { loginResult in
                // I12: the login completion fires from Twilio's client-creation
                // callback (not guaranteed main); reply on the platform thread.
                DispatchQueue.main.async {
                    guard let loginResultSuccessful: Bool = loginResult?.isSuccessful else {
                        result(Strings.authenticationFailed)
                        return
                    }
                    if(loginResultSuccessful) {
                        result(Strings.authenticationSuccessful)
                    } else {
                        result(Strings.authenticationFailed)
                    }
                }
            }
            break
        case Methods.createConversation:
            guard let conversationName = arguments?["conversationName"] as? String, !conversationName.isEmpty else {
                result(FlutterError(code: "INVALID_ARGS", message: "createConversation requires conversationName:String", details: nil))
                break
            }
            self.conversationsHandler.createConversation (uniqueConversationName: conversationName){ (success, conversation,status)  in
                if success, let conversation = conversation {
                    self.conversationsHandler.joinConversation(conversation) { joinConversationStatus in
                        if joinConversationStatus == nil {
                            print("createConversation: created \(conversation.sid ?? "?") but auto-join failed")
                        }
                    }
                    result(Strings.createConversationSuccess)
                }else {
                    if (status == Strings.conversationExists) {
                        result(Strings.conversationExists)
                    } else if (status == Strings.clientNotInitialized) {
                        result(Strings.clientNotInitialized)
                    } else {
                        result(Strings.createConversationFailure)
                    }
                }
            }
            break
        case Methods.getConversations:
            self.conversationsHandler.getConversations { conversationList in
                var listOfConversations: [[String: Any]] = []
                for conversation in conversationList {
                    var dictionary: [String: Any] = [:]
                    dictionary["conversationName"] = conversation.friendlyName
                    dictionary["sid"] = conversation.sid
                    dictionary["createdBy"] = conversation.createdBy
                    // I19: send a String, not a raw Date — FlutterStandardCodec
                    // rejects NSDate ("Unsupported value"). Matches lastMessageDate.
                    dictionary["dateCreated"] = conversation.dateCreated?.description
                    dictionary["lastReadIndex"] = conversation.lastReadMessageIndex
                    dictionary["lastMessageIndex"] = conversation.lastMessageIndex
                    if (conversation.lastMessageDate != nil){
                        dictionary["lastMessageDate"] = conversation.lastMessageDate?.description
                    }
                    dictionary["uniqueName"] = conversation.uniqueName
                    dictionary["participantsCount"] = conversation.participants().count
                    dictionary["isGroup"] = conversation.participants().count > 2
                    if (ConvertorUtility.isNilOrEmpty(dictionary["conversationName"]) == false && ConvertorUtility.isNilOrEmpty(dictionary["sid"]) == false){
                        listOfConversations.append(dictionary)
                    }
                    print(dictionary)
                }
                result(listOfConversations)
            }
            break
        case Methods.getParticipants:
            guard let conversationId = arguments?["conversationId"] as? String, !conversationId.isEmpty else {
                result(FlutterError(code: "INVALID_ARGS", message: "getParticipants requires conversationId:String", details: nil))
                break
            }
            var listOfParticipants: [[String:Any]] = []
            self.conversationsHandler.getParticipants(conversationId: conversationId) { participantsList in
                for user in participantsList {
                    var participant: [String: Any] = [:]
                    if (!ConvertorUtility.isNilOrEmpty(user.identity)) {
                        participant["identity"] = user.identity
                        participant["sid"] = user.sid
                        participant["conversationSid"] = user.conversation?.sid
                        participant["dateCreated"] = user.dateCreated?.description
                        participant["conversationCreatedBy"] = user.conversation?.createdBy
                        participant["isAdmin"] = (user.conversation?.createdBy == user.identity)
                        do {
                            // I20: avoid force-unwrap — a participant may have no
                            // attributes (matches getParticipantsWithName below).
                            let jsonData = try JSONSerialization.data(withJSONObject: user.attributes()?.dictionary ?? [:], options: .prettyPrinted)
                            if let jsonString = String(data: jsonData, encoding: .utf8) {
                                print(jsonString)
                                participant["attributes"] = jsonString

                            }
                        } catch {
                            print("Error converting dictionary to string: \(error.localizedDescription)")
                            participant["attributes"] = ""

                        }
                        listOfParticipants.append(participant)
                    }
                }
                result(listOfParticipants)
            }
            break
        case Methods.getParticipantsWithName:
            guard let conversationId = arguments?["conversationId"] as? String, !conversationId.isEmpty else {
                result(FlutterError(code: "INVALID_ARGS", message: "getParticipantsWithName requires conversationId:String", details: nil))
                break
            }
            var listOfParticipants: [[String: Any]] = []

            // Fetch participants for the provided conversation ID
            self.conversationsHandler.getParticipants(conversationId: conversationId) { participantsList in
                // Create a DispatchGroup to track asynchronous tasks
                let dispatchGroup = DispatchGroup()

                // Loop through each participant in the fetched list
                for user in participantsList {
                    var participant: [String: Any] = [:]

                    // Ensure identity is not nil or empty
                    if !ConvertorUtility.isNilOrEmpty(user.identity) {
                        participant["identity"] = user.identity
                        participant["sid"] = user.sid
                        participant["conversationSid"] = user.conversation?.sid
                        participant["dateCreated"] = user.dateCreated?.description
                        participant["conversationCreatedBy"] = user.conversation?.createdBy
                        participant["isAdmin"] = (user.conversation?.createdBy == user.identity)
                        // Handle attributes serialization
                        do {
                            let jsonData = try JSONSerialization.data(withJSONObject: user.attributes()?.dictionary ?? [:], options: .prettyPrinted)
                            if let jsonString = String(data: jsonData, encoding: .utf8) {
                                print(jsonString)
                                participant["attributes"] = jsonString
                            }
                        } catch {
                            // Handle error in serialization
                            print("Error converting dictionary to string: \(error.localizedDescription)")
                            participant["attributes"] = ""  // Provide empty string if serialization fails
                        }
                        // Enter the DispatchGroup before making the async call
                        dispatchGroup.enter()

                        // Call the subscribedUser method asynchronously
                        user.subscribedUser { result, users in
                            // Update participant data with the user details
                            participant["friendlyIdentity"] = users?.identity
                            participant["friendlyName"] = users?.friendlyName
                            // Add the participant to the list once the subscribedUser completes
                            listOfParticipants.append(participant)
                            
                            // Leave the DispatchGroup after finishing the async task
                            dispatchGroup.leave()
                        }
                    }
                }

                // Once all async tasks are finished, notify and return the result
                dispatchGroup.notify(queue: .main) {
                    result(listOfParticipants)  // Return the list of participants once everything is done
                }
            }
            break


            
 
        case Methods.addParticipant:
            guard let conversationId = arguments?["conversationId"] as? String,
                  let participantName = arguments?["participantName"] as? String, !participantName.isEmpty else {
                result(FlutterError(code: "INVALID_ARGS", message: "addParticipant requires conversationId:String, participantName:String", details: nil))
                break
            }
            self.conversationsHandler.addParticipants(conversationId: conversationId, participantName: participantName) { status, failureReason in
                guard let addParticipantStatus = status else {
                    // Prefer the handler's specific reason (timeout /
                    // sync-fail / handler-released) over the generic
                    // "Conversation not found" so retry/diagnostic logic
                    // upstream can distinguish them.
                    result(failureReason ?? "Conversation not found")
                    return
                }
                if (addParticipantStatus.isSuccessful){
                    result(Strings.addParticipantSuccess)
                }else {
                    result(addParticipantStatus.resultText)
                }
            }
            break
        case Methods.removeParticipant:
            guard let conversationId = arguments?["conversationId"] as? String,
                  let participantName = arguments?["participantName"] as? String, !participantName.isEmpty else {
                result(FlutterError(code: "INVALID_ARGS", message: "removeParticipant requires conversationId:String, participantName:String", details: nil))
                break
            }
            self.conversationsHandler.removeParticipants(conversationId: conversationId, participantName: participantName) { status, failureReason in
                guard let removeParticipantStatus = status else {
                    result(failureReason ?? "Conversation not found")
                    return
                }
                if (removeParticipantStatus.isSuccessful){
                    result(Strings.removedParticipantSuccess)
                }else {
                    result(removeParticipantStatus.resultText)
                }
            }
            break
        case Methods.joinConversation:
            guard let conversationId = arguments?["conversationId"] as? String, !conversationId.isEmpty else {
                result(FlutterError(code: "INVALID_ARGS", message: "joinConversation requires conversationId:String", details: nil))
                break
            }
            self.conversationsHandler.getConversationFromId(conversationId: conversationId) { conversation in
                guard let conversationFromId = conversation else {
                    result("Conversation not found")
                    return
                }
                self.conversationsHandler.joinConversation(conversationFromId) { tchConversationStatus in
                    result(tchConversationStatus)
                }
            }
        case Methods.getMessages:
            guard let conversationId = arguments?["conversationId"] as? String, !conversationId.isEmpty else {
                result(FlutterError(code: "INVALID_ARGS", message: "getMessages requires conversationId:String", details: nil))
                break
            }
            // Dart `int` arrives as NSNumber/Int — coerce through Int before
            // narrowing to UInt so large counts don't silently default to nil.
            let messageCount: UInt? = (arguments?["messageCount"] as? Int).flatMap { UInt(exactly: $0) }
            self.conversationsHandler.getConversationFromId(conversationId: conversationId) { conversation in
                guard let conversationFromId = conversation else {
                    result([])
                    return
                }
                // Previously: `self.conversationsHandler.conversationId = conversationId`.
                // Removed — the field is the source-of-truth for the
                // messageAdded auto-setLastReadMessageIndex gate (handler
                // line 71), and assigning it here both (a) widened the
                // race window to the full ~30s sync wait now wrapped
                // around loadPreviousMessages and (b) duplicated
                // subscribeToMessageUpdate's legitimate job. Callers that
                // need the auto-read-mark must subscribe first.
                self.conversationsHandler.loadPreviousMessages(conversationFromId, messageCount) { listOfMessages in
                    result(listOfMessages)
                }
            }
            break
        case Methods.getLastMessages:
            guard let conversationId = arguments?["conversationId"] as? String, !conversationId.isEmpty else {
                result(FlutterError(code: "INVALID_ARGS", message: "getLastMessages requires conversationId:String", details: nil))
                break
            }
            let messageCount: UInt? = (arguments?["messageCount"] as? Int).flatMap { UInt(exactly: $0) }
            self.conversationsHandler.getConversationFromId(conversationId: conversationId) { conversation in
                guard let conversationFromId = conversation else {
                    result([])
                    return
                }
                self.conversationsHandler.getLastMessage(conversationFromId, messageCount) { listOfMessages in
                    result(listOfMessages)
                }
            }
            break

        case Methods.getUnReadMsgCount:
            guard let conversationId = arguments?["conversationId"] as? String, !conversationId.isEmpty else {
                result(FlutterError(code: "INVALID_ARGS", message: "getUnReadMsgCount requires conversationId:String", details: nil))
                break
            }
            self.conversationsHandler.getUnReadMsgCount(conversationId: conversationId) { list in
                result(list)
            }
            break
            
        case Methods.sendMessage:
            guard let conversationId = arguments?["conversationId"] as? String,
                  let message = arguments?["message"] as? String else {
                result(FlutterError(code: "INVALID_ARGS", message: "sendMessage requires conversationId:String, message:String", details: nil))
                break
            }
            // Dart sometimes sends `null` for attribute when the caller has no
            // metadata — NSNull arrives, so `as? [String:Any]` is nil. Default
            // to empty dict instead of trapping.
            // Pass attribute through as Optional — handler skips setAttributes
            // when nil to match Android (and to avoid overwriting existing
            // message attributes with an empty {} blob on updateMessage).
            let attribute = arguments?["attribute"] as? [String: Any]
            self.conversationsHandler.sendMessage(conversationId: conversationId, messageText: message, attributes: attribute) { tchResult, tchMessages, failureReason in
                if (tchResult.isSuccessful){
                    result(tchMessages ?? "")
                }else {
                    let msg = failureReason ?? tchResult.resultText ?? "Conversation not found"
                    result(FlutterError(code: "SEND_FAILED",
                                        message: msg,
                                        details: failureReason))
                }
            }
            break
        case Methods.updateMessage:
            guard let conversationId = arguments?["conversationId"] as? String,
                  let msgId = arguments?["msgId"] as? String, !msgId.isEmpty,
                  let message = arguments?["message"] as? String else {
                result(FlutterError(code: "INVALID_ARGS", message: "updateMessage requires conversationId:String, msgId:String, message:String", details: nil))
                break
            }
            // Pass attribute through as Optional — handler skips setAttributes
            // when nil to match Android (and to avoid overwriting existing
            // message attributes with an empty {} blob on updateMessage).
            let attribute = arguments?["attribute"] as? [String: Any]
            self.conversationsHandler.body(conversationId: conversationId, msgId: msgId, messageText: message, attributes: attribute) { tchResult, tchMessages, failureReason in
                if (tchResult.isSuccessful){
                    result("success")
                }else {
                    // Surface the iOS-side reason rather than collapsing every
                    // failure to nil — failureReason carries sync timeout /
                    // msg_not_found / conv_failed diagnostics.
                    result(failureReason ?? tchResult.resultText ?? "failed")
                }
            }
            break
        case Methods.updateMessages:
            guard let conversationId = arguments?["conversationId"] as? String,
                  let messages = arguments?["messages"] as? [[String: Any]] else {
                result(FlutterError(code: "INVALID_ARGS", message: "updateMessages requires conversationId:String, messages:[[String:Any]]", details: nil))
                break
            }
            self.conversationsHandler.updateMessages(conversationId: conversationId, messages: messages) { responseMap in
                result(responseMap)
            }
            break
        case Methods.setTypingStatus:
            guard let conversationId = arguments?["conversationId"] as? String,
                  let isTyping = arguments?["isTyping"] as? Bool else {
                result(FlutterError(code: "INVALID_ARGS", message: "setTypingStatus requires conversationId:String, isTyping:Bool", details: nil))
                break
            }
            self.conversationsHandler.setTypingStatus(conversationId: conversationId, isTyping: isTyping) { status in
                result(status)
            }
            break
        case Methods.sendMessageWithMedia:
            guard let conversationId = arguments?["conversationId"] as? String,
                  let message = arguments?["message"] as? String,
                  let mediaFilePath = arguments?["mediaFilePath"] as? String, !mediaFilePath.isEmpty,
                  let mimeType = arguments?["mimeType"] as? String, !mimeType.isEmpty,
                  let fileName = arguments?["fileName"] as? String, !fileName.isEmpty else {
                result(FlutterError(code: "INVALID_ARGS", message: "sendMessageWithMedia requires conversationId, message, mediaFilePath, mimeType, fileName all as String", details: nil))
                break
            }
            // Pass attribute through as Optional — handler skips setAttributes
            // when nil to match Android (and to avoid overwriting existing
            // message attributes with an empty {} blob on updateMessage).
            let attribute = arguments?["attribute"] as? [String: Any]
            self.conversationsHandler.sendMessageWithMedia(conversationId: conversationId, messageText: message, attributes: attribute, mediaFilePath: mediaFilePath, mimeType: mimeType, fileName: fileName) { tchResult, tchMessages, failureReason in
                if (tchResult.isSuccessful){
                    print("sendMessageWithMedia send success")
                    result("send")
                }else {
                    let msg = failureReason ?? tchResult.resultText ?? "Conversation not found"
                    result(FlutterError(code: "SEND_FAILED",
                                        message: msg,
                                        details: failureReason))
                }
            }

//            self.conversationsHandler.sendMessage(conversationId: arguments?["conversationId"] as! String, messageText: arguments?["message"] as! String,
//                attributes: arguments?["attribute"] as! [String : Any]) { tchResult, tchMessages in
//                if (tchResult.isSuccessful){
//                    result("send")
//                }else {
//                    result(tchResult.resultText)
//                }

            break
        case Methods.subscribeToMessageUpdate:
            if let conversationId = arguments?["conversationId"] as? String {
                conversationsHandler.messageDelegate = self
                conversationsHandler.messageSubscriptionId = conversationId
                //MARK: TODO
                self.conversationsHandler.getConversationFromId(conversationId: conversationId) { conversation in
                    self.conversationsHandler.messageDelegate?.onSynchronizationChanged(status: ["status" : conversation?.synchronizationStatus.rawValue])

                    //MARK: setLastReadMessageIndex
                    conversation?.setLastReadMessageIndex(conversation?.lastMessageIndex ?? 0, completion: { result, index in
                        print("setLastReadMessageIndex\(result.description)")
                        self.conversationsHandler.lastReadIndex = nil
                    })
                }
                self.conversationsHandler.isSubscribe = true
                result("subscribed")
            } else {
                result(FlutterError(code: "INVALID_ARGS",
                                    message: "conversationId required",
                                    details: nil))
            }

            break
        case Methods.unSubscribeToMessageUpdate:
            guard let conversationId = arguments?["conversationId"] as? String, !conversationId.isEmpty else {
                result(FlutterError(code: "INVALID_ARGS",
                                    message: "conversationId required",
                                    details: nil))
                break
            }
            self.conversationsHandler.getConversationFromId(conversationId: conversationId) { conversation in
//                self.conversationsHandler.lastReadIndex = conversation?.lastMessageIndex
//                conversation?.setLastReadMessageIndex(conversation?.lastMessageIndex ?? 0, completion: { result, index in
//                    print("setLastReadMessageIndex \(result.description)")
//                    self.conversationsHandler.lastReadIndex = nil
//                })
            }
            self.conversationsHandler.conversationId = nil
            conversationsHandler.isSubscribe = nil
            conversationsHandler.messageDelegate = nil
            result("unsubscribed")

            break
        case Methods.isClientInitialized:
            let isInitialized = self.conversationsHandler.isClientInitialized()
            result(isInitialized)
            break
        case Methods.shutdownClient:
            self.conversationsHandler.shutdownClient { shutdownResult in
                result(shutdownResult)
            }
            break
        case Methods.deleteConversation:
            // Ported from ALAlliancetek fork. ALAT's original used
            // `as! String` which traps on missing/mistyped arguments;
            // PR #3 (I4) replaced that pattern with guard-let across
            // the file — apply the same here.
            guard let conversationId = arguments?["conversationId"] as? String, !conversationId.isEmpty else {
                result(FlutterError(code: "INVALID_ARGS",
                                    message: "deleteConversation requires conversationId:String",
                                    details: nil))
                break
            }
            self.conversationsHandler.deleteConversation(conversationId: conversationId) { resultString in
                result(resultString)
            }
            break
        case Methods.deleteMessageWithSid:
            guard let conversationId = arguments?["conversationId"] as? String,
                  let messageSid = arguments?["messageSid"] as? String, !messageSid.isEmpty else {
                result(FlutterError(code: "INVALID_ARGS",
                                    message: "deleteMessageWithSid requires conversationId:String, messageSid:String",
                                    details: nil))
                break
            }
            // Dart `int` bridges as NSNumber/Int — coerce via Int to
            // accept both, default to 100 when missing.
            let messageCount = (arguments?["messageCount"] as? Int) ?? 100
            self.conversationsHandler.deleteMessageWithSid(
                conversationId: conversationId,
                messageSid: messageSid,
                messageCount: messageCount
            ) { resultString in
                result(resultString)
            }
            break
        default:
            break
        }
    }
    

}

extension TwilioConversationSdkPlugin : MessageDelegate {
    func onSynchronizationChanged(status: [String : Any]) {
        // Conversation-sync status. The Dart side currently reads it off the
        // MESSAGE stream (the dedicated sync-channel listener in
        // twilio_conversation_sdk.dart is commented out), so we publish to both
        // for Android parity. NOTE: if that sync-channel listener is re-enabled,
        // drop the messageEventSink emit below or status is delivered twice.
        emitMain { [weak self] in
            self?.messageEventSink?(status)
            self?.syncEventSink?(status)
        }
    }

    func onMessageUpdate(message: [String : Any], messageSubscriptionId: String) {
        // ✅ Check if this is a typing event
        if let typingStatus = message["typingStatus"], let conversationSid = message["conversationSid"] as? String {
            if (messageSubscriptionId == conversationSid) {
                print("📤 Forwarding typing event to Flutter: \(message)")
                emitMain { [weak self] in self?.messageEventSink?(message) }
            } else {
                print("⚠️ Typing event conversationSid mismatch: subscribed=\(messageSubscriptionId), received=\(conversationSid)")
            }
            return
        }

        // ✅ Check if this is a message event
        if let conversationId = message["conversationId"] as? String, let messageData = message["message"] as? [String:Any] {
            if (messageSubscriptionId == conversationId) {
                print("📤 Forwarding message event to Flutter: \(messageData)")
                emitMain { [weak self] in self?.messageEventSink?(messageData) }
            } else {
                print("⚠️ Message event conversationId mismatch: subscribed=\(messageSubscriptionId), received=\(conversationId)")
            }
            return
        }

        print("⚠️ Unknown event type received: \(message)")
    }
}

extension TwilioConversationSdkPlugin : ClientDelegate {
    func onClientSynchronizationChanged(status: [String : Any]) {
        print("--------Status-------------> " + "\(status)")
        emitMain { [weak self] in self?.clientEventSink?(status) }
    }
}



