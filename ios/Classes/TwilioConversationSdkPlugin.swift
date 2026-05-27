import Flutter
import UIKit
import Foundation
import TwilioConversationsClient

public class TwilioConversationSdkPlugin: NSObject, FlutterPlugin,FlutterStreamHandler  {
    var conversationsHandler = ConversationsHandler()
    var eventSink: FlutterEventSink?
    var localConversation: TCHConversation?
    var tokenEventSink: FlutterEventSink?
    private var conversationsHandlers: ConversationsHandler?
    
    public func onListen(withArguments arguments: Any?, eventSink events: @escaping FlutterEventSink) -> FlutterError? {
        self.eventSink = events
        self.conversationsHandler.tokenEventSink = events
        return nil
    }
    
    public func onCancel(withArguments arguments: Any?) -> FlutterError? {
        self.eventSink = nil
        self.tokenEventSink = nil
        return nil
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
        messageEventChannel.setStreamHandler(instance)
        synchronizationStatusEventChannel.setStreamHandler(instance)
        tokenEventChannel.setStreamHandler(instance)
        onClientSynchronizationChangedEventChannel.setStreamHandler(instance)
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
                if success {
                    result("Token Registerd")
                } else {
                    result(FlutterError(code: "FCM_REGISTER_FAILED",
                                        message: "Failed to register FCM token",
                                        details: nil))
                }
            }
            break
        case Methods.unregisterFCMToken:
            guard let fcmToken = arguments?["fcmToken"] as? String, !fcmToken.isEmpty else {
                result(FlutterError(code: "INVALID_ARGS", message: "unregisterFCMToken requires fcmToken:String", details: nil))
                break
            }
            conversationsHandler.unregisterFCMToken(token: fcmToken) { success in
                if success {
                    result("Token unregisterFCMToken")
                } else {
                    result(FlutterError(code: "FCM_UNREGISTER_FAILED",
                                        message: "Failed to unregister FCM token",
                                        details: nil))
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
                guard let loginResultSuccessful: Bool = loginResult?.isSuccessful else {
                    result(Strings.authenticationFailed)
                    return
                }
                if(loginResultSuccessful) {
                    result(Strings.authenticationSuccessful)
                }else {
                    result(Strings.authenticationFailed)
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
                    dictionary["dateCreated"] = conversation.dateCreated
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
                        participant["dateCreated"] = user.dateCreated
                        participant["conversationCreatedBy"] = user.conversation?.createdBy
                        participant["isAdmin"] = (user.conversation?.createdBy == user.identity)
                        do {
                            let jsonData = try JSONSerialization.data(withJSONObject: user.attributes()!.dictionary ?? Dictionary(), options: .prettyPrinted)
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
                        participant["dateCreated"] = user.dateCreated
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
            self.conversationsHandler.addParticipants(conversationId: conversationId, participantName: participantName) { status in
                guard let addParticipantStatus = status else {
                    result("Conversation not found")
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
            self.conversationsHandler.removeParticipants(conversationId: conversationId, participantName: participantName) { status in
                guard let removeParticipantStatus = status else {
                    result("Conversation not found")
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
        self.eventSink?(status)
    }

    func onMessageUpdate(message: [String : Any], messageSubscriptionId: String) {
        // ✅ Check if this is a typing event
        if let typingStatus = message["typingStatus"], let conversationSid = message["conversationSid"] as? String {
            if (messageSubscriptionId == conversationSid) {
                print("📤 Forwarding typing event to Flutter: \(message)")
                self.eventSink?(message)
            } else {
                print("⚠️ Typing event conversationSid mismatch: subscribed=\(messageSubscriptionId), received=\(conversationSid)")
            }
            return
        }

        // ✅ Check if this is a message event
        if let conversationId = message["conversationId"] as? String, let messageData = message["message"] as? [String:Any] {
            if (messageSubscriptionId == conversationId) {
                print("📤 Forwarding message event to Flutter: \(messageData)")
                self.eventSink?(messageData)
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
        self.eventSink?(status)
    }
}



