import 'dart:async';

import 'package:flutter/services.dart';

import 'twilio_conversation_sdk_platform_interface.dart';

class TwilioConversationSdk {
  Future<String?> getPlatformVersion() {
    return TwilioConversationSdkPlatform.instance.getPlatformVersion();
  }

  // Event channels for message updates and token status changes.
  static const EventChannel _messageEventChannel =
      EventChannel('twilio_conversation_sdk/onMessageUpdated');
  static const EventChannel _tokenEventChannel =
      EventChannel('twilio_conversation_sdk/onTokenStatusChange');
  static const EventChannel _clientEventChannel =
      EventChannel('twilio_conversation_sdk/onClientSynchronizationChanged');

  // Stream controllers for message updates and token status changes.
  static final StreamController<Map> _messageUpdateController =
      StreamController<Map>.broadcast();
  static final StreamController<Map> _tokenStatusController =
      StreamController<Map>.broadcast();
  static final StreamController<Map> _clientStatusController =
      StreamController<Map>.broadcast();

  /// Stream for receiving incoming messages.
  Stream<Map> get onMessageReceived => _messageUpdateController.stream;

  /// Stream for client synchronous status.
  Stream<Map> get onClientSyncStatusChanged => _clientStatusController.stream;

  /// Generates a Twilio Chat token.
  Future<String?> generateToken(
      {required String accountSid,
      required String apiKey,
      required String apiSecret,
      required String identity,
      required String serviceSid,
      required String pushSid}) {
    return TwilioConversationSdkPlatform.instance.generateToken(
        accountSid: accountSid,
        apiKey: apiKey,
        apiSecret: apiSecret,
        identity: identity,
        serviceSid: serviceSid,
        pushSid: pushSid);
  }

  /// Initializes the Twilio Conversation Client with an access token.
  ///
  /// This method initializes the Twilio Conversation Client using the provided
  /// access token. Once initialized, the client can be used to interact with
  /// conversations and send/receive messages.
  ///
  /// - [accessToken]: The access token used for authentication.
  ///
  /// Returns a [String] indicating the result of the initialization, or `null` if it fails.
  Future<String?> initializeConversationClient(
      {required String accessToken}) async {
    _clientEventChannel
        .receiveBroadcastStream(accessToken)
        .listen((dynamic clientStatus) {
      print("Status Listen $clientStatus");
      _clientStatusController.add(clientStatus);
    });
    var result = await TwilioConversationSdkPlatform.instance
        .initializeConversationClient(accessToken: accessToken);

    return result;
  }

  /// Register FCM Token
  ///
  /// This method register fcm token against identity.
  ///
  /// - [fcmToken]: FCM Token received from firebase.
  ///
  /// Returns a [String] indicating the result of the operation, or `null` if it fails.
  Future<String?> registerFCMToken({required String fcmToken}) {
    return TwilioConversationSdkPlatform.instance
        .registerFCMToken(fcmToken: fcmToken);
  }

  /// UnRegister FCM Token
  ///
  /// This method un register fcm token against identity.
  ///
  /// - [fcmToken]: FCM Token received from firebase.
  ///
  /// Returns a [String] indicating the result of the operation, or `null` if it fails.
  Future<String?> unregisterFCMToken({required String fcmToken}) {
    return TwilioConversationSdkPlatform.instance
        .unregisterFCMToken(fcmToken: fcmToken);
  }

  /// Creates a new conversation.
  ///
  /// This method creates a new conversation with the specified name and identity.
  ///
  /// - [conversationName]: The name of the new conversation.
  /// - [identity]: The identity of the user initiating the conversation.
  ///
  /// Returns a [String] indicating the result of the operation, or `null` if it fails.
  Future<String?> createConversation(
      {required String conversationName, required String identity}) {
    return TwilioConversationSdkPlatform.instance.createConversation(
        conversationName: conversationName, identity: identity);
  }

  /// Retrieves a list of conversations.
  ///
  /// This method retrieves a list of conversations available to the user.
  ///
  /// Returns a list of conversations as [List], or `null` if the operation fails.
  Future<List?> getConversations() {
    return TwilioConversationSdkPlatform.instance.getConversations();
  }

  /// Retrieves a list of conversations logged user last messages.
  ///
  /// This method retrieves a list of conversations available to the user.
  ///
  /// Returns a list of conversations logged user last messages [List], or `null` if the operation fails.
  Future<List?> getLastMessages({required String conversationId}) {
    return TwilioConversationSdkPlatform.instance
        .getLastMessages(conversationId: conversationId);
  }

  /// Retrieves a list of conversations logged user last messages unread count.
  ///
  /// This method retrieves a list of conversations available to the user.
  ///
  /// Returns a list of conversations logged user last messages unread count [List], or `null` if the operation fails.
  Future<List?> getUnReadMsgCount({required String conversationId}) {
    return TwilioConversationSdkPlatform.instance
        .getUnReadMsgCount(conversationId: conversationId);
  }

  /// Retrieves messages from a conversation.
  ///
  /// This method retrieves messages from the specified conversation. The optional
  /// [messageCount] parameter allows you to limit the number of messages to retrieve.
  ///
  /// - [conversationId]: The ID of the conversation from which to retrieve messages.
  /// - [messageCount]: The maximum number of messages to retrieve (optional).
  ///
  /// Returns a list of messages as [List], or `null` if the operation fails.
  Future<List?> getMessages(
      {required String conversationId, int? messageCount}) {
    return TwilioConversationSdkPlatform.instance.getMessages(
        conversationId: conversationId, messageCount: messageCount);
  }

  /// Joins a conversation.
  ///
  /// This method allows a user to join an existing conversation by specifying its ID.
  ///
  /// - [conversationId]: The ID of the conversation to join.
  ///
  /// Returns a [String] indicating the result of the operation, or `null` if it fails.
  Future<String?> joinConversation({required String conversationId}) {
    return TwilioConversationSdkPlatform.instance
        .joinConversation(conversationId: conversationId);
  }

  /// Sends a message in a conversation.
  ///
  /// This method sends a message in the specified conversation.
  ///
  /// - [message]: The message content to send.
  /// - [conversationId]: The ID of the conversation in which to send the message.
  ///
  /// Returns a [String] indicating the result of the operation, or `null` if it fails.
  Future<String?> sendMessage(
      {required String message,
      required String conversationId,
      required dynamic attribute}) {
    return TwilioConversationSdkPlatform.instance.sendMessage(
        conversationId: conversationId, message: message, attribute: attribute);
  }

  /// Update a message in a conversation.
  ///
  /// This method sends a message in the specified conversation.
  ///
  /// - [message]: The message content to send.
  /// - [conversationId]: The ID of the conversation in which to send the message.
  /// - [msgId]: The ID of the message.
  ///
  /// Returns a [String] indicating the result of the operation, or `null` if it fails.
  Future<String?> updateMessage(
      {required String message,
      required String conversationId,
      required String msgId,
      required dynamic attribute}) {
    return TwilioConversationSdkPlatform.instance.updateMessage(
        conversationId: conversationId,
        msgId: msgId,
        message: message,
        attribute: attribute);
  }

  /// Update multiple messages in a conversation.
  ///
  /// This method updates multiple messages in the specified conversation.
  ///
  /// - [conversationId]: The ID of the conversation containing the messages.
  /// - [messages]: List of messages to update, each containing msgId, message, and attribute.
  ///
  /// Example:
  /// ```dart
  /// final result = await twilioConversationSdk.updateMessages(
  ///   conversationId: 'CH...',
  ///   messages: [
  ///     {
  ///       'msgId': 'IM123...',
  ///       'message': 'Updated text 1',
  ///       'attribute': {'edited': true}
  ///     },
  ///     {
  ///       'msgId': 'IM456...',
  ///       'message': 'Updated text 2',
  ///       'attribute': {'edited': true}
  ///     }
  ///   ]
  /// );
  /// ```
  ///
  /// Returns a [Map] containing:
  /// - success: List of successfully updated message IDs
  /// - errors: List of error messages for failed updates
  /// - totalSuccess: Total number of successful updates
  /// - totalErrors: Total number of failed updates
  Future<Map> updateMessages(
      {required String conversationId,
      required List<Map<String, dynamic>> messages}) {
    return TwilioConversationSdkPlatform.instance.updateMessages(
        conversationId: conversationId, messages: messages);
  }

  /// Sends a message with media in a conversation.
  ///
  /// This method sends a message in the specified conversation.
  ///
  /// - [message]: The message content to send.
  /// - [conversationId]: The ID of the conversation in which to send the message.
  ///
  /// Returns a [String] indicating the result of the operation, or `null` if it fails.
  Future<String?> sendMessageWithMedia(
      {required String message,
      required String conversationId,
      required dynamic attribute,
      required String mediaFilePath,
      required String mimeType,
      required String fileName}) {
    return TwilioConversationSdkPlatform.instance.sendMessageWithMedia(
        message: message,
        conversationId: conversationId,
        attribute: attribute,
        mediaFilePath: mediaFilePath,
        mimeType: mimeType,
        fileName: fileName);
  }

  /// Adds a participant in a conversation.
  ///
  /// - [participantName]: The name of the participant to be added.
  /// - [conversationId]: The ID of the conversation in which to add the participant.
  Future<String?> addParticipant(
      {required String participantName, required String conversationId}) {
    return TwilioConversationSdkPlatform.instance.addParticipant(
        conversationId: conversationId, participantName: participantName);
  }

  /// Removes a participant from a conversation.
  ///
  /// - [participantName]: The name of the participant to be removed.
  /// - [conversationId]: The ID of the conversation from which to remove the participant.
  Future<String?> removeParticipant(
      {required String participantName, required String conversationId}) {
    return TwilioConversationSdkPlatform.instance.removeParticipant(
        conversationId: conversationId, participantName: participantName);
  }

  /// Receives messages for a specific conversation.
  ///
  /// - [conversationId]: The ID of the conversation for which to receive messages.
  ///
  /// Returns a [String] indicating the result of the operation, or `null` if it fails.
  Future<String?> receiveMessages({required String conversationId}) {
    return TwilioConversationSdkPlatform.instance
        .receiveMessages(conversationId: conversationId);
  }

  /// Retrieves a list of participants for a conversation.
  ///
  /// - [conversationId]: The ID of the conversation for which to retrieve participants.
  ///
  /// Returns a list of participants as [List], or `null` if the operation fails.
  Future<List?> getParticipants({required String conversationId}) {
    return TwilioConversationSdkPlatform.instance
        .getParticipants(conversationId: conversationId);
  }

  /// Returns a list of participants with name as [List], or `null` if the operation fails.
  Future<List?> getParticipantsWithName({required String conversationId}) {
    return TwilioConversationSdkPlatform.instance
        .getParticipantsWithName(conversationId: conversationId);
  }

  /// Delete conversation.
  ///
  /// This method delete conversation with the specified name and identity.
  ///
  /// Returns a [String] indicating the result of the operation, or `null` if it fails.
  Future<String?> deleteConversation({required String conversationId}) {
    return TwilioConversationSdkPlatform.instance
        .deleteConversation(conversationId: conversationId);
  }

  /// Delete conversation.
  ///
  /// This method delete conversation with the specified name and identity.
  ///
  /// Returns a [String] indicating the result of the operation, or `null` if it fails.
  Future<String?> deleteMessage(
      {required String conversationId, required int index}) {
    return TwilioConversationSdkPlatform.instance
        .deleteMessage(conversationId: conversationId, index: index);
  }

  /// Delete a single message identified by its SID.
  ///
  /// Looks up the message inside the last [messageCount] messages of the
  /// conversation, then removes it. Useful when the caller has the
  /// message SID but not its numeric index (e.g. from a chat-message
  /// model bound to a server message id).
  ///
  /// Returns one of the status strings: `"success"`,
  /// `"msg_not_found: ..."`, `"delete_failed: ..."`,
  /// `"conv_failed: ..."`, `"getLastMessages error: ..."`, or
  /// `"Sync error: ..."` (iOS + Android — surfaces sync-wait timeout).
  Future<String?> deleteMessageWithSid({
    required String conversationId,
    required String messageSid,
    int messageCount = 100,
  }) {
    return TwilioConversationSdkPlatform.instance.deleteMessageWithSid(
      conversationId: conversationId,
      messageSid: messageSid,
      messageCount: messageCount,
    );
  }

  /// Subscribes to message update events for a specific conversation.
  void subscribeToMessageUpdate({required String conversationSid}) async {
    TwilioConversationSdkPlatform.instance
        .subscribeToMessageUpdate(conversationId: conversationSid);
    _messageEventChannel
        .receiveBroadcastStream(conversationSid)
        .listen((dynamic message) {
      if (message is! Map) return;
      // D5: the native codec delivers Map<Object?, Object?>; normalize to
      // Map<String, dynamic> once so consumers of onMessageReceived can cast
      // without a runtime TypeError.
      final Map<String, dynamic> msg = Map<String, dynamic>.from(message);
      // D4: forward each recognized event exactly once. These were 8 separate
      // `if`s with identical bodies, so an event carrying several of these keys
      // (e.g. a media event has mediaStatus + messageStatus + bytesSent +
      // conversationSid) was emitted 2-4 times. The map carries all its keys
      // either way, so emit the complete map a single time.
      final bool isRecognizedEvent =
          (msg["author"] != null && msg["body"] != null) ||
              msg["status"] != null ||
              msg["mediaStatus"] != null ||
              msg["messageStatus"] != null ||
              msg["bytesSent"] != null ||
              msg["identity"] != null ||
              msg["typingStatus"] != null ||
              msg["conversationSid"] != null;
      if (isRecognizedEvent) {
        _messageUpdateController.add(msg);
      }
    });
    /*_syncEventChannel
        .receiveBroadcastStream(conversationSid)
        .listen((dynamic syncStatus) {
      if (syncStatus != null) {
        if (syncStatus["status"] != null) {
          _syncUpdateController.add(syncStatus);
        }
      }
    });*/
  }

  Future<String?> setTypingStatus(
      {required String conversationId, required bool isTyping}) async {
    return TwilioConversationSdkPlatform.instance
        .setTypingStatus(conversationId: conversationId, isTyping: isTyping);
  }

  /// Unsubscribes from message update events for a specific conversation.
  void unSubscribeToMessageUpdate({required String conversationSid}) {
    TwilioConversationSdkPlatform.instance
        .unSubscribeToMessageUpdate(conversationId: conversationSid);
  }

  /// Updates the access token used for communication.
  Future<Map> updateAccessToken({required String accessToken}) {
    return TwilioConversationSdkPlatform.instance
        .updateAccessToken(accessToken: accessToken);
  }

  /// Stream for receiving token status changes.
  Stream<Map> get onTokenStatusChange {
    _tokenEventChannel.receiveBroadcastStream().listen((dynamic tokenStatus) {
      _tokenStatusController.add(tokenStatus);
    });
    return _tokenStatusController.stream;
  }

  /// Checks if the Twilio Conversation Client is initialized and ready to use.
  ///
  /// This method checks whether the client has been properly initialized and
  /// synchronized with the Twilio service. Use this to verify the client is
  /// ready before performing operations like sending messages or creating conversations.
  ///
  /// Returns `true` if the client is initialized and synchronized, `false` otherwise.
  ///
  /// Example:
  /// ```dart
  /// final twilioSdk = TwilioConversationSdk();
  /// bool isReady = await twilioSdk.isClientInitialized();
  /// if (isReady) {
  ///   // Client is ready, proceed with operations
  ///   await twilioSdk.sendMessage(
  ///     conversationId: 'CH123...',
  ///     message: 'Hello!',
  ///     attribute: {}
  ///   );
  /// } else {
  ///   // Client not ready, show error or initialize
  ///   print('Client not initialized');
  /// }
  /// ```
  Future<bool> isClientInitialized() {
    return TwilioConversationSdkPlatform.instance.isClientInitialized();
  }

  /// Shuts down and cleans up the Twilio Conversation Client.
  ///
  /// This method properly disposes of the Twilio client, cleaning up resources
  /// and closing connections. Call this method when you want to log out or
  /// when the app is being disposed.
  ///
  /// Returns a [String] indicating the result of the shutdown operation.
  ///
  /// Example:
  /// ```dart
  /// final twilioSdk = TwilioConversationSdk();
  /// // When logging out or disposing
  /// String result = await twilioSdk.shutdownClient();
  /// print(result); // "Client shutdown successfully"
  /// ```
  Future<String?> shutdownClient() {
    return TwilioConversationSdkPlatform.instance.shutdownClient();
  }
}
