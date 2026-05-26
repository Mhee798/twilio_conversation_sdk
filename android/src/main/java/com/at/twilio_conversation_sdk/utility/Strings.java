package com.at.twilio_conversation_sdk.utility;

public class Strings {
    /// Used when conversation is created successfully
    public final static String createConversationSuccess = "Conversation created successfully.";
    /// Used when there is an error while creating a conversation
    public final static String createConversationFailure = "Error while creating conversation.";
    /// Used when conversation with provided name already exists
    public final static  String conversationExists = "Conversation with provided unique name already exists";
    /// Used when participant is added to a conversation successfully
    public final static String addParticipantSuccess = "Participant added successfully.";
    public final static String removedParticipantSuccess = "Participant removed successfully.";

    /// Used when there is an error while adding a participant to a conversation
    public final static String authenticationSuccessful = "Authentication Successful";
    /// Used when authentication is unsuccessful
    public final static  String authenticationFailed = "Authentication Failed";
    /// Used when twilio access token is about to expire
    public final static  String accessTokenWillExpire = "Access token will expire";
    /// Used when twilio access token is expired
    public final static  String accessTokenExpired = "Access token expired";
    /// Used when twilio access token is refreshed
    public final static  String accessTokenRefreshed = "Access token refreshed";

    public final static String fcmSuccess = "FCM Registered Successful";
    public final static String fcmUnSuccess = "FCM UnRegistered Successful";
    public final static String fcmFail = "FCM Registered Fail";
    public final static String fcmUnFail = "FCM UnRegistered Fail";
    public final static String failed = "Failed";
    public final static String success = "Success";

}