package uk.gov.dwp.dataworks.azkaban.services;

import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.model.Topic;
import uk.gov.dwp.dataworks.azkaban.utility.SnsUtility;

import java.util.Optional;

public class NotificationService {

    public NotificationService(AmazonSNS sns, String topicName, String clusterName) {
        this.sns = sns;
        this.topic = SnsUtility.topicWithName(sns, topicName);
        this.clusterName = clusterName;
    }

    public void notifyStarted() {
        sendNotification(payload("Medium", "Information", "started"));
    }

    public void notifySucceeded() {
        sendNotification(payload("Medium", "Information", "succeeded"));
    }

    public void notifyFailed() {
        sendNotification(payload("High", "Error", "failed"));
    }

    private void sendNotification(String payload) {
        topic.map(Topic::getTopicArn).ifPresent(arn -> sns.publish(arn, payload));
    }

    private String payload(String severity, String notificationType, String status) {
        return "{" +
            "\"severity\": \"" + severity + "\"," +
                ("true".equals(System.getenv("AWS_USE_DEVELOPMENT_REMOTELY")) ?
                        "\"slack_channel_override\": \"dataworks-alerts\"," : "") +
            "\"notification_type\": \"" + notificationType + "\"," +
            "\"slack_username\": \"EMR Launch Notification\"," +
            "\"title_text\": \"" + clusterName + ": " + status + ".\"" +
        "}";
    }

    private final AmazonSNS sns;
    private final String clusterName;
    private final Optional<Topic> topic;
}
