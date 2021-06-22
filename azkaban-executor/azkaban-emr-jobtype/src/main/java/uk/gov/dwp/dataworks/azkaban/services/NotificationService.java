package uk.gov.dwp.dataworks.azkaban.services;

import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.model.Topic;
import uk.gov.dwp.dataworks.azkaban.utility.SnsUtility;

/**
 * Used by the {@link EmrLaunchAndMonitoringService} to post SNS messages
 * that indicate the status of the process (started, failed, succeeded etc.)
 */
public class NotificationService {

    private final AmazonSNS sns;
    private final String clusterName;
    private final Topic topic;
    private final boolean skipNotifications;

    public NotificationService(AmazonSNS sns, String topicName, String clusterName, boolean skipNotifications) {
        this.sns = sns;
        this.topic = SnsUtility.topicWithName(sns, topicName).orElseThrow(
                () -> new RuntimeException("Could not find configured SNS topic: '" + topicName + "'"));
        this.clusterName = clusterName;
        this.skipNotifications = skipNotifications;
    }

    public void notifyStarted() {
        if (!skipNotifications) {
            sendNotification(payload("Medium", "Information", "started"));
        }
    }

    public void notifySucceeded() {
        if (!skipNotifications) {
            sendNotification(payload("Medium", "Information", "succeeded"));
        }
    }

    public void notifyFailed() {
        sendNotification(payload("High", "Error", "failed"));
    }

    private void sendNotification(String payload) {
        sns.publish(topic.getTopicArn(), payload);
    }

    private String payload(String severity, String notificationType, String status) {
        return "{" + "\"severity\": \"" + severity + "\"," + ("true"
                .equals(System.getenv("AWS_USE_DEVELOPMENT_REMOTELY")) ?
                "\"slack_channel_override\": \"dataworks-alerts\"," :
                "") + "\"notification_type\": \"" + notificationType + "\","
                + "\"slack_username\": \"EMR Launch Notification\"," + "\"title_text\": \"" + clusterName + ": "
                + status + ".\"" + "}";
    }
}
