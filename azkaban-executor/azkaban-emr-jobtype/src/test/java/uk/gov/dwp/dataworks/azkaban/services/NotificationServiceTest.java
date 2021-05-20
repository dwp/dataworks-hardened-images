package uk.gov.dwp.dataworks.azkaban.services;

import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.model.ListTopicsRequest;
import com.amazonaws.services.sns.model.ListTopicsResult;
import com.amazonaws.services.sns.model.Topic;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class NotificationServiceTest {

    @Test
    public void shouldFindTopicAndNotifyStarted() {
        AmazonSNS sns = sns(true);
        NotificationService service = new NotificationService(sns, MATCHING_TOPIC_NAME, CLUSTER_NAME);
        service.notifyStarted();
        verifySnsInteractions(sns, "started");
    }

    @Test
    public void shouldFindTopicAndNotifySuccess() {
        AmazonSNS sns = sns(true);
        NotificationService service = new NotificationService(sns, MATCHING_TOPIC_NAME, CLUSTER_NAME);
        service.notifySucceeded();
        verifySnsInteractions(sns, "succeeded");
    }

    @Test
    public void shouldFindTopicAndNotifyFailure() {
        AmazonSNS sns = sns(true);
        NotificationService service = new NotificationService(sns, MATCHING_TOPIC_NAME, CLUSTER_NAME);
        service.notifyFailed();
        verifySnsInteractions(sns, "failed");
    }

    @Test
    public void shouldNotNotifyIfNoTopicFound() {
        AmazonSNS sns = sns(false);
        NotificationService service = new NotificationService(sns, MATCHING_TOPIC_NAME, CLUSTER_NAME);
        service.notifyStarted();
        service.notifySucceeded();
        service.notifyFailed();
        verify(sns, never()).publish(any(), any());
    }

    private AmazonSNS sns(boolean addMatchingTopic) {
        AmazonSNS sns = mock(AmazonSNS.class);

        List<Topic> page1Results = IntStream.range(0, 99).mapToObj(i -> {
            Topic topic = mock(Topic.class);
            when(topic.getTopicArn()).thenReturn("arn:aws:sns:" + NON_MATCHING_TOPIC_NAME + "_1_" + i);
            return topic;
        }).collect(Collectors.toList());

        ListTopicsResult result1 = mock(ListTopicsResult.class);
        when(result1.getNextToken()).thenReturn(NEXT_TOKEN);
        when(result1.getTopics()).thenReturn(page1Results);

        List<Topic> page2Results = IntStream.range(0, 99).mapToObj(i -> {
            Topic topic = mock(Topic.class);
            when(topic.getTopicArn()).thenReturn("arn:aws:sns:" + NON_MATCHING_TOPIC_NAME + "_2_" + i);
            return topic;
        }).collect(Collectors.toList());

        if (addMatchingTopic) {
            Topic matchingTopic = mock(Topic.class);
            when(matchingTopic.getTopicArn()).thenReturn("arn:aws:sns:" + MATCHING_TOPIC_NAME);
            page2Results.set(50, matchingTopic);
        }


        ListTopicsResult result2 = mock(ListTopicsResult.class);
        when(result2.getTopics()).thenReturn(page2Results);
        when(sns.listTopics(any(ListTopicsRequest.class))).thenReturn(result1, result2);
        return sns;
    }

    private void verifySnsInteractions(AmazonSNS sns, String keyword) {
        ArgumentCaptor<String> arnCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> payloadCaptor = ArgumentCaptor.forClass(String.class);
        verify(sns, times(1)).publish(arnCaptor.capture(), payloadCaptor.capture());
        assertEquals("arn:aws:sns:" + MATCHING_TOPIC_NAME, arnCaptor.getValue());
        assertTrue(payloadCaptor.getValue().matches(".+" + CLUSTER_NAME + ": " + keyword + ".+"));
    }

    private final static String MATCHING_TOPIC_NAME = "MATCHING_TOPIC_NAME";
    private final static String NON_MATCHING_TOPIC_NAME = "NON_MATCHING_TOPIC_NAME";
    private final static String NEXT_TOKEN = "NEXT_TOKEN";
    private final static String CLUSTER_NAME = "CLUSTER_NAME";
}
