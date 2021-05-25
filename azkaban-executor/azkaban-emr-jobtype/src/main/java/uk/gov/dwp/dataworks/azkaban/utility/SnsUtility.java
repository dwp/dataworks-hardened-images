package uk.gov.dwp.dataworks.azkaban.utility;

import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.model.ListTopicsRequest;
import com.amazonaws.services.sns.model.ListTopicsResult;
import com.amazonaws.services.sns.model.Topic;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class SnsUtility {

    public static Optional<Topic> topicWithName(AmazonSNS sns, String topicName) {
        List<Topic> topics = new ArrayList<>();
        String nextToken = null;
        do {
            ListTopicsRequest request = new ListTopicsRequest().withNextToken(nextToken);
            ListTopicsResult result = sns.listTopics(request);
            topics.addAll(result.getTopics());
            nextToken = result.getNextToken();
        }
        while (nextToken != null);

        return topics.stream().filter(topic -> topic.getTopicArn().matches("^.+:" + topicName + "$")).findFirst();
    }

}
