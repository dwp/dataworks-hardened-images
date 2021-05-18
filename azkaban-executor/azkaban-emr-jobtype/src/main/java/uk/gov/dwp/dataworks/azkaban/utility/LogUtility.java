package uk.gov.dwp.dataworks.azkaban.utility;

import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.model.Instance;
import com.amazonaws.services.elasticmapreduce.model.ListInstancesRequest;
import com.amazonaws.services.elasticmapreduce.model.ListInstancesResult;
import com.amazonaws.services.logs.AWSLogs;
import com.amazonaws.services.logs.model.DescribeLogStreamsRequest;
import com.amazonaws.services.logs.model.DescribeLogStreamsResult;
import com.amazonaws.services.logs.model.LogStream;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static uk.gov.dwp.dataworks.azkaban.utility.EmrUtility.clusterInstances;

public class LogUtility {

    public static List<String> clusterStepLogStreams(AmazonElasticMapReduce emr, AWSLogs logs, String clusterId, String logGroup) {
        final Set<String> clusterInstances = clusterInstances(emr, clusterId);
        DescribeLogStreamsRequest req = new DescribeLogStreamsRequest().withLogGroupName(logGroup);
        String nextToken;
        final List<String> logStreams = new ArrayList<>();
        do {
            final DescribeLogStreamsResult res = logs.describeLogStreams(req);
            logStreams.addAll(res.getLogStreams().stream().map(LogStream::getLogStreamName)
                    .filter(logStream -> clusterInstances.stream().anyMatch(logStream::contains))
                    .collect(Collectors.toList()));
            nextToken = res.getNextToken();
            req = new DescribeLogStreamsRequest().withLogGroupName(logGroup).withNextToken(nextToken);
        } while (nextToken != null);

        return logStreams;
    }
}
