package uk.gov.dwp.dataworks.azkaban.services;

import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.model.DescribeStepResult;
import com.amazonaws.services.elasticmapreduce.model.Instance;
import com.amazonaws.services.elasticmapreduce.model.ListInstancesResult;
import com.amazonaws.services.logs.AWSLogs;
import com.amazonaws.services.logs.model.DescribeLogStreamsResult;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class LogServiceTest {

    //    public static Set<String> clusterInstances(AmazonElasticMapReduce emr, String clusterId) {
    //        final ListInstancesRequest instancesRequest = new ListInstancesRequest().withClusterId(clusterId);
    //        final ListInstancesResult instancesResult = emr.listInstances(instancesRequest);
    //        return instancesResult.getInstances().stream().map(Instance::getEc2InstanceId).collect(Collectors.toSet());
    //    }

    @Test
    public void works() {
        AmazonElasticMapReduce emr = mock(AmazonElasticMapReduce.class);
        AWSLogs logs = mock(AWSLogs.class);
        ListInstancesResult result = mock(ListInstancesResult.class);
        List<Instance> instances = new ArrayList<>();
        Instance instance1 = mock(Instance.class);
        Instance instance2 = mock(Instance.class);
        instances.add(instance1);
        instances.add(instance2);
        when(result.getInstances()).thenReturn(instances);
        when(emr.listInstances(any())).thenReturn(result);

        DescribeLogStreamsResult logStreamsResult = mock(DescribeLogStreamsResult.class);
        when(logs.describeLogStreams(any())).thenReturn(logStreamsResult);

        DescribeStepResult describeStepRequest = mock(DescribeStepResult.class);
        when(emr.describeStep(any())).thenReturn(describeStepRequest);
        LogService logService = new LogService(emr, logs, LOG_GROUP);
        logService.monitorStepLogs(CLUSTER_ID, STEP_ID);
    }

    private final static String LOG_GROUP = "LOG_GROUP";
    private final static String CLUSTER_ID = "CLUSTER_ID";
    private final static String STEP_ID = "STEP_ID";
}
