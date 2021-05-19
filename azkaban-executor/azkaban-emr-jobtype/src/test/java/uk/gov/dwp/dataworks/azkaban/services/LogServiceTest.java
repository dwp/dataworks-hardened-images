package uk.gov.dwp.dataworks.azkaban.services;

import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.model.DescribeStepResult;
import com.amazonaws.services.elasticmapreduce.model.Instance;
import com.amazonaws.services.elasticmapreduce.model.ListInstancesResult;
import com.amazonaws.services.elasticmapreduce.model.Step;
import com.amazonaws.services.elasticmapreduce.model.StepStatus;
import com.amazonaws.services.logs.AWSLogs;
import com.amazonaws.services.logs.model.DescribeLogStreamsResult;
import com.amazonaws.services.logs.model.GetLogEventsResult;
import com.amazonaws.services.logs.model.LogStream;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class LogServiceTest {

    @Test
    public void shouldFetchLogsUntilStreamIsDrained() {
        AmazonElasticMapReduce emr = mock(AmazonElasticMapReduce.class);
        AWSLogs logs = mock(AWSLogs.class);
        ListInstancesResult result = mock(ListInstancesResult.class);
        List<Instance> instances = new ArrayList<>();
        Instance instance1 = mock(Instance.class);
        when(instance1.getEc2InstanceId()).thenReturn(INSTANCE_ID_1);
        Instance instance2 = mock(Instance.class);
        when(instance2.getEc2InstanceId()).thenReturn(INSTANCE_ID_2);
        instances.add(instance1);
        instances.add(instance2);
        when(result.getInstances()).thenReturn(instances);
        when(emr.listInstances(any())).thenReturn(result);
        List<LogStream> logStreams = new ArrayList<>();
        LogStream logStream = mock(LogStream.class);
        when(logStream.getLogStreamName()).thenReturn(LOG_STREAM_NAME);
        logStreams.add(logStream);
        DescribeLogStreamsResult logStreamsResult = mock(DescribeLogStreamsResult.class);
        when(logs.describeLogStreams(any())).thenReturn(logStreamsResult);
        when(logStreamsResult.getLogStreams()).thenReturn(logStreams);
        GetLogEventsResult getLogEventsResult = mock(GetLogEventsResult.class);
        when(getLogEventsResult.getNextForwardToken()).thenReturn(LOG_TOKEN_1, LOG_TOKEN_2, LOG_TOKEN_2);
        when(logs.getLogEvents(any())).thenReturn(getLogEventsResult);
        Step step = mock(Step.class);
        StepStatus stepStatus = mock(StepStatus.class);
        when(stepStatus.getState()).thenReturn(STEP_STATUS_RUNNING, STEP_STATUS_RUNNING, STEP_STATUS_RUNNING, STEP_STATUS_COMPLETED);
        when(step.getStatus()).thenReturn(stepStatus);
        when(step.getId()).thenReturn(STEP_ID);
        when(step.getName()).thenReturn(STEP_NAME);
        DescribeStepResult describeStepResult = mock(DescribeStepResult.class);
        when(describeStepResult.getStep()).thenReturn(step);
        when(emr.describeStep(any())).thenReturn(describeStepResult);
        LogService logService = new LogService(emr, logs, LOG_GROUP);
        logService.monitorStepLogs(CLUSTER_ID, STEP_ID);
        verify(logs, times(2)).getLogEvents(any());
    }

    private final static String INSTANCE_ID_1 = "INSTANCE_ID_1";
    private final static String INSTANCE_ID_2 = "INSTANCE_ID_2";
    private final static String LOG_GROUP = "LOG_GROUP";
    private final static String CLUSTER_ID = "CLUSTER_ID";
    private final static String STEP_ID = "STEP_ID";
    private final static String STEP_NAME = "STEP_NAME";
    private final static String STEP_STATUS_COMPLETED = "COMPLETED";
    private final static String STEP_STATUS_RUNNING = "RUNNING";
    private final static String LOG_STREAM_NAME = "LOG_STREAM_" + STEP_NAME + "_" + INSTANCE_ID_1;
    private final static String LOG_TOKEN_1 = "LOG_TOKEN_1";
    private final static String LOG_TOKEN_2 = "LOG_TOKEN_2";
}
