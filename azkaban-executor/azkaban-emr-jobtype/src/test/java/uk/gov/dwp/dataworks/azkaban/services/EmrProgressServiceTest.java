package uk.gov.dwp.dataworks.azkaban.services;

import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.model.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.when;
import static org.mockito.Mockito.*;

class EmrProgressServiceTest {

    @Test
    public void shouldMonitorStepsWhenClusterReachesRunning() {
        AmazonElasticMapReduce emr = mock(AmazonElasticMapReduce.class);
        LogService logService = mock(LogService.class);
        DescribeClusterResult result = mock(DescribeClusterResult.class);
        Cluster cluster = mock(Cluster.class);
        ClusterStatus clusterStatus = mock(ClusterStatus.class);
        ListStepsResult stepsResult = mock(ListStepsResult.class);
        StepStatus status = mock(StepStatus.class);
        StepSummary stepSummary = mock(StepSummary.class);
        List<StepSummary> summaries = new ArrayList<>();
        summaries.add(stepSummary);
        when(clusterStatus.getState()).thenReturn(CLUSTER_RUNNING_STATE);
        when(cluster.getStatus()).thenReturn(clusterStatus);
        when(result.getCluster()).thenReturn(cluster);
        when(status.getState()).thenReturn(STEP_RUNNING_STATE, STEP_COMPLETED_STATE);
        when(stepSummary.getStatus()).thenReturn(status);
        when(stepSummary.getId()).thenReturn(STEP_ID);
        when(stepSummary.getName()).thenReturn(STEP_NAME);
        when(stepsResult.getSteps()).thenReturn(summaries);
        when(emr.describeCluster(any())).thenReturn(result);
        when(emr.listSteps(any())).thenReturn(stepsResult);
        EmrProgressService service = new EmrProgressService(emr, logService);
        boolean succeeded = service.observeEmr(CLUSTER_ID);
        verify(logService, times(1)).monitorStepLogs(CLUSTER_ID, STEP_ID);
        assertTrue(succeeded);
    }

    @ParameterizedTest
    @ValueSource(strings = {"TERMINATED", "TERMINATED_WITH_ERRORS", "TERMINATING", "WAITING"})
    public void shouldNotMonitorStepsIfClusterNotRunning(String clusterFinalStatus) {
        AmazonElasticMapReduce emr = mock(AmazonElasticMapReduce.class);
        LogService logService = mock(LogService.class);
        DescribeClusterResult result = mock(DescribeClusterResult.class);
        Cluster cluster = mock(Cluster.class);
        ClusterStatus clusterStatus = mock(ClusterStatus.class);
        when(clusterStatus.getState()).thenReturn(clusterFinalStatus);
        when(cluster.getStatus()).thenReturn(clusterStatus);
        when(result.getCluster()).thenReturn(cluster);
        when(emr.describeCluster(any())).thenReturn(result);
        EmrProgressService service = new EmrProgressService(emr, logService);
        boolean succeeded = service.observeEmr(CLUSTER_ID);
        verifyNoInteractions(logService);
        assertFalse(succeeded);
    }

    @ParameterizedTest
    @ValueSource(strings = {"CANCELLED", "FAILED", "INTERRUPTED"})
    public void shouldNotMonitorStepsWhenStepsFail(String stepFinalStatus) {
        AmazonElasticMapReduce emr = mock(AmazonElasticMapReduce.class);
        LogService logService = mock(LogService.class);
        DescribeClusterResult result = mock(DescribeClusterResult.class);
        Cluster cluster = mock(Cluster.class);
        ClusterStatus clusterStatus = mock(ClusterStatus.class);
        ListStepsResult stepsResult = mock(ListStepsResult.class);
        StepStatus status = mock(StepStatus.class);
        StepSummary stepSummary = mock(StepSummary.class);
        List<StepSummary> summaries = new ArrayList<>();
        summaries.add(stepSummary);
        when(clusterStatus.getState()).thenReturn(CLUSTER_RUNNING_STATE);
        when(cluster.getStatus()).thenReturn(clusterStatus);
        when(result.getCluster()).thenReturn(cluster);
        when(status.getState()).thenReturn(stepFinalStatus);
        when(stepSummary.getStatus()).thenReturn(status);
        when(stepSummary.getId()).thenReturn(STEP_ID);
        when(stepSummary.getName()).thenReturn(STEP_NAME);
        when(stepsResult.getSteps()).thenReturn(summaries);
        when(emr.describeCluster(any())).thenReturn(result);
        when(emr.listSteps(any())).thenReturn(stepsResult);
        EmrProgressService service = new EmrProgressService(emr, logService);
        boolean succeeded = service.observeEmr(CLUSTER_ID);
        verifyNoInteractions(logService);
        assertFalse(succeeded);
    }

    @Test
    public void shouldNotMonitorClusterIfCancelled() {
        AmazonElasticMapReduce emr = mock(AmazonElasticMapReduce.class);
        LogService logService = mock(LogService.class);
        EmrProgressService service = new EmrProgressService(emr, logService);
        service.cancel();
        boolean succeeded = service.observeEmr(CLUSTER_ID);
        verifyNoInteractions(emr);
        verify(logService, times(1)).cancel();
        verifyNoMoreInteractions(logService);
        assertFalse(succeeded);
    }

    private final static String CLUSTER_ID = "CLUSTER_ID";
    private final static String CLUSTER_RUNNING_STATE = "RUNNING";
    private final static String STEP_COMPLETED_STATE = "COMPLETED";
    private final static String STEP_ID = "STEP_ID";
    private final static String STEP_NAME = "STEP_NAME";
    private final static String STEP_RUNNING_STATE = "RUNNING";
}
