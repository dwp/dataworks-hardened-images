package uk.gov.dwp.dataworks.azkaban.jobtype;

import azkaban.utils.Props;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.model.ClusterSummary;
import com.amazonaws.services.elasticmapreduce.model.ListClustersResult;
import com.amazonaws.services.lambda.AWSLambda;
import com.amazonaws.services.lambda.model.InvokeResult;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static uk.gov.dwp.dataworks.azkaban.jobtype.EMRStep.AWS_EMR_CLUSTER_NAME;
import static uk.gov.dwp.dataworks.azkaban.jobtype.EMRStep.AWS_EMR_COPY_SECCONFIG;
import static uk.gov.dwp.dataworks.azkaban.jobtype.EMRStep.EMR_INITIAL_WAIT;

class EMRStepTest {

    @Test
    void shouldNotStartClusterIfClusterAlreadyStarted() {
        AmazonElasticMapReduce emr = mock(AmazonElasticMapReduce.class);
        ListClustersResult clustersResult = mock(ListClustersResult.class);
        when(emr.listClusters(any())).thenReturn(clustersResult);

        ClusterSummary otherClusterSummary = otherClusterSummary(1);
        ClusterSummary targetClusterSummary = targetClusterSummary();

        List<ClusterSummary> summaries = new ArrayList<>();
        summaries.add(otherClusterSummary);
        summaries.add(targetClusterSummary);

        when(clustersResult.getClusters()).thenReturn(summaries);

        AWSLambda lambda = mock(AWSLambda.class);
        String clusterId = getClusterId(new AtomicBoolean(false), emr, lambda);
        assertEquals(TARGET_CLUSTER_ID, clusterId);
        verify(emr, times(1)).listClusters(any());
        verifyNoMoreInteractions(emr);
        verifyNoInteractions(lambda);
    }


    @Test
    void shouldNotStartClusterIfClusterStartsAfterInitialAdditionalCheck() {
        ListClustersResult clustersResult1 = mock(ListClustersResult.class);
        ListClustersResult clustersResult2 = mock(ListClustersResult.class);
        ClusterSummary otherClusterSummary1 = otherClusterSummary(1);
        ClusterSummary otherClusterSummary2 = otherClusterSummary(2);
        ClusterSummary targetClusterSummary = targetClusterSummary();

        List<ClusterSummary> summaries1 = new ArrayList<>();
        summaries1.add(otherClusterSummary1);
        summaries1.add(otherClusterSummary2);

        List<ClusterSummary> summaries2 = new ArrayList<>();
        summaries2.add(otherClusterSummary1);
        summaries2.add(otherClusterSummary2);
        summaries2.add(targetClusterSummary);

        when(clustersResult1.getClusters()).thenReturn(summaries1);
        when(clustersResult2.getClusters()).thenReturn(summaries2);
        AmazonElasticMapReduce emr = mock(AmazonElasticMapReduce.class);
        when(emr.listClusters(any())).thenReturn(clustersResult1, clustersResult2);

        AWSLambda lambda = mock(AWSLambda.class);

        String clusterId = getClusterId(new AtomicBoolean(false), emr, lambda);
        assertEquals(TARGET_CLUSTER_ID, clusterId);
        verify(emr, times(2)).listClusters(any());
        verifyNoMoreInteractions(emr);
        verifyNoInteractions(lambda);
    }

    @Test
    void shouldStartClusterIfClusterNotStartedAfterInitialAdditionalCheck() {
        ClusterSummary otherClusterSummary1 = otherClusterSummary(1);
        ClusterSummary otherClusterSummary2 = otherClusterSummary(2);
        ClusterSummary targetClusterSummary = targetClusterSummary();

        List<ClusterSummary> summaries1 = new ArrayList<>();
        summaries1.add(otherClusterSummary1);
        summaries1.add(otherClusterSummary2);

        List<ClusterSummary> summaries2 = new ArrayList<>();
        summaries2.add(otherClusterSummary1);
        summaries2.add(otherClusterSummary2);

        List<ClusterSummary> summaries3 = new ArrayList<>();
        summaries3.add(otherClusterSummary1);
        summaries3.add(otherClusterSummary2);
        summaries3.add(targetClusterSummary);

        ListClustersResult clustersResult1 = mock(ListClustersResult.class);
        ListClustersResult clustersResult2 = mock(ListClustersResult.class);
        ListClustersResult clustersResult3 = mock(ListClustersResult.class);

        when(clustersResult1.getClusters()).thenReturn(summaries1);
        when(clustersResult2.getClusters()).thenReturn(summaries2);
        when(clustersResult3.getClusters()).thenReturn(summaries3);

        AmazonElasticMapReduce emr = mock(AmazonElasticMapReduce.class);
        when(emr.listClusters(any())).thenReturn(clustersResult1, clustersResult2, clustersResult3);

        AWSLambda lambda = mock(AWSLambda.class);
        InvokeResult result = mock(InvokeResult.class);
        when(result.getStatusCode()).thenReturn(200);
        when(lambda.invoke(any())).thenReturn(result);

        String clusterId = getClusterId(new AtomicBoolean(false), emr, lambda);
        assertEquals(TARGET_CLUSTER_ID, clusterId);
        verify(emr, times(3)).listClusters(any());
        verifyNoMoreInteractions(emr);
        verify(lambda, times(1)).invoke(any());
        verifyNoMoreInteractions(lambda);
    }

    @Test
    void shouldThrowExceptionIfLambdaInvocationFails() {
        ClusterSummary otherClusterSummary1 = otherClusterSummary(1);
        ClusterSummary otherClusterSummary2 = otherClusterSummary(2);
        ClusterSummary targetClusterSummary = targetClusterSummary();

        List<ClusterSummary> summaries1 = new ArrayList<>();
        summaries1.add(otherClusterSummary1);
        summaries1.add(otherClusterSummary2);

        List<ClusterSummary> summaries2 = new ArrayList<>();
        summaries2.add(otherClusterSummary1);
        summaries2.add(otherClusterSummary2);

        List<ClusterSummary> summaries3 = new ArrayList<>();
        summaries3.add(otherClusterSummary1);
        summaries3.add(otherClusterSummary2);
        summaries3.add(targetClusterSummary);

        ListClustersResult clustersResult1 = mock(ListClustersResult.class);
        ListClustersResult clustersResult2 = mock(ListClustersResult.class);
        ListClustersResult clustersResult3 = mock(ListClustersResult.class);

        when(clustersResult1.getClusters()).thenReturn(summaries1);
        when(clustersResult2.getClusters()).thenReturn(summaries2);
        when(clustersResult3.getClusters()).thenReturn(summaries3);

        AmazonElasticMapReduce emr = mock(AmazonElasticMapReduce.class);
        when(emr.listClusters(any())).thenReturn(clustersResult1, clustersResult2, clustersResult3);

        AWSLambda lambda = mock(AWSLambda.class);
        InvokeResult result = mock(InvokeResult.class);
        when(result.getStatusCode()).thenReturn(501);
        when(lambda.invoke(any())).thenReturn(result);

        assertThrows(IllegalStateException.class, () ->
                EMRStep.getClusterId(emr, lambda, SYSTEM_PROPERTIES, JOB_PROPERTIES,
                                    new AtomicBoolean(false),
                                    Logger.getLogger(EMRStep.class)));

        verify(emr, times(2)).listClusters(any());
        verifyNoMoreInteractions(emr);
        verify(lambda, times(1)).invoke(any());
        verifyNoMoreInteractions(lambda);
    }

    @Test
    void shouldThrowExceptionIfClusterDoesntAppearAfterLambdaInvocation() {
        ClusterSummary otherClusterSummary1 = otherClusterSummary(1);
        ClusterSummary otherClusterSummary2 = otherClusterSummary(2);
        List<ClusterSummary> summaries1 = new ArrayList<>();
        summaries1.add(otherClusterSummary1);
        summaries1.add(otherClusterSummary2);
        ListClustersResult clustersResult1 = mock(ListClustersResult.class);

        when(clustersResult1.getClusters()).thenReturn(summaries1);

        AmazonElasticMapReduce emr = mock(AmazonElasticMapReduce.class);
        when(emr.listClusters(any())).thenReturn(clustersResult1);

        AWSLambda lambda = mock(AWSLambda.class);
        InvokeResult result = mock(InvokeResult.class);
        when(result.getStatusCode()).thenReturn(200);
        when(lambda.invoke(any())).thenReturn(result);

        assertThrows(IllegalStateException.class, () ->
                EMRStep.getClusterId(emr, lambda, SYSTEM_PROPERTIES, JOB_PROPERTIES,
                        new AtomicBoolean(false),
                        Logger.getLogger(EMRStep.class)));

        verify(emr, times(6)).listClusters(any());
        verifyNoMoreInteractions(emr);
        verify(lambda, times(1)).invoke(any());
        verifyNoMoreInteractions(lambda);
    }

    @Test
    void shouldNotStartClusterIfJobKilledBeforeInitialAdditionalCheck() {
        ClusterSummary otherClusterSummary1 = otherClusterSummary(1);
        ClusterSummary otherClusterSummary2 = otherClusterSummary(2);

        List<ClusterSummary> summaries1 = new ArrayList<>();
        summaries1.add(otherClusterSummary1);
        summaries1.add(otherClusterSummary2);
        ListClustersResult clustersResult1 = mock(ListClustersResult.class);

        when(clustersResult1.getClusters()).thenReturn(summaries1);

        AtomicBoolean killed = new AtomicBoolean(false);
        AmazonElasticMapReduce emr = mock(AmazonElasticMapReduce.class);
        when(emr.listClusters(any())).thenAnswer(x -> {
            killed.set(true);
            return clustersResult1;
        });

        AWSLambda lambda = mock(AWSLambda.class);
        InvokeResult result = mock(InvokeResult.class);
        when(result.getStatusCode()).thenReturn(200);
        when(lambda.invoke(any())).thenReturn(result);

        String clusterId = getClusterId(killed, emr, lambda);
        assertEquals("", clusterId);
        verify(emr, times(1)).listClusters(any());
        verifyNoMoreInteractions(emr);
        verifyNoInteractions(lambda);
    }

    private String getClusterId(AtomicBoolean killed, AmazonElasticMapReduce emr, AWSLambda lambda) {
        return EMRStep.getClusterId(emr, lambda, SYSTEM_PROPERTIES, JOB_PROPERTIES, killed,
                Logger.getLogger(EMRStep.class));
    }

    @Test
    void shouldNotStartClusterIfJobKilledAfterInitialAdditionalCheck() {
        ListClustersResult clustersResult1 = mock(ListClustersResult.class);
        ListClustersResult clustersResult2 = mock(ListClustersResult.class);
        ClusterSummary otherClusterSummary1 = otherClusterSummary(1);
        ClusterSummary otherClusterSummary2 = otherClusterSummary(2);

        List<ClusterSummary> summaries1 = new ArrayList<>();
        summaries1.add(otherClusterSummary1);
        summaries1.add(otherClusterSummary2);

        List<ClusterSummary> summaries2 = new ArrayList<>();
        summaries2.add(otherClusterSummary1);
        summaries2.add(otherClusterSummary2);

        when(clustersResult1.getClusters()).thenReturn(summaries1);
        when(clustersResult2.getClusters()).thenReturn(summaries2);
        AmazonElasticMapReduce emr = mock(AmazonElasticMapReduce.class);
        AtomicBoolean killed = new AtomicBoolean(false);

        when(emr.listClusters(any())).thenReturn(clustersResult1).thenAnswer(x -> {
            killed.set(true);
            return clustersResult2;
        });

        AWSLambda lambda = mock(AWSLambda.class);

        String clusterId = getClusterId(killed, emr, lambda);
        assertEquals("", clusterId);
        verify(emr, times(2)).listClusters(any());
        verifyNoMoreInteractions(emr);
        verifyNoInteractions(lambda);
    }

    private ClusterSummary targetClusterSummary() {
        ClusterSummary targetSummary = mock(ClusterSummary.class);
        when(targetSummary.getName()).thenReturn(TARGET_CLUSTER_NAME);
        when(targetSummary.getId()).thenReturn(TARGET_CLUSTER_ID);
        return targetSummary;
    }

    private ClusterSummary otherClusterSummary(int x) {
        ClusterSummary clusterSummary = mock(ClusterSummary.class);
        when(clusterSummary.getName()).thenReturn("OTHER_CLUSTER_NAME_" + x);
        when(clusterSummary.getId()).thenReturn("OTHER_CLUSTER_ID_" + x);
        return clusterSummary;
    }

    private final String TARGET_CLUSTER_ID = "TARGET_CLUSTER_ID";
    private final String TARGET_CLUSTER_NAME = "TARGET_CLUSTER_NAME";
    private final Props SYSTEM_PROPERTIES = Props.of("emr.boot.poll.interval", "1", "emr.boot.poll.attempts.max", "5", AWS_EMR_CLUSTER_NAME, "cluster");
    private final Props JOB_PROPERTIES = Props.of(AWS_EMR_CLUSTER_NAME, TARGET_CLUSTER_NAME, AWS_EMR_COPY_SECCONFIG, "false", EMR_INITIAL_WAIT, "10");

}
