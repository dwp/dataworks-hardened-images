package uk.gov.dwp.dataworks.azkaban.jobtype;

import azkaban.utils.Props;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.model.ClusterSummary;
import com.amazonaws.services.elasticmapreduce.model.ListClustersResult;
import com.amazonaws.services.lambda.AWSLambda;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
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

        ClusterSummary clusterSummary1 = mock(ClusterSummary.class);
        when(clusterSummary1.getName()).thenReturn(OTHER_CLUSTER_ID);
        when(clusterSummary1.getId()).thenReturn(OTHER_CLUSTER_NAME);

        ClusterSummary clusterSummary2 = mock(ClusterSummary.class);
        when(clusterSummary2.getName()).thenReturn(TARGET_CLUSTER_NAME);
        when(clusterSummary2.getId()).thenReturn(TARGET_CLUSTER_ID);

        List<ClusterSummary> summaries = new ArrayList<>();
        summaries.add(clusterSummary1);
        summaries.add(clusterSummary2);

        when(clustersResult.getClusters()).thenReturn(summaries);

        AWSLambda lambda = mock(AWSLambda.class);
        String clusterId = EMRStep.getClusterId(emr, lambda, SYSTEM_PROPERTIES, JOB_PROPERTIES, new AtomicBoolean(false), Logger.getLogger(EMRStep.class));
        assertEquals(TARGET_CLUSTER_ID, clusterId);
        verify(emr, times(1)).listClusters(any());
        verifyNoMoreInteractions(emr);
        verifyNoInteractions(lambda);
    }


    @Test
    void shouldNotStartClusterIfClusterStartsAfterInitialCheck() {
        ListClustersResult clustersResult1 = mock(ListClustersResult.class);
        ListClustersResult clustersResult2 = mock(ListClustersResult.class);

        ClusterSummary clusterSummary1_1 = mock(ClusterSummary.class);
        when(clusterSummary1_1.getName()).thenReturn(OTHER_CLUSTER_NAME + "_1_1");
        when(clusterSummary1_1.getId()).thenReturn(OTHER_CLUSTER_ID + "_1_1");

        ClusterSummary clusterSummary1_2 = mock(ClusterSummary.class);
        when(clusterSummary1_2.getName()).thenReturn(OTHER_CLUSTER_NAME + "_1_2");
        when(clusterSummary1_2.getId()).thenReturn(OTHER_CLUSTER_ID + "_1_2");

        ClusterSummary clusterSummary2_1 = mock(ClusterSummary.class);
        when(clusterSummary2_1.getName()).thenReturn(OTHER_CLUSTER_NAME+ "_2_1");
        when(clusterSummary2_1.getId()).thenReturn(OTHER_CLUSTER_ID + "_2_1");
        ClusterSummary clusterSummary2_2 = mock(ClusterSummary.class);
        when(clusterSummary2_2.getName()).thenReturn(TARGET_CLUSTER_NAME);
        when(clusterSummary2_2.getId()).thenReturn(TARGET_CLUSTER_ID);

        List<ClusterSummary> summaries1 = new ArrayList<>();
        summaries1.add(clusterSummary1_1);
        summaries1.add(clusterSummary1_2);

        List<ClusterSummary> summaries2 = new ArrayList<>();
        summaries2.add(clusterSummary2_1);
        summaries2.add(clusterSummary2_2);

        when(clustersResult1.getClusters()).thenReturn(summaries1);
        when(clustersResult2.getClusters()).thenReturn(summaries2);
        AmazonElasticMapReduce emr = mock(AmazonElasticMapReduce.class);
        when(emr.listClusters(any())).thenReturn(clustersResult1).thenReturn(clustersResult2);

        AWSLambda lambda = mock(AWSLambda.class);

        String clusterId = EMRStep.getClusterId(emr, lambda, SYSTEM_PROPERTIES, JOB_PROPERTIES, new AtomicBoolean(false), Logger.getLogger(EMRStep.class));
        assertEquals(TARGET_CLUSTER_ID, clusterId);
        verify(emr, times(2)).listClusters(any());
        verifyNoMoreInteractions(emr);
        verifyNoInteractions(lambda);
    }

    private final String TARGET_CLUSTER_ID = "test_cluster_id";
    private final String TARGET_CLUSTER_NAME = "test_cluster_name";
    private final String OTHER_CLUSTER_ID = "other_cluster_id";
    private final String OTHER_CLUSTER_NAME = "other_cluster_name";
    private final Props SYSTEM_PROPERTIES = Props.of("emr.boot.poll.interval", "1", "emr.boot.poll.attempts.max", "5", AWS_EMR_CLUSTER_NAME, "cluster");
    private final Props JOB_PROPERTIES = Props.of(AWS_EMR_CLUSTER_NAME, TARGET_CLUSTER_NAME, AWS_EMR_COPY_SECCONFIG, "false", EMR_INITIAL_WAIT, "10");

}
