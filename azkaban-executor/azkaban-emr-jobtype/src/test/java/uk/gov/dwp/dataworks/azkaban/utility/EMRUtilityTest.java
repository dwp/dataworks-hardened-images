package uk.gov.dwp.dataworks.azkaban.utility;

import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.model.ClusterSummary;
import com.amazonaws.services.elasticmapreduce.model.ListClustersRequest;
import com.amazonaws.services.elasticmapreduce.model.ListClustersResult;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class EMRUtilityTest {

    @Test
    void activeClusterIdReturnsOptionalOfIdWhenFound() {
        IntStream.range(0, 25).forEach(this::verifyActiveCluster);
    }

    @Test
    void activeClusterIdReturnsEmptyOptionalWhenNotFound() {
        IntStream.range(0, 25).forEach(this::verifyClusterNotFound);
    }

    @Test
    void activeClusterSummariesReturnsAllPaginatedResults() {
        AmazonElasticMapReduce emr = emrClient();
        List<ClusterSummary> summaries = EMRUtility.activeClusterSummaries(emr);
        ArgumentCaptor<ListClustersRequest> requestCaptor = ArgumentCaptor.forClass(ListClustersRequest.class);
        assertEquals(25, summaries.size());
        IntStream.range(0, 25).forEach(x -> assertEquals(clusterId(x), summaries.get(x).getId()));
        verify(emr, times(3)).listClusters(requestCaptor.capture());
        List<ListClustersRequest> requests = requestCaptor.getAllValues();
        assertEquals(3, requests.size());
        assertNull(requests.get(0).getMarker());
        assertEquals(marker(1), requests.get(1).getMarker());
        assertEquals(marker(2), requests.get(2).getMarker());
    }

    private AmazonElasticMapReduce emrClient() {
        ListClustersResult page1 = clustersResult(1, 0, 10);
        ListClustersResult page2 = clustersResult(2, 10, 20);
        ListClustersResult page3 = clustersResult(null, 20, 25);
        AmazonElasticMapReduce emr = mock(AmazonElasticMapReduce.class);
        when(emr.listClusters(any())).thenReturn(page1, page2, page3);
        return emr;
    }

    private ListClustersResult clustersResult(Integer marker, int startId, int endId) {
        ListClustersResult page1 = mock(ListClustersResult.class);
        when(page1.getMarker()).thenReturn(marker(marker));
        List<ClusterSummary> page1Summaries = paginatedSummaries(startId, endId);
        when(page1.getClusters()).thenReturn(page1Summaries);
        return page1;
    }

    private void verifyActiveCluster(int x) {
        verifyClusterId("CLUSTER-" + x, "J-" + x, "NOT FOUND " + x);
    }

    private void verifyClusterNotFound(int x) {
        verifyClusterId("NON_ACTIVE_CLUSTER-" + x, "NOT FOUND " + x, "NOT FOUND " + x);
    }


    private void verifyClusterId(String clusterName, String expected, String optionalElse) {
        AmazonElasticMapReduce emr = emrClient();
        Optional<String> clusterId = EMRUtility.activeClusterId(emr, clusterName);
        String id = clusterId.orElse(optionalElse);
        assertEquals(expected, id);
    }

    private List<ClusterSummary> paginatedSummaries(int startId, int endId) {
        return IntStream.range(startId, endId).mapToObj(this::clusterSummary).collect(Collectors.toList());
    }

    private ClusterSummary clusterSummary(int x) {
        ClusterSummary summary = mock(ClusterSummary.class);
        when(summary.getId()).thenReturn(clusterId(x));
        when(summary.getName()).thenReturn(clusterName(x));
        return summary;
    }

    private String clusterName(int x) {
        return "CLUSTER-" + x;
    }

    private String clusterId(int x) {
        return "J-" + x;
    }

    private String marker(Integer x) {
        return x == null ? null : "MARKER-" + x;
    }
}
