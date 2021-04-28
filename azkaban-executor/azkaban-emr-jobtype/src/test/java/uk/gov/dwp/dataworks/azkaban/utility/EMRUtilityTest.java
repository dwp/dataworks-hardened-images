package uk.gov.dwp.dataworks.azkaban.utility;

import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.model.ClusterSummary;
import com.amazonaws.services.elasticmapreduce.model.ListClustersRequest;
import com.amazonaws.services.elasticmapreduce.model.ListClustersResult;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class EMRUtilityTest {

    @Test
    void activeClusterSummaries() {
        ListClustersResult page1 = clustersResult(1, 0, 10);
        ListClustersResult page2 = clustersResult(2, 10, 20);
        ListClustersResult page3 = clustersResult(null, 20, 25);
        AmazonElasticMapReduce emr = mock(AmazonElasticMapReduce.class);
        when(emr.listClusters(any())).thenReturn(page1, page2, page3);

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

    private ListClustersResult clustersResult(Integer marker, int startId, int endId) {
        ListClustersResult page1 = mock(ListClustersResult.class);
        when(page1.getMarker()).thenReturn(marker(marker));
        List<ClusterSummary> page1Summaries = paginatedSummaries(startId, endId);
        when(page1.getClusters()).thenReturn(page1Summaries);
        return page1;
    }

    private List<ClusterSummary> paginatedSummaries(int startId, int endId) {
        return IntStream.range(startId, endId).mapToObj(this::clusterSummary).collect(Collectors.toList());
    }

    private ClusterSummary clusterSummary(int x) {
        ClusterSummary summary = mock(ClusterSummary.class);
        when(summary.getId()).thenReturn(clusterId(x));
        return summary;
    }

    private String clusterId(int x) {
        return "CLUSTER-" + x;
    }

    private String marker(Integer x) {
        return x == null ? null : "MARKER-" + x;
    }
}
