package uk.gov.dwp.dataworks.azkaban.utility;

import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.model.ClusterSummary;
import com.amazonaws.services.elasticmapreduce.model.ListClustersRequest;
import com.amazonaws.services.elasticmapreduce.model.ListClustersResult;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class EMRUtility {

    public static List<ClusterSummary> activeClusterSummaries(AmazonElasticMapReduce emr) {
        List<ClusterSummary> clusters = new ArrayList<>();
        String marker = "";
        do {
            ListClustersRequest clustersRequest = clusterRequest(marker);
            ListClustersResult clustersResult = emr.listClusters(clustersRequest);
            marker = clustersResult.getMarker();
            clusters.addAll(clustersResult.getClusters());
        } while (marker != null && !marker.trim().equals(""));

        return clusters;
    }

    private static ListClustersRequest clusterRequest(String marker) {
        ListClustersRequest clustersRequest = new ListClustersRequest();
        clustersRequest.setClusterStates(Arrays.asList("STARTING", "BOOTSTRAPPING", "WAITING", "RUNNING"));
        if (marker != null && !marker.trim().equals("")) {
            clustersRequest.setMarker(marker);
        }
        return clustersRequest;
    }

}
