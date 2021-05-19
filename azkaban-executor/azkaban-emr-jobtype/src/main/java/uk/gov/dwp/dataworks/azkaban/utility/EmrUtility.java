package uk.gov.dwp.dataworks.azkaban.utility;

import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.model.*;
import uk.gov.dwp.dataworks.azkaban.domain.EmrClusterStatus;
import uk.gov.dwp.dataworks.azkaban.domain.EmrStepStatus;

import java.util.*;
import java.util.stream.Collectors;

public class EmrUtility {

    public static boolean allStepsFinished(AmazonElasticMapReduce emr, String clusterId) {
        return clusterSteps(emr, clusterId).stream().map(StepSummary::getStatus).map(StepStatus::getState)
                .map(EmrStepStatus::valueOf).noneMatch(EmrStepStatus::isActive);
    }

    public static EmrClusterStatus clusterStatus(AmazonElasticMapReduce emr, String clusterId) {
        DescribeClusterResult clusterDetails = emr.describeCluster(describeClusterRequest(clusterId));
        ClusterStatus status = clusterDetails.getCluster().getStatus();
        return EmrClusterStatus.valueOf(status.getState());
    }

    public static String incompleteSteps(AmazonElasticMapReduce emr, String clusterId) {
        return clusterSteps(emr, clusterId).stream().filter(s -> EmrStepStatus.valueOf(s.getStatus().getState()).isActive())
                .map(x -> x.getId() + "/" + x.getName() + "/" + x.getStatus().getState())
                .collect(Collectors.joining(", "));
    }

    public static String completedSteps(AmazonElasticMapReduce emr, String clusterId) {
        return clusterSteps(emr, clusterId).stream().filter(s -> !EmrStepStatus.valueOf(s.getStatus().getState()).isActive())
                .map(x -> x.getId() + "/" + x.getName() + "/" + x.getStatus().getState())
                .collect(Collectors.joining(", "));
    }


    public static Set<String> clusterInstances(AmazonElasticMapReduce emr, String clusterId) {
        final ListInstancesRequest instancesRequest = new ListInstancesRequest().withClusterId(clusterId);
        final ListInstancesResult instancesResult = emr.listInstances(instancesRequest);
        return instancesResult.getInstances().stream()
                .map(Instance::getEc2InstanceId).collect(Collectors.toSet());
    }

    public static Step clusterStep(AmazonElasticMapReduce emr, String clusterId, String stepId) {
        DescribeStepRequest request = new DescribeStepRequest().withClusterId(clusterId).withStepId(stepId);
        DescribeStepResult result = emr.describeStep(request);
        return result.getStep();
    }

    public static Optional<String> activeClusterId(AmazonElasticMapReduce emr, String clusterName) {
        return activeClusterSummaries(emr).stream().filter(x -> x.getName().equals(clusterName))
                .map(ClusterSummary::getId).findFirst();
    }

    public static List<ClusterSummary> activeClusterSummaries(AmazonElasticMapReduce emr) {
        List<ClusterSummary> clusters = new ArrayList<>();
        String marker = "";
        do {
            ListClustersResult clustersResult = emr.listClusters(clusterRequest(marker));
            marker = clustersResult.getMarker();
            clusters.addAll(clustersResult.getClusters());
        } while (marker != null && !marker.trim().equals(""));

        return clusters;
    }

    public static boolean isRunning(StepSummary x) {
        return EmrStepStatus.valueOf(x.getStatus().getState()) == EmrStepStatus.RUNNING;
    }


    private static ListClustersRequest clusterRequest(String marker) {
        ListClustersRequest clustersRequest = new ListClustersRequest();
        clustersRequest.setClusterStates(Arrays.asList("STARTING", "BOOTSTRAPPING", "WAITING", "RUNNING"));
        if (marker != null && !marker.trim().equals("")) {
            clustersRequest.setMarker(marker);
        }
        return clustersRequest;
    }

    public static List<StepSummary> clusterSteps(AmazonElasticMapReduce emr, String clusterId) {
        ListStepsRequest request = new ListStepsRequest().withClusterId(clusterId);
        ListStepsResult result = emr.listSteps(request);
        return result.getSteps();
    }

    private static DescribeClusterRequest describeClusterRequest(String clusterId) {
        return new DescribeClusterRequest().withClusterId(clusterId);
    }

}
