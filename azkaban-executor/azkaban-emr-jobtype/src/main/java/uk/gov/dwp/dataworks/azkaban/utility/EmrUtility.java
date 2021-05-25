package uk.gov.dwp.dataworks.azkaban.utility;

import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.model.CancelStepsRequest;
import com.amazonaws.services.elasticmapreduce.model.CancelStepsResult;
import com.amazonaws.services.elasticmapreduce.model.ClusterStatus;
import com.amazonaws.services.elasticmapreduce.model.ClusterSummary;
import com.amazonaws.services.elasticmapreduce.model.DescribeClusterRequest;
import com.amazonaws.services.elasticmapreduce.model.DescribeClusterResult;
import com.amazonaws.services.elasticmapreduce.model.DescribeStepRequest;
import com.amazonaws.services.elasticmapreduce.model.DescribeStepResult;
import com.amazonaws.services.elasticmapreduce.model.Instance;
import com.amazonaws.services.elasticmapreduce.model.ListClustersRequest;
import com.amazonaws.services.elasticmapreduce.model.ListClustersResult;
import com.amazonaws.services.elasticmapreduce.model.ListInstancesRequest;
import com.amazonaws.services.elasticmapreduce.model.ListInstancesResult;
import com.amazonaws.services.elasticmapreduce.model.ListStepsRequest;
import com.amazonaws.services.elasticmapreduce.model.ListStepsResult;
import com.amazonaws.services.elasticmapreduce.model.Step;
import com.amazonaws.services.elasticmapreduce.model.StepStatus;
import com.amazonaws.services.elasticmapreduce.model.StepSummary;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import uk.gov.dwp.dataworks.azkaban.model.EmrClusterStatus;
import uk.gov.dwp.dataworks.azkaban.model.EmrStepStatus;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class EmrUtility {

    private final static Logger logger = LogManager.getLogger(EmrUtility.class);

    public static void cancelSteps(AmazonElasticMapReduce emr, String clusterId) {
        List<String> stepIds = EmrUtility.clusterSteps(emr, clusterId).stream()
                                         .filter(s -> EmrStepStatus.valueOf(s.getStatus().getState()).isActive())
                                         .map(StepSummary::getId).collect(Collectors.toList());

        if (stepIds.size() > 0) {
            CancelStepsRequest request = new CancelStepsRequest().withClusterId(clusterId).withStepIds(stepIds);
            logger.info("Cancelling steps '" + stepIds + "' on clusterId: '" + clusterId + "'.");
            CancelStepsResult result = emr.cancelSteps(request);
            result.getCancelStepsInfoList().forEach(cancelInfo -> logger
                    .info("Cancelled step '" + cancelInfo.getStepId() + "', " + "cancellation status: '" + cancelInfo
                            .getStatus() + "', " + "cancellation reason '" + cancelInfo.getReason() + "'."));
        }
    }

    public static boolean allStepsFinished(AmazonElasticMapReduce emr, String clusterId) {
        return clusterStepStatuses(emr, clusterId).noneMatch(EmrStepStatus::isActive);
    }

    private static Stream<EmrStepStatus> clusterStepStatuses(AmazonElasticMapReduce emr, String clusterId) {
        return clusterSteps(emr, clusterId).stream().map(StepSummary::getStatus).map(StepStatus::getState)
                                           .map(EmrStepStatus::valueOf);
    }

    public static List<StepSummary> clusterSteps(AmazonElasticMapReduce emr, String clusterId) {
        ListStepsRequest request = new ListStepsRequest().withClusterId(clusterId);
        ListStepsResult result = emr.listSteps(request);
        return result.getSteps();
    }

    public static boolean allStepsSucceeded(AmazonElasticMapReduce emr, String clusterId) {
        return clusterStepStatuses(emr, clusterId).allMatch(s -> s == EmrStepStatus.COMPLETED);
    }

    public static EmrClusterStatus clusterStatus(AmazonElasticMapReduce emr, String clusterId) {
        DescribeClusterResult clusterDetails = emr.describeCluster(describeClusterRequest(clusterId));
        ClusterStatus status = clusterDetails.getCluster().getStatus();
        return EmrClusterStatus.valueOf(status.getState());
    }

    private static DescribeClusterRequest describeClusterRequest(String clusterId) {
        return new DescribeClusterRequest().withClusterId(clusterId);
    }

    public static String incompleteSteps(AmazonElasticMapReduce emr, String clusterId) {
        return clusterSteps(emr, clusterId).stream()
                                           .filter(s -> EmrStepStatus.valueOf(s.getStatus().getState()).isActive())
                                           .map(x -> x.getId() + "/" + x.getName() + "/" + x.getStatus().getState())
                                           .collect(Collectors.joining(", "));
    }

    public static String completedSteps(AmazonElasticMapReduce emr, String clusterId) {
        return clusterSteps(emr, clusterId).stream()
                                           .filter(s -> !EmrStepStatus.valueOf(s.getStatus().getState()).isActive())
                                           .map(x -> x.getId() + "/" + x.getName() + "/" + x.getStatus().getState())
                                           .collect(Collectors.joining(", "));
    }

    public static Set<String> clusterInstances(AmazonElasticMapReduce emr, String clusterId) {
        final ListInstancesRequest instancesRequest = new ListInstancesRequest().withClusterId(clusterId);
        final ListInstancesResult instancesResult = emr.listInstances(instancesRequest);
        return instancesResult.getInstances().stream().map(Instance::getEc2InstanceId).collect(Collectors.toSet());
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
        }
        while (marker != null && !marker.trim().equals(""));

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

    public static boolean isRunning(StepSummary x) {
        return EmrStepStatus.valueOf(x.getStatus().getState()) == EmrStepStatus.RUNNING;
    }
}
