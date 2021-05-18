package uk.gov.dwp.dataworks.azkaban.services;

import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.dwp.dataworks.azkaban.domain.EmrClusterStatus;
import uk.gov.dwp.dataworks.azkaban.domain.EmrStepStatus;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class EmrProgressService extends AbstractCancellableService {

    public EmrProgressService(AmazonElasticMapReduce amazonElasticMapReduce) {
        this.amazonElasticMapReduce = amazonElasticMapReduce;
        this.clusterStartupLatch = new CountDownLatch(1);
        this.stepsMonitorLatch = new CountDownLatch(1);
    }

    public void observeEmr(String clusterId) {
        monitorClusterStartUp(clusterId)
                .filter(x -> x == EmrClusterStatus.RUNNING || x == EmrClusterStatus.WAITING)
                .ifPresent(status -> monitorSteps(clusterId));
    }

    private void monitorSteps(String clusterId) {
        if (proceed.get()) {
            try {
                logger.info("Cluster is running, monitoring steps.");
                final ScheduledExecutorService stepsMonitorExecutor = Executors.newSingleThreadScheduledExecutor();
                stepsMonitorExecutor.scheduleWithFixedDelay(() -> {
                    clusterSteps(clusterId).stream()
                            .filter(x -> EmrStepStatus.valueOf(x.getStatus().getState()) == EmrStepStatus.RUNNING)
                            .findFirst().ifPresent(step -> {
                        monitorStep(clusterId, step);
                    });

                    if (allStepsFinished(clusterId)) {
                        stepsMonitorLatch.countDown();
                    } else {
                        logger.info("Cluster " + clusterId + " has incomplete steps: " + incompleteSteps(clusterId) + ".");
                    }

                }, 0, 1, TimeUnit.SECONDS);

                stepsMonitorLatch.await();
                stepsMonitorExecutor.shutdown();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }


    private boolean allStepsFinished(String clusterId) {
        return clusterSteps(clusterId).stream()
                .map(StepSummary::getStatus)
                .map(StepStatus::getState)
                .map(EmrStepStatus::valueOf)
                .noneMatch(EmrStepStatus::isActive);
    }

    private void monitorStep(String clusterId, StepSummary step) {
        System.out.println("MONITORING STEP " + step);
    }

    @Override
    public void cancel() {
        super.cancel();
        this.clusterStartupLatch.countDown();
    }

    private Optional<EmrClusterStatus> monitorClusterStartUp(String clusterId) {
        try {
            if (proceed.get()) {
                logger.info("Observing '" + clusterId + "'");
                final ScheduledExecutorService startupMonitorExecutor = Executors.newSingleThreadScheduledExecutor();
                startupMonitorExecutor.scheduleAtFixedRate(() -> {
                    EmrClusterStatus state = clusterStatus(clusterId);
                    if (state == EmrClusterStatus.RUNNING || state.hasCompleted()) {
                        clusterStartupLatch.countDown();
                    }
                }, 0, 10, TimeUnit.SECONDS);
                clusterStartupLatch.await();
                startupMonitorExecutor.shutdown();
                return Optional.of(clusterStatus(clusterId));
            } else {
                return Optional.empty();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
            return Optional.empty();
        }
    }

    private EmrClusterStatus clusterStatus(String clusterId) {
        DescribeClusterResult clusterDetails = amazonElasticMapReduce.describeCluster(describeClusterRequest(clusterId));
        ClusterStatus status = clusterDetails.getCluster().getStatus();
        logger.info(clusterDetails.getCluster().getName() + "/" + clusterId + " cluster state: '" + status.getState() + "'.");
        return EmrClusterStatus.valueOf(status.getState());
    }

    private String incompleteSteps(String clusterId) {
        return clusterSteps(clusterId).stream()
                .filter(s -> EmrStepStatus.valueOf(s.getStatus().getState()).isActive())
                .map(x -> x.getName() + "/" + x.getStatus().getState())
                .collect(Collectors.joining(", "));
    }

    private List<StepSummary> clusterSteps(String clusterId) {
        ListStepsRequest request = new ListStepsRequest().withClusterId(clusterId);
        ListStepsResult result = amazonElasticMapReduce.listSteps(request);
        return result.getSteps();
    }

    private DescribeClusterRequest describeClusterRequest(String clusterId) {
        return new DescribeClusterRequest().withClusterId(clusterId);
    }

    private final CountDownLatch clusterStartupLatch;
    private CountDownLatch stepsMonitorLatch;
    private final AmazonElasticMapReduce amazonElasticMapReduce;
    private final Logger logger = LoggerFactory.getLogger(EmrProgressService.class);
}
