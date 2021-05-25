package uk.gov.dwp.dataworks.azkaban.services;

import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.model.StepSummary;
import uk.gov.dwp.dataworks.azkaban.model.EmrClusterStatus;
import uk.gov.dwp.dataworks.azkaban.utility.EmrUtility;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static uk.gov.dwp.dataworks.azkaban.utility.EmrUtility.allStepsFinished;
import static uk.gov.dwp.dataworks.azkaban.utility.EmrUtility.allStepsSucceeded;
import static uk.gov.dwp.dataworks.azkaban.utility.EmrUtility.clusterStatus;
import static uk.gov.dwp.dataworks.azkaban.utility.EmrUtility.clusterSteps;
import static uk.gov.dwp.dataworks.azkaban.utility.EmrUtility.completedSteps;
import static uk.gov.dwp.dataworks.azkaban.utility.EmrUtility.incompleteSteps;

public class EmrProgressService extends CancellableLoggingService {

    private final CountDownLatch clusterStartupLatch;
    private final CountDownLatch stepsMonitorLatch;
    private final AmazonElasticMapReduce emr;
    private final LogService logService;

    public EmrProgressService(AmazonElasticMapReduce emr, LogService logService) {
        this.emr = emr;
        this.clusterStartupLatch = new CountDownLatch(1);
        this.stepsMonitorLatch = new CountDownLatch(1);
        this.logService = logService;
    }

    public boolean waitForCluster(String clusterId) {
        if (proceed.get()) {
            try {
                return monitorClusterStartUp(clusterId).filter(x -> x == EmrClusterStatus.RUNNING)
                                                       .map(status -> monitorSteps(clusterId)).orElse(false);
            } catch (InterruptedException e) {
                return false;
            }
        } else {
            return false;
        }
    }

    private Optional<EmrClusterStatus> monitorClusterStartUp(String clusterId) throws InterruptedException {
        if (proceed.get()) {
            info("Monitoring '" + clusterId + "'");
            final ScheduledExecutorService startupMonitorExecutor = Executors.newSingleThreadScheduledExecutor();
            startupMonitorExecutor.scheduleAtFixedRate(() -> checkClusterStatus(clusterId), 0, 10, TimeUnit.SECONDS);
            clusterStartupLatch.await();
            info("Shutting down startupMonitorExecutor executor");
            startupMonitorExecutor.shutdownNow();
            info("Shut down startupMonitorExecutor executor, shutdown: " + startupMonitorExecutor.isShutdown()
                    + ", terminated: " + startupMonitorExecutor.isTerminated());
            return Optional.of(clusterStatus(emr, clusterId));
        } else {
            return Optional.empty();
        }
    }

    private boolean monitorSteps(String clusterId) {
        if (proceed.get()) {
            try {
                info("Cluster is running, monitoring steps.");
                final ScheduledExecutorService stepsMonitorExecutor = Executors.newSingleThreadScheduledExecutor();
                stepsMonitorExecutor
                        .scheduleWithFixedDelay(() -> checkForStepCompletion(clusterId), 0, 5, TimeUnit.SECONDS);
                stepsMonitorLatch.await();
                info("Shutting down stepsMonitor executor");
                stepsMonitorExecutor.shutdownNow();
                info("Shut down stepsMonitor executor, shutdown: " + stepsMonitorExecutor.isShutdown()
                        + ", terminated: " + stepsMonitorExecutor.isTerminated());
                return allStepsSucceeded(emr, clusterId);
            } catch (InterruptedException e) {
                error("Steps monitoring interrupted", e);
                return false;
            }
        } else {
            return false;
        }
    }

    private void checkClusterStatus(String clusterId) {
        if (proceed.get()) {
            try {
                final EmrClusterStatus state = clusterStatus(emr, clusterId);
                info("Cluster '" + clusterId + "', status is '" + state + "'.");
                if (state == EmrClusterStatus.RUNNING || state.hasCompleted()) {
                    clusterStartupLatch.countDown();
                }
            } catch (Exception e) {
                error("Error encountered monitoring cluster status: '" + e.getMessage() + "'", e);
                clusterStartupLatch.countDown();
            }
        }
    }

    private void checkForStepCompletion(String clusterId) {
        if (proceed.get()) {
            try {
                clusterSteps(emr, clusterId).stream().filter(EmrUtility::isRunning).findFirst().map(StepSummary::getId)
                                            .ifPresent(stepId -> logService.monitorStepLogs(clusterId, stepId));
                if (allStepsFinished(emr, clusterId)) {

                    info("Cluster '" + clusterId + "' all steps completed: " + completedSteps(emr, clusterId) + ".");
                    stepsMonitorLatch.countDown();
                } else {
                    info("Cluster '" + clusterId + "' has incomplete steps: " + incompleteSteps(emr, clusterId) + ".");
                }
            } catch (Exception e) {
                error("Error encountered monitoring steps: '" + e.getMessage() + "'", e);
                stepsMonitorLatch.countDown();
            }
        }
    }

    @Override
    public void cancel() {
        super.cancel();
        this.logService.cancel();
        this.clusterStartupLatch.countDown();
        this.stepsMonitorLatch.countDown();
    }
}
