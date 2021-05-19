package uk.gov.dwp.dataworks.azkaban.services;

import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.model.Step;
import com.amazonaws.services.elasticmapreduce.model.StepSummary;
import com.amazonaws.services.logs.AWSLogs;
import com.amazonaws.services.logs.model.GetLogEventsRequest;
import com.amazonaws.services.logs.model.GetLogEventsResult;
import com.amazonaws.services.logs.model.OutputLogEvent;
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
import java.util.concurrent.atomic.AtomicReference;

import static uk.gov.dwp.dataworks.azkaban.utility.EmrUtility.*;
import static uk.gov.dwp.dataworks.azkaban.utility.LogUtility.clusterStepLogStreams;

public class EmrProgressService extends AbstractCancellableService {

    public EmrProgressService(AmazonElasticMapReduce emr, AWSLogs awsLogs, String logGroup) {
        this.emr = emr;
        this.clusterStartupLatch = new CountDownLatch(1);
        this.stepsMonitorLatch = new CountDownLatch(1);
        this.logMonitorLatch = new CountDownLatch(1);
        this.logGroup = logGroup;
        this.awsLogs = awsLogs;
    }

    public void observeEmr(String clusterId) {
        try {
            if (proceed.get()) {
                monitorClusterStartUp(clusterId).filter(x -> x == EmrClusterStatus.RUNNING)
                        .ifPresent(status -> monitorSteps(clusterId));
                logger.info("Cluster " + clusterId + " monitoring complete");
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private Optional<EmrClusterStatus> monitorClusterStartUp(String clusterId) throws InterruptedException {
        if (proceed.get()) {
            logger.info("Monitoring '" + clusterId + "'");
            final ScheduledExecutorService startupMonitorExecutor = Executors.newSingleThreadScheduledExecutor();
            startupMonitorExecutor.scheduleAtFixedRate(() -> checkClusterStatus(clusterId), 10, 10, TimeUnit.SECONDS);
            clusterStartupLatch.await();
            logger.info("Shutting down startUp executor");
            startupMonitorExecutor.shutdownNow();
            logger.info(
                    "Shut down startUp executor, shutdown: " + startupMonitorExecutor.isShutdown() + ", terminated: "
                            + startupMonitorExecutor.isTerminated());
            return Optional.of(clusterStatus(emr, clusterId));
        } else {
            return Optional.empty();
        }
    }

    private void checkClusterStatus(String clusterId) {
        try {
            EmrClusterStatus state = clusterStatus(emr, clusterId);
            logger.info("Cluster '" + clusterId + "', status is '" + state + "'.");
            if (state == EmrClusterStatus.RUNNING || state.hasCompleted()) {
                clusterStartupLatch.countDown();
            }
        } catch (Exception e) {
            logger.error("Error encountered monitoring cluster status: '" + e.getMessage() + "'", e);
            clusterStartupLatch.countDown();
        }
    }


    private void monitorSteps(String clusterId) {
        try {
            if (proceed.get()) {
                logger.info("Cluster is running, monitoring steps.");
                final ScheduledExecutorService stepsMonitorExecutor = Executors.newSingleThreadScheduledExecutor();
                stepsMonitorExecutor.scheduleWithFixedDelay(() -> {
                    checkForStepCompletion(clusterId);
                }, 0, 5, TimeUnit.SECONDS);

                stepsMonitorLatch.await();
                logger.info("Shutting down stepsMonitor executor");
                stepsMonitorExecutor.shutdownNow();
                logger.info("Shut down stepsMonitor executor, shutdown: " + stepsMonitorExecutor.isShutdown()
                        + ", terminated: " + stepsMonitorExecutor.isTerminated());
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void checkForStepCompletion(String clusterId) {
        try {
            clusterSteps(emr, clusterId).stream()
                    .filter(x -> EmrStepStatus.valueOf(x.getStatus().getState()) == EmrStepStatus.RUNNING)
                    .findFirst().map(StepSummary::getId).ifPresent(stepId -> monitorStepLogs(clusterId, stepId));

            if (allStepsFinished(emr, clusterId)) {
                logger.info("Cluster " + clusterId + " all steps completed: " + completedSteps(emr, clusterId)
                        + ".");
                stepsMonitorLatch.countDown();
            } else {
                logger.info("Cluster " + clusterId + " has incomplete steps: " + incompleteSteps(emr, clusterId)
                        + ".");
            }
        } catch (Exception e) {
            logger.error("Error encountered monitoring steps: '" + e.getMessage() + "'", e);
            stepsMonitorLatch.countDown();
        }
    }

    private void monitorStepLogs(String clusterId, String stepId) {
        try {
            final ScheduledExecutorService logsMonitorExecutor = Executors.newSingleThreadScheduledExecutor();
            logMonitorLatch = new CountDownLatch(1);
            AtomicReference<String> logStreamToken = new AtomicReference<>();
            logsMonitorExecutor.scheduleWithFixedDelay(() -> {
                drainLogStream(clusterId, stepId, logStreamToken);
            }, 0, 5, TimeUnit.SECONDS);

            logMonitorLatch.await();
            logger.info("Shutting down " + stepId + " logsMonitor executor");
            logsMonitorExecutor.shutdownNow();
            logger.info(
                    "Shut down " + stepId + " logsMonitor executor: " + "shutdown: " + logsMonitorExecutor.isShutdown()
                            + " terminated: " + logsMonitorExecutor.isTerminated());
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void drainLogStream(String clusterId, String stepId, AtomicReference<String> logStreamToken) {
        final GetLogEventsRequest logEventsRequest = new GetLogEventsRequest().withLogGroupName(this.logGroup)
                .withStartFromHead(true);
        try {
            final List<String> logStreams = clusterStepLogStreams(emr, awsLogs, clusterId, logGroup);
            Step step = clusterStep(emr, clusterId, stepId);
            logger.info("Monitoring step '" + clusterId + "/" + step.getId() + "/" + step.getName() + "/" + step
                    .getStatus().getState() + "', log group: '" + this.logGroup + "', logStreams: '"
                    + logStreams + "'");

            logStreams.stream().filter(s -> s.contains(step.getName())).findFirst().ifPresent(logStream -> {
                boolean logsDrained = false;
                do {
                    logger.info(
                            "Fetching logs for '" + step.getName() + "/" + step.getId() + "/" + step.getStatus()
                                    .getState() + "'.");
                    GetLogEventsResult result = awsLogs
                            .getLogEvents(logEventsRequest.withLogStreamName(logStream));
                    String nextToken = result.getNextForwardToken();
                    if (nextToken != null && !nextToken.equals(logStreamToken.get())) {
                        result.getEvents().stream().map(OutputLogEvent::getMessage).map(String::trim).forEach(
                                message -> logger.info("Step '" + step.getName() + "/" + step.getId() + "': "
                                        + message));
                    } else {
                        logger.info("Step '" + step.getName() + "/" + step.getId() + "/" + step.getStatus()
                                .getState() + "': no new logs.");
                        logsDrained = true;
                    }
                    logStreamToken.set(nextToken);
                    logEventsRequest.setNextToken(logStreamToken.get());
                }
                while (!logsDrained && EmrStepStatus.valueOf(step.getStatus().getState()).isActive());
            });

            if (!EmrStepStatus.valueOf(step.getStatus().getState()).isActive()) {
                logMonitorLatch.countDown();
            }
        } catch (Exception e) {
            logger.error("Error encountered monitoring logs: '" + e.getMessage() + "'", e);
            logMonitorLatch.countDown();
        }
    }

    @Override
    public void cancel() {
        super.cancel();
        this.clusterStartupLatch.countDown();
        this.stepsMonitorLatch.countDown();
        this.logMonitorLatch.countDown();
    }

    private final CountDownLatch clusterStartupLatch;
    private final CountDownLatch stepsMonitorLatch;
    private CountDownLatch logMonitorLatch;
    private final String logGroup;
    private final AmazonElasticMapReduce emr;
    private final AWSLogs awsLogs;
    private final Logger logger = LoggerFactory.getLogger(EmrProgressService.class);
}
