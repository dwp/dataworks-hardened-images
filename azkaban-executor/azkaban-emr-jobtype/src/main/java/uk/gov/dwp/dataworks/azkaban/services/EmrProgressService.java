package uk.gov.dwp.dataworks.azkaban.services;

import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.model.Step;
import com.amazonaws.services.elasticmapreduce.model.StepSummary;
import com.amazonaws.services.logs.AWSLogs;
import com.amazonaws.services.logs.model.AWSLogsException;
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
        this.stepMonitorLatch = new CountDownLatch(1);
        this.logGroup = logGroup;
        this.awsLogs = awsLogs;
    }

    public void observeEmr(String clusterId) {
        if (proceed.get()) {
            monitorClusterStartUp(clusterId).filter(x -> x == EmrClusterStatus.RUNNING)
                    .ifPresent(status -> monitorSteps(clusterId));
        }
    }

    private Optional<EmrClusterStatus> monitorClusterStartUp(String clusterId) {
        try {
            if (proceed.get()) {
                logger.info("Monitoring '" + clusterId + "'");
                final ScheduledExecutorService startupMonitorExecutor = Executors.newSingleThreadScheduledExecutor();
                startupMonitorExecutor
                        .scheduleAtFixedRate(() -> checkClusterStatus(clusterId), 10, 10, TimeUnit.SECONDS);
                clusterStartupLatch.await();
                startupMonitorExecutor.shutdown();
                return Optional.of(clusterStatus(emr, clusterId));
            } else {
                return Optional.empty();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
            return Optional.empty();
        }
    }

    private void monitorSteps(String clusterId) {
        if (proceed.get()) {
            try {
                logger.info("Cluster is running, monitoring steps.");
                final ScheduledExecutorService stepsMonitorExecutor = Executors.newSingleThreadScheduledExecutor();
                stepsMonitorExecutor.scheduleWithFixedDelay(() -> {
                    clusterSteps(emr, clusterId).stream()
                            .filter(x -> EmrStepStatus.valueOf(x.getStatus().getState()) == EmrStepStatus.RUNNING)
                            .findFirst().map(StepSummary::getId).ifPresent(stepId -> monitorStep(clusterId, stepId));

                    if (allStepsFinished(emr, clusterId)) {
                        logger.info("Cluster " + clusterId + " all steps completed: " + completedSteps(emr, clusterId) + ".");
                        stepsMonitorLatch.countDown();
                    } else {
                        logger.info("Cluster " + clusterId + " has incomplete steps: " + incompleteSteps(emr, clusterId) + ".");
                    }

                }, 0, 1, TimeUnit.SECONDS);

                stepsMonitorLatch.await();
                stepsMonitorExecutor.shutdown();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void monitorStep(String clusterId, String stepId) {

        try {
            final ScheduledExecutorService logsMonitorExecutor = Executors.newSingleThreadScheduledExecutor();
            final List<String> logStreams = clusterStepLogStreams(emr, awsLogs, clusterId, logGroup);
            final GetLogEventsRequest logEventsRequest = new GetLogEventsRequest().withLogGroupName(this.logGroup)
                    .withStartFromHead(true);
            stepMonitorLatch = new CountDownLatch(1);
            AtomicReference<String> logStreamToken = new AtomicReference<>();
            logsMonitorExecutor.scheduleWithFixedDelay(() -> {
                try {
                    Step step = clusterStep(emr, clusterId, stepId);
                    logger.info("Monitoring step '" + clusterId + "/" + step.getId() + "/" + step.getName() + "/" + step
                            .getStatus().getState() + "', log group: '" + this.logGroup + "', logStream: '" + stepId
                            + "'");

                    logStreams.stream().filter(s -> s.contains(step.getName())).findFirst().ifPresent(logStream -> {
                        boolean logsDrained = false;
                        do {
                            GetLogEventsResult result = awsLogs.getLogEvents(logEventsRequest.withLogStreamName(logStream));
                            String nextToken = result.getNextForwardToken();
                            if (nextToken != null && !nextToken.equals(logStreamToken.get())) {
                                result.getEvents().stream().map(OutputLogEvent::getMessage)
                                        .map(String::trim)
                                        .forEach(message -> logger.info("Step '" + step.getName() + "/" + step.getId() + "': " + message));
                            } else {
                                logger.info("Step '" + step.getName() + "/" + step.getId() + "/" + step.getStatus().getState() + "': no new logs.");
                                logsDrained = true;
                            }
                            logStreamToken.set(nextToken);
                            logEventsRequest.setNextToken(logStreamToken.get());
                            System.out.println("===============================================================================");
                        } while (!logsDrained);
                    });

                    if (!EmrStepStatus.valueOf(step.getStatus().getState()).isActive()) {
                        stepMonitorLatch.countDown();
                    }
                } catch (AWSLogsException e) {
                    logger.info("Failed to get logs: '" + e.getMessage() + "'");
                }
            }, 0, 5, TimeUnit.SECONDS);

            stepMonitorLatch.await();
            logsMonitorExecutor.shutdown();
            awsLogs.getLogEvents(logEventsRequest);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void cancel() {
        super.cancel();
        this.clusterStartupLatch.countDown();
        this.stepsMonitorLatch.countDown();
        this.stepMonitorLatch.countDown();
    }

    private void checkClusterStatus(String clusterId) {
        EmrClusterStatus state = clusterStatus(emr, clusterId);
        logger.info("Cluster '" + clusterId + "', status is '" + state + "'.");
        if (state == EmrClusterStatus.RUNNING || state.hasCompleted()) {
            clusterStartupLatch.countDown();
        }
    }

    private final CountDownLatch clusterStartupLatch;
    private final CountDownLatch stepsMonitorLatch;
    private CountDownLatch stepMonitorLatch;
    private final String logGroup;
    private final AmazonElasticMapReduce emr;
    private final AWSLogs awsLogs;
    private final Logger logger = LoggerFactory.getLogger(EmrProgressService.class);
}
