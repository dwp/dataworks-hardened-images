package uk.gov.dwp.dataworks.azkaban.services;

import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.model.Step;
import com.amazonaws.services.logs.AWSLogs;
import com.amazonaws.services.logs.model.GetLogEventsRequest;
import com.amazonaws.services.logs.model.GetLogEventsResult;
import com.amazonaws.services.logs.model.OutputLogEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.dwp.dataworks.azkaban.domain.EmrStepStatus;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static uk.gov.dwp.dataworks.azkaban.utility.EmrUtility.clusterStep;
import static uk.gov.dwp.dataworks.azkaban.utility.LogUtility.clusterStepLogStreams;

public class LogService extends AbstractCancellableService {

    private final String logGroup;
    private final AmazonElasticMapReduce emr;
    private final AWSLogs awsLogs;
    private final Logger logger = LoggerFactory.getLogger(EmrProgressService.class);
    private CountDownLatch logMonitorLatch;

    public LogService(AmazonElasticMapReduce emr, AWSLogs awsLogs, String logGroup) {
        this.emr = emr;
        this.logMonitorLatch = new CountDownLatch(1);
        this.logGroup = logGroup;
        this.awsLogs = awsLogs;
    }

    public void monitorStepLogs(String clusterId, String stepId) {
        if (proceed.get()) {
            try {
                final ScheduledExecutorService logsMonitorExecutor = Executors.newSingleThreadScheduledExecutor();
                logMonitorLatch = new CountDownLatch(1);
                final AtomicReference<String> logStreamToken = new AtomicReference<>();
                logsMonitorExecutor
                        .scheduleWithFixedDelay(() -> checkLogStream(clusterId, stepId, logStreamToken), 0, 5,
                                TimeUnit.SECONDS);

                logMonitorLatch.await();
                logger.info("Shutting down " + stepId + " logsMonitor executor");
                logsMonitorExecutor.shutdownNow();
                logger.info("Shut down " + stepId + " logsMonitor executor: " + "shutdown: " + logsMonitorExecutor
                        .isShutdown() + " terminated: " + logsMonitorExecutor.isTerminated());
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void cancel() {
        super.cancel();
        this.logMonitorLatch.countDown();
    }

    private void checkLogStream(String clusterId, String stepId, AtomicReference<String> logStreamToken) {
        if (proceed.get()) {
            final GetLogEventsRequest logEventsRequest = new GetLogEventsRequest().withLogGroupName(this.logGroup)
                                                                                  .withStartFromHead(true);
            try {
                final List<String> logStreams = clusterStepLogStreams(emr, awsLogs, clusterId, logGroup);
                final Step step = clusterStep(emr, clusterId, stepId);
                logger.info("Monitoring step '" + clusterId + "/" + step.getId() + "/" + step.getName() + "/" + step
                        .getStatus().getState() + "', log group: '" + this.logGroup + "', logStreams: '" + logStreams
                        + "'");

                logStreams.stream().filter(s -> s.contains(step.getName())).findFirst()
                          .ifPresent(logStream -> drainLogStream(logStreamToken, logEventsRequest, step, logStream));

                if (!EmrStepStatus.valueOf(step.getStatus().getState()).isActive()) {
                    logMonitorLatch.countDown();
                }
            } catch (Exception e) {
                logger.error("Error encountered monitoring logs: '" + e.getMessage() + "'", e);
                logMonitorLatch.countDown();
            }
        }
    }

    private void drainLogStream(AtomicReference<String> logStreamToken, GetLogEventsRequest logEventsRequest, Step step,
            String logStream) {
        if (proceed.get()) {
            boolean logsDrained = false;
            do {
                logger.info(
                        "Fetching logs for '" + step.getName() + "/" + step.getId() + "/" + step.getStatus().getState()
                                + "'.");
                final GetLogEventsResult result = awsLogs.getLogEvents(logEventsRequest.withLogStreamName(logStream));
                final String nextToken = result.getNextForwardToken();
                if (nextToken != null && !nextToken.equals(logStreamToken.get())) {
                    result.getEvents().stream().map(OutputLogEvent::getMessage).map(String::trim).forEach(
                            message -> logger.info("Step '" + step.getName() + "/" + step.getId() + "': " + message));
                } else {
                    logger.info("Step '" + step.getName() + "/" + step.getId() + "/" + step.getStatus().getState()
                            + "': no new logs.");
                    logsDrained = true;
                }
                logStreamToken.set(nextToken);
                logEventsRequest.setNextToken(logStreamToken.get());
            }
            while (!logsDrained && EmrStepStatus.valueOf(step.getStatus().getState()).isActive() && proceed.get());
        }
    }

}
