package uk.gov.dwp.dataworks.azkaban.services;

import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.model.Step;
import com.amazonaws.services.logs.AWSLogs;
import com.amazonaws.services.logs.model.GetLogEventsRequest;
import com.amazonaws.services.logs.model.GetLogEventsResult;
import com.amazonaws.services.logs.model.OutputLogEvent;
import uk.gov.dwp.dataworks.azkaban.model.EmrStepStatus;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static uk.gov.dwp.dataworks.azkaban.utility.EmrUtility.clusterStep;
import static uk.gov.dwp.dataworks.azkaban.utility.LogUtility.clusterStepLogStreams;

/**
 * Used by the {@link EmrProgressService} to fetch step logs for a running EMR steps
 * from cloudwatch and print them on the console.
 */
public class LogService extends CancellableLoggingService {

    private final String logGroup;
    private final AmazonElasticMapReduce emr;
    private final AWSLogs awsLogs;
    private CountDownLatch logMonitorLatch;

    public LogService(AmazonElasticMapReduce emr, AWSLogs awsLogs, String logGroup) {
        this.emr = emr;
        this.logMonitorLatch = new CountDownLatch(1);
        this.logGroup = logGroup;
        this.awsLogs = awsLogs;
    }

    public void monitorStepLogs(String clusterId, String stepId) {
        if (proceed.get()) {
            if (logGroup != null && logGroup.trim().length() > 0)
                try {
                    final ScheduledExecutorService logsMonitorExecutor = Executors.newSingleThreadScheduledExecutor();
                    logMonitorLatch = new CountDownLatch(1);
                    final AtomicReference<String> logStreamToken = new AtomicReference<>();
                    logsMonitorExecutor
                            .scheduleWithFixedDelay(() -> checkLogStream(clusterId, stepId, logStreamToken), 0, 5,
                                    TimeUnit.SECONDS);

                    logMonitorLatch.await();
                    info("Shutting down '" + stepId + "' logsMonitor executor.");
                    logsMonitorExecutor.shutdownNow();
                    info("Shut down '" + stepId + "' logsMonitor executor: " + "shutdown: " + logsMonitorExecutor
                            .isShutdown() + ", terminated: " + logsMonitorExecutor.isTerminated() + ".");
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
        }
    }

    private void checkLogStream(String clusterId, String stepId, AtomicReference<String> logStreamToken) {
        if (proceed.get()) {
            final GetLogEventsRequest logEventsRequest = new GetLogEventsRequest().withLogGroupName(logGroup)
                                                                                  .withStartFromHead(true);
            try {
                final List<String> logStreams = clusterStepLogStreams(emr, awsLogs, clusterId, logGroup);
                final Step step = clusterStep(emr, clusterId, stepId);

                info("Monitoring step '" + clusterId + "/" + step.getId() + "/" + step.getName() + "/" + step
                        .getStatus().getState() + "', log group: '" + logGroup + "', logStreams: '" + logStreams + "'");

                logStreams.stream().filter(s -> s.contains(step.getName())).findFirst()
                          .ifPresent(logStream -> drainLogStream(logStreamToken, logEventsRequest, step, logStream));

                if (!EmrStepStatus.valueOf(step.getStatus().getState()).isActive()) {
                    logMonitorLatch.countDown();
                }
            } catch (Exception e) {
                error("Error encountered monitoring logs: '" + e.getMessage() + "'", e);
                logMonitorLatch.countDown();
            }
        }
    }

    private void drainLogStream(AtomicReference<String> logStreamToken, GetLogEventsRequest logEventsRequest, Step step,
            String logStream) {
        if (proceed.get()) {
            boolean logsDrained = false;
            do {
                info("Fetching logs for '" + step.getName() + "/" + step.getId() + "/" + step.getStatus().getState()
                        + "'.");
                final GetLogEventsResult result = awsLogs.getLogEvents(logEventsRequest.withLogStreamName(logStream));
                final String nextToken = result.getNextForwardToken();
                if (nextToken != null && !nextToken.equals(logStreamToken.get())) {
                    result.getEvents().stream().map(OutputLogEvent::getMessage).map(String::trim)
                          .forEach(message -> info("Step '" + step.getName() + "/" + step.getId() + "': " + message));
                } else {
                    info("Step '" + step.getName() + "/" + step.getId() + "/" + step.getStatus().getState()
                            + "': no new logs.");
                    logsDrained = true;
                }
                logStreamToken.set(nextToken);
                logEventsRequest.setNextToken(logStreamToken.get());
            }
            while (!logsDrained && EmrStepStatus.valueOf(step.getStatus().getState()).isActive() && proceed.get());
        }
    }

    @Override
    public void cancel() {
        super.cancel();
        this.logMonitorLatch.countDown();
    }
}
