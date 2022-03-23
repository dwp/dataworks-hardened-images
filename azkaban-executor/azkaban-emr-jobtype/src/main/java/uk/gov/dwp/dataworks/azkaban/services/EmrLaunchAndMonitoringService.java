package uk.gov.dwp.dataworks.azkaban.services;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import uk.gov.dwp.dataworks.azkaban.model.InvocationPayload;
import uk.gov.dwp.dataworks.azkaban.model.InvocationResult;
import uk.gov.dwp.dataworks.azkaban.utility.EmrUtility;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Orchestrates the whole launch and subsequent monitoring.
 * First it waits until the dependencies have been met (i.e. that the build of the upstream
 * products has completed) this job is delegated to the {@link DependencyService}
 * Next it calls the emr launching lambda (delegated to the {@link LaunchInvocationService}
 * Finally it monitors the EMR (delegated to the {@link EmrProgressService}) until it
 * completes its steps.
 * Whilst doing this SNS messages are issued (delegated to the {@link NotificationService}
 * and DynamoDb entries are updated (delegated to the {@link StatusService}) to indicate how the
 * process is going (has it started, succeeded, failed, etc.)
 */
public class EmrLaunchAndMonitoringService extends CancellableLoggingService {

    private final AtomicReference<Optional<String>> clusterId = new AtomicReference<>();
    private final DependencyService dependencyService;
    private final LaunchInvocationService launchInvocationService;
    private final EmrProgressService emrProgressService;
    private final NotificationService notificationService;
    private final StatusService statusService;
    private final AmazonElasticMapReduce emr;

    public EmrLaunchAndMonitoringService(DependencyService dependencyService,
            LaunchInvocationService launchInvocationService, EmrProgressService emrProgressService,
            NotificationService notificationService, StatusService statusService, AmazonElasticMapReduce emr) {
        this.dependencyService = dependencyService;
        this.launchInvocationService = launchInvocationService;
        this.emrProgressService = emrProgressService;
        this.notificationService = notificationService;
        this.statusService = statusService;

        this.emr = emr;
    }

    public boolean launchClusterAndWaitForStepCompletion(String dependency, String ... collections) {
        try {
            this.notificationService.notifyStarted();
            boolean succeeded = dependencyMetadata(dependency, collections)
                    .map(this::registerDependenciesCompleted)
                    .filter(x -> proceed.get())
                    .flatMap(launchInvocationService::invokeEmrLauncher)
                    .filter(InvocationResult::wasSuccessful)
                    .map(InvocationResult::getClusterId)
                    .map(this::setClusterId)
                    .map(this::registerClusterId)
                    .filter(x -> proceed.get())
                    .map(emrProgressService::waitForCluster)
                    .orElse(false);

            if (succeeded) {
                this.statusService.registerSuccess();
                this.notificationService.notifySucceeded();
                return true;
            } else {
                this.statusService.registerFailure();
                this.notificationService.notifyFailed();
                return false;
            }
        } catch (Exception e) {
            error("Job failed", e);
            this.statusService.registerFailure();
            this.notificationService.notifyFailed();
            return false;
        }
    }

    private String registerClusterId(String clusterId) {
        this.statusService.registerClusterId(clusterId);
        return clusterId;
    }

    private InvocationPayload registerDependenciesCompleted(InvocationPayload payload) {
        this.statusService.registerDependenciesCompleted(payload);
        return payload;
    }

    @Override
    public void cancel() {
        super.cancel();
        dependencyService.cancel();
        launchInvocationService.cancel();
        emrProgressService.cancel();
        clusterId.get().ifPresent(id -> EmrUtility.cancelSteps(emr, id));
    }

    private Optional<InvocationPayload> dependencyMetadata(String dependencies, String ... collections) {
        return completedDependency(dependencies, collections).map(InvocationPayload::from); //Return values from retrived metadata (correlationId etc...)
    }

    private Optional<Map<String, AttributeValue>> completedDependency(String dependency, String ... collections) {
        return dependencyService.successfulDependency(dependency, collections);
    }

    private String setClusterId(String id) {
        clusterId.set(Optional.of(id));
        return id;
    }
}
