package uk.gov.dwp.dataworks.azkaban.services;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import uk.gov.dwp.dataworks.azkaban.model.InvocationPayload;
import uk.gov.dwp.dataworks.azkaban.model.InvocationResult;
import uk.gov.dwp.dataworks.azkaban.utility.EmrUtility;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

public class CompositeService extends DelegateService {


    public CompositeService(DependencyService dependencyService,
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
                    .map(this::setGetClusterId)
                    .map(this::registerClusterId)
                    .filter(x -> proceed.get())
                    .map(emrProgressService::observeEmr)
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

    private String registerClusterId(String x) {
        this.statusService.registerClusterId(x);
        return x;
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
        return completedDependency(dependencies, collections).map(InvocationPayload::from);
    }

    private Optional<Map<String, AttributeValue>> completedDependency(String dependency, String ... collections) {
        return dependencyService.successfulDependency(dependency, collections);
    }

    private String setGetClusterId(String id) {
        clusterId.set(Optional.of(id));
        return id;
    }

    private final AtomicReference<Optional<String>> clusterId = new AtomicReference<>();
    private final DependencyService dependencyService;
    private final LaunchInvocationService launchInvocationService;
    private final EmrProgressService emrProgressService;
    private final NotificationService notificationService;
    private final StatusService statusService;
    private final AmazonElasticMapReduce emr;
}
