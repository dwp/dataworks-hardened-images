package uk.gov.dwp.dataworks.azkaban.services;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import uk.gov.dwp.dataworks.azkaban.model.InvocationPayload;
import uk.gov.dwp.dataworks.azkaban.model.InvocationResult;
import uk.gov.dwp.dataworks.azkaban.utility.EmrUtility;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

public class CompositeService extends CancellableService {

    public CompositeService(PipelineMetadataService pipelineMetadataService,
            EmrLauncherLambdaService emrLauncherLambdaService, EmrProgressService emrProgressService,
            NotificationService notificationService, AmazonElasticMapReduce emr) {
        this.pipelineMetadataService = pipelineMetadataService;
        this.emrLauncherLambdaService = emrLauncherLambdaService;
        this.emrProgressService = emrProgressService;
        this.notificationService = notificationService;
        this.emr = emr;
    }

    public void launchClusterAndWaitForStepCompletion(String metadataTableName, String exportDate, String ... dependencies) {
        try {
            this.notificationService.notifyStarted();
            boolean succeeded = dependencyMetadata(metadataTableName, exportDate, dependencies)
                    .flatMap(emrLauncherLambdaService::invokeEmrLauncher)
                    .filter(InvocationResult::wasSuccessful)
                    .map(InvocationResult::getClusterId).map(this::setGetClusterId)
                    .map(emrProgressService::observeEmr).orElse(false);

            if (succeeded) {
                this.notificationService.notifySucceeded();
            } else {
                this.notificationService.notifyFailed();
            }
        } catch (Exception e) {
            this.notificationService.notifyFailed();
        }

    }

    @Override
    public void cancel() {
        super.cancel();
        pipelineMetadataService.cancel();
        emrLauncherLambdaService.cancel();
        emrProgressService.cancel();
        clusterId.get().ifPresent(id -> EmrUtility.cancelSteps(emr, id));
    }

    private Optional<InvocationPayload> dependencyMetadata(String metadataTableName, String exportDate, String ... dependencies) {
        return completedDependencies(metadataTableName, exportDate, dependencies)
                .filter(xs -> xs.size() > 0)
                .map(xs -> xs.get(0))
                .map(InvocationPayload::from);
    }

    private Optional<List<Map<String, AttributeValue>>> completedDependencies(String metadataTableName, String exportDate, String ... dependencies) {
        return pipelineMetadataService.successfulDependencies(metadataTableName, exportDate, dependencies);
    }

    private String setGetClusterId(String id) {
        clusterId.set(Optional.of(id));
        return id;
    }

    private final AtomicReference<Optional<String>> clusterId = new AtomicReference<>();
    private final PipelineMetadataService pipelineMetadataService;
    private final EmrLauncherLambdaService emrLauncherLambdaService;
    private final EmrProgressService emrProgressService;
    private final NotificationService notificationService;
    private final AmazonElasticMapReduce emr;
}
