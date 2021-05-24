package uk.gov.dwp.dataworks.azkaban.services;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import org.junit.jupiter.api.Test;
import uk.gov.dwp.dataworks.azkaban.model.InvocationResult;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

class CompositeServiceTest {

    @Test
    public void shouldCallEachServiceInTurnIfSuccessful() {
        DependencyService dependencyService = successfulPipelineMetadataService();

        EmrLauncherLambdaService emrLauncherLambdaService = mock(EmrLauncherLambdaService.class);
        InvocationResult invocationResult = mock(InvocationResult.class);
        when(invocationResult.wasSuccessful()).thenReturn(true);
        when(invocationResult.getClusterId()).thenReturn(CLUSTER_ID);
        when(emrLauncherLambdaService.invokeEmrLauncher(any())).thenReturn(Optional.of(invocationResult));

        EmrProgressService emrProgressService = mock(EmrProgressService.class);
        when(emrProgressService.observeEmr(any())).thenReturn(true);

        NotificationService notificationService = mock(NotificationService.class);
        StatusService statusService = mock(StatusService.class);
        AmazonElasticMapReduce emr = mock(AmazonElasticMapReduce.class);

        CompositeService service = new CompositeService(dependencyService, emrLauncherLambdaService,
                emrProgressService, notificationService, statusService, emr);

        boolean successful = service
                .launchClusterAndWaitForStepCompletion(METADATA_TABLE_NAME, EXPORT_DATE, DEPENDENCY);

        verify(dependencyService, times(1)).successfulDependencies(any());
        verifyNoMoreInteractions(dependencyService);

        verify(statusService, times(1)).registerDependenciesCompleted(any());
        verify(statusService, times(1)).registerClusterId(any());
        verify(statusService, times(1)).registerSuccess();
        verifyNoMoreInteractions(statusService);

        verify(emrLauncherLambdaService, times(1)).invokeEmrLauncher(any());
        verifyNoMoreInteractions(emrLauncherLambdaService);

        verify(emrProgressService, times(1)).observeEmr(any());
        verifyNoMoreInteractions(emrProgressService);

        verify(notificationService, times(1)).notifyStarted();
        verify(notificationService, times(1)).notifySucceeded();
        verifyNoMoreInteractions(notificationService);

        assertTrue(successful);
    }

    @Test
    public void shouldNotCallSubsequentServicesIfDependencyCheckFails() {
        DependencyService dependencyService = mock(DependencyService.class);
        when(dependencyService.successfulDependencies(METADATA_TABLE_NAME, EXPORT_DATE, DEPENDENCY))
                .thenReturn(Optional.empty());

        EmrLauncherLambdaService emrLauncherLambdaService = mock(EmrLauncherLambdaService.class);

        EmrProgressService emrProgressService = mock(EmrProgressService.class);
        NotificationService notificationService = mock(NotificationService.class);
        StatusService statusService = mock(StatusService.class);
        AmazonElasticMapReduce emr = mock(AmazonElasticMapReduce.class);
        CompositeService service = new CompositeService(dependencyService, emrLauncherLambdaService,
                emrProgressService, notificationService, statusService, emr);
        boolean successful = service
                .launchClusterAndWaitForStepCompletion(METADATA_TABLE_NAME, EXPORT_DATE, DEPENDENCY);

        verify(dependencyService, times(1)).successfulDependencies(any());
        verifyNoMoreInteractions(dependencyService);

        verify(statusService, times(1)).registerFailure();
        verifyNoMoreInteractions(statusService);

        verifyNoInteractions(emrLauncherLambdaService);
        verifyNoInteractions(emrProgressService);

        verify(notificationService, times(1)).notifyStarted();
        verify(notificationService, times(1)).notifyFailed();
        verifyNoMoreInteractions(notificationService);

        assertFalse(successful);
    }

    @Test
    public void shouldNotCallSubsequentServicesIfDependencyCheckReturnsEmptyList() {
        DependencyService dependencyService = mock(DependencyService.class);
        when(dependencyService.successfulDependencies(METADATA_TABLE_NAME, EXPORT_DATE, DEPENDENCY))
                .thenReturn(Optional.of(new ArrayList<>()));

        EmrLauncherLambdaService emrLauncherLambdaService = mock(EmrLauncherLambdaService.class);

        EmrProgressService emrProgressService = mock(EmrProgressService.class);
        NotificationService notificationService = mock(NotificationService.class);
        StatusService statusService = mock(StatusService.class);
        AmazonElasticMapReduce emr = mock(AmazonElasticMapReduce.class);
        CompositeService service = new CompositeService(dependencyService, emrLauncherLambdaService,
                emrProgressService, notificationService, statusService, emr);
        boolean successful = service
                .launchClusterAndWaitForStepCompletion(METADATA_TABLE_NAME, EXPORT_DATE, DEPENDENCY);

        verify(dependencyService, times(1)).successfulDependencies(any());
        verifyNoMoreInteractions(dependencyService);

        verify(statusService, times(1)).registerFailure();
        verifyNoMoreInteractions(statusService);

        verifyNoInteractions(emrLauncherLambdaService);
        verifyNoInteractions(emrProgressService);

        verify(notificationService, times(1)).notifyStarted();
        verify(notificationService, times(1)).notifyFailed();
        verifyNoMoreInteractions(notificationService);

        assertFalse(successful);
    }

    @Test
    public void shouldNotCallSubsequentServicesInLambdaInvocationFails() {
        DependencyService dependencyService = successfulPipelineMetadataService();

        EmrLauncherLambdaService emrLauncherLambdaService = mock(EmrLauncherLambdaService.class);
        InvocationResult invocationResult = mock(InvocationResult.class);
        when(invocationResult.wasSuccessful()).thenReturn(false);
        when(invocationResult.getClusterId()).thenReturn(CLUSTER_ID);
        when(emrLauncherLambdaService.invokeEmrLauncher(any())).thenReturn(Optional.of(invocationResult));

        EmrProgressService emrProgressService = mock(EmrProgressService.class);
        NotificationService notificationService = mock(NotificationService.class);
        StatusService statusService = mock(StatusService.class);
        AmazonElasticMapReduce emr = mock(AmazonElasticMapReduce.class);

        CompositeService service = new CompositeService(dependencyService, emrLauncherLambdaService,
                emrProgressService, notificationService, statusService, emr);

        boolean successful = service
                .launchClusterAndWaitForStepCompletion(METADATA_TABLE_NAME, EXPORT_DATE, DEPENDENCY);

        verify(dependencyService, times(1)).successfulDependencies(any());
        verifyNoMoreInteractions(dependencyService);

        verify(statusService, times(1)).registerDependenciesCompleted(any());
        verify(statusService, times(1)).registerFailure();
        verifyNoMoreInteractions(statusService);

        verify(emrLauncherLambdaService, times(1)).invokeEmrLauncher(any());
        verifyNoMoreInteractions(emrLauncherLambdaService);

        verifyNoInteractions(emrProgressService);

        verify(notificationService, times(1)).notifyStarted();
        verify(notificationService, times(1)).notifyFailed();
        verifyNoMoreInteractions(notificationService);

        assertFalse(successful);
    }

    @Test
    public void shouldNotCallSubsequentServicesInLambdaInvocationReturnsEmpty() {
        DependencyService dependencyService = successfulPipelineMetadataService();

        EmrLauncherLambdaService emrLauncherLambdaService = mock(EmrLauncherLambdaService.class);
        when(emrLauncherLambdaService.invokeEmrLauncher(any())).thenReturn(Optional.empty());

        EmrProgressService emrProgressService = mock(EmrProgressService.class);
        NotificationService notificationService = mock(NotificationService.class);
        StatusService statusService = mock(StatusService.class);
        AmazonElasticMapReduce emr = mock(AmazonElasticMapReduce.class);

        CompositeService service = new CompositeService(dependencyService, emrLauncherLambdaService,
                emrProgressService, notificationService, statusService, emr);

        boolean successful = service
                .launchClusterAndWaitForStepCompletion(METADATA_TABLE_NAME, EXPORT_DATE, DEPENDENCY);

        verify(dependencyService, times(1)).successfulDependencies(any());
        verifyNoMoreInteractions(dependencyService);

        verify(statusService, times(1)).registerDependenciesCompleted(any());
        verify(statusService, times(1)).registerFailure();
        verifyNoMoreInteractions(statusService);

        verify(emrLauncherLambdaService, times(1)).invokeEmrLauncher(any());
        verifyNoMoreInteractions(emrLauncherLambdaService);

        verifyNoInteractions(emrProgressService);

        verify(notificationService, times(1)).notifyStarted();
        verify(notificationService, times(1)).notifyFailed();
        verifyNoMoreInteractions(notificationService);

        assertFalse(successful);
    }

    @Test
    public void shouldFailIfStepsFail() {
        DependencyService dependencyService = successfulPipelineMetadataService();

        EmrLauncherLambdaService emrLauncherLambdaService = mock(EmrLauncherLambdaService.class);
        InvocationResult invocationResult = mock(InvocationResult.class);
        when(invocationResult.wasSuccessful()).thenReturn(true);
        when(invocationResult.getClusterId()).thenReturn(CLUSTER_ID);
        when(emrLauncherLambdaService.invokeEmrLauncher(any())).thenReturn(Optional.of(invocationResult));

        EmrProgressService emrProgressService = mock(EmrProgressService.class);
        when(emrProgressService.observeEmr(any())).thenReturn(false);

        NotificationService notificationService = mock(NotificationService.class);
        StatusService statusService = mock(StatusService.class);
        AmazonElasticMapReduce emr = mock(AmazonElasticMapReduce.class);

        CompositeService service = new CompositeService(dependencyService, emrLauncherLambdaService,
                emrProgressService, notificationService, statusService, emr);

        boolean successful = service
                .launchClusterAndWaitForStepCompletion(METADATA_TABLE_NAME, EXPORT_DATE, DEPENDENCY);

        verify(dependencyService, times(1)).successfulDependencies(any());
        verifyNoMoreInteractions(dependencyService);

        verify(statusService, times(1)).registerDependenciesCompleted(any());
        verify(statusService, times(1)).registerClusterId(any());
        verify(statusService, times(1)).registerFailure();
        verifyNoMoreInteractions(statusService);

        verify(emrLauncherLambdaService, times(1)).invokeEmrLauncher(any());
        verifyNoMoreInteractions(emrLauncherLambdaService);

        verify(emrProgressService, times(1)).observeEmr(any());
        verifyNoMoreInteractions(emrProgressService);

        verify(notificationService, times(1)).notifyStarted();
        verify(notificationService, times(1)).notifyFailed();
        verifyNoMoreInteractions(notificationService);

        assertFalse(successful);
    }

    @Test
    public void shouldFailIfStepThrowsException() {
        DependencyService dependencyService = mock(DependencyService.class);
        when(dependencyService.successfulDependencies(METADATA_TABLE_NAME, EXPORT_DATE, DEPENDENCY))
                .thenThrow(new RuntimeException("FAILURE"));

        EmrLauncherLambdaService emrLauncherLambdaService = mock(EmrLauncherLambdaService.class);
        EmrProgressService emrProgressService = mock(EmrProgressService.class);
        NotificationService notificationService = mock(NotificationService.class);
        StatusService statusService = mock(StatusService.class);
        AmazonElasticMapReduce emr = mock(AmazonElasticMapReduce.class);

        CompositeService service = new CompositeService(dependencyService, emrLauncherLambdaService,
                emrProgressService, notificationService, statusService, emr);
        boolean successful = service
                .launchClusterAndWaitForStepCompletion(METADATA_TABLE_NAME, EXPORT_DATE, DEPENDENCY);

        verify(dependencyService, times(1)).successfulDependencies(any());
        verifyNoMoreInteractions(dependencyService);

        verify(statusService, times(1)).registerFailure();
        verifyNoMoreInteractions(statusService);

        verify(notificationService, times(1)).notifyStarted();
        verify(notificationService, times(1)).notifyFailed();
        verifyNoMoreInteractions(notificationService);
        assertFalse(successful);
    }

    private DependencyService successfulPipelineMetadataService() {
        DependencyService dependencyService = mock(DependencyService.class);
        List<Map<String, AttributeValue>> completedDependencies = new ArrayList<>();
        Map<String, AttributeValue> completedDependency = new HashMap<>();
        completedDependencies.add(completedDependency);
        when(dependencyService.successfulDependencies(METADATA_TABLE_NAME, EXPORT_DATE, DEPENDENCY))
                .thenReturn(Optional.of(completedDependencies));
        return dependencyService;
    }

    private final static String METADATA_TABLE_NAME = "METADATA_TABLE_NAME";
    private final static String EXPORT_DATE = "EXPORT_DATE";
    private final static String DEPENDENCY = "DEPENDENCY";
    private final static String CLUSTER_ID = "CLUSTER_ID";
}
