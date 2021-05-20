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
        PipelineMetadataService pipelineMetadataService = successfulPipelineMetadataService();

        EmrLauncherLambdaService emrLauncherLambdaService = mock(EmrLauncherLambdaService.class);
        InvocationResult invocationResult = mock(InvocationResult.class);
        when(invocationResult.wasSuccessful()).thenReturn(true);
        when(invocationResult.getClusterId()).thenReturn(CLUSTER_ID);
        when(emrLauncherLambdaService.invokeEmrLauncher(any())).thenReturn(Optional.of(invocationResult));

        EmrProgressService emrProgressService = mock(EmrProgressService.class);
        when(emrProgressService.observeEmr(any())).thenReturn(true);

        NotificationService notificationService = mock(NotificationService.class);

        AmazonElasticMapReduce emr = mock(AmazonElasticMapReduce.class);

        CompositeService service = new CompositeService(pipelineMetadataService, emrLauncherLambdaService,
                emrProgressService, notificationService, emr);

        boolean successful = service
                .launchClusterAndWaitForStepCompletion(METADATA_TABLE_NAME, EXPORT_DATE, DEPENDENCY);

        verify(pipelineMetadataService, times(1)).successfulDependencies(any(), any(), any());
        verifyNoMoreInteractions(pipelineMetadataService);

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
        PipelineMetadataService pipelineMetadataService = mock(PipelineMetadataService.class);
        when(pipelineMetadataService.successfulDependencies(METADATA_TABLE_NAME, EXPORT_DATE, DEPENDENCY))
                .thenReturn(Optional.empty());

        EmrLauncherLambdaService emrLauncherLambdaService = mock(EmrLauncherLambdaService.class);

        EmrProgressService emrProgressService = mock(EmrProgressService.class);
        NotificationService notificationService = mock(NotificationService.class);
        AmazonElasticMapReduce emr = mock(AmazonElasticMapReduce.class);
        CompositeService service = new CompositeService(pipelineMetadataService, emrLauncherLambdaService,
                emrProgressService, notificationService, emr);
        boolean successful = service
                .launchClusterAndWaitForStepCompletion(METADATA_TABLE_NAME, EXPORT_DATE, DEPENDENCY);

        verify(pipelineMetadataService, times(1)).successfulDependencies(any(), any(), any());
        verifyNoMoreInteractions(pipelineMetadataService);

        verifyNoInteractions(emrLauncherLambdaService);
        verifyNoInteractions(emrProgressService);

        verify(notificationService, times(1)).notifyStarted();
        verify(notificationService, times(1)).notifyFailed();
        verifyNoMoreInteractions(notificationService);

        assertFalse(successful);
    }

    @Test
    public void shouldNotCallSubsequentServicesIfDependencyCheckReturnsEmptyList() {
        PipelineMetadataService pipelineMetadataService = mock(PipelineMetadataService.class);
        when(pipelineMetadataService.successfulDependencies(METADATA_TABLE_NAME, EXPORT_DATE, DEPENDENCY))
                .thenReturn(Optional.of(new ArrayList<>()));

        EmrLauncherLambdaService emrLauncherLambdaService = mock(EmrLauncherLambdaService.class);

        EmrProgressService emrProgressService = mock(EmrProgressService.class);
        NotificationService notificationService = mock(NotificationService.class);
        AmazonElasticMapReduce emr = mock(AmazonElasticMapReduce.class);
        CompositeService service = new CompositeService(pipelineMetadataService, emrLauncherLambdaService,
                emrProgressService, notificationService, emr);
        boolean successful = service
                .launchClusterAndWaitForStepCompletion(METADATA_TABLE_NAME, EXPORT_DATE, DEPENDENCY);

        verify(pipelineMetadataService, times(1)).successfulDependencies(any(), any(), any());
        verifyNoMoreInteractions(pipelineMetadataService);

        verifyNoInteractions(emrLauncherLambdaService);
        verifyNoInteractions(emrProgressService);

        verify(notificationService, times(1)).notifyStarted();
        verify(notificationService, times(1)).notifyFailed();
        verifyNoMoreInteractions(notificationService);

        assertFalse(successful);
    }

    @Test
    public void shouldNotCallSubsequentServicesInLambdaInvocationFails() {
        PipelineMetadataService pipelineMetadataService = successfulPipelineMetadataService();

        EmrLauncherLambdaService emrLauncherLambdaService = mock(EmrLauncherLambdaService.class);
        InvocationResult invocationResult = mock(InvocationResult.class);
        when(invocationResult.wasSuccessful()).thenReturn(false);
        when(invocationResult.getClusterId()).thenReturn(CLUSTER_ID);
        when(emrLauncherLambdaService.invokeEmrLauncher(any())).thenReturn(Optional.of(invocationResult));

        EmrProgressService emrProgressService = mock(EmrProgressService.class);
        NotificationService notificationService = mock(NotificationService.class);
        AmazonElasticMapReduce emr = mock(AmazonElasticMapReduce.class);

        CompositeService service = new CompositeService(pipelineMetadataService, emrLauncherLambdaService,
                emrProgressService, notificationService, emr);

        boolean successful = service
                .launchClusterAndWaitForStepCompletion(METADATA_TABLE_NAME, EXPORT_DATE, DEPENDENCY);

        verify(pipelineMetadataService, times(1)).successfulDependencies(any(), any(), any());
        verifyNoMoreInteractions(pipelineMetadataService);

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
        PipelineMetadataService pipelineMetadataService = successfulPipelineMetadataService();

        EmrLauncherLambdaService emrLauncherLambdaService = mock(EmrLauncherLambdaService.class);
        when(emrLauncherLambdaService.invokeEmrLauncher(any())).thenReturn(Optional.empty());

        EmrProgressService emrProgressService = mock(EmrProgressService.class);
        NotificationService notificationService = mock(NotificationService.class);
        AmazonElasticMapReduce emr = mock(AmazonElasticMapReduce.class);

        CompositeService service = new CompositeService(pipelineMetadataService, emrLauncherLambdaService,
                emrProgressService, notificationService, emr);

        boolean successful = service
                .launchClusterAndWaitForStepCompletion(METADATA_TABLE_NAME, EXPORT_DATE, DEPENDENCY);

        verify(pipelineMetadataService, times(1)).successfulDependencies(any(), any(), any());
        verifyNoMoreInteractions(pipelineMetadataService);

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
        PipelineMetadataService pipelineMetadataService = successfulPipelineMetadataService();

        EmrLauncherLambdaService emrLauncherLambdaService = mock(EmrLauncherLambdaService.class);
        InvocationResult invocationResult = mock(InvocationResult.class);
        when(invocationResult.wasSuccessful()).thenReturn(true);
        when(invocationResult.getClusterId()).thenReturn(CLUSTER_ID);
        when(emrLauncherLambdaService.invokeEmrLauncher(any())).thenReturn(Optional.of(invocationResult));

        EmrProgressService emrProgressService = mock(EmrProgressService.class);
        when(emrProgressService.observeEmr(any())).thenReturn(false);

        NotificationService notificationService = mock(NotificationService.class);

        AmazonElasticMapReduce emr = mock(AmazonElasticMapReduce.class);

        CompositeService service = new CompositeService(pipelineMetadataService, emrLauncherLambdaService,
                emrProgressService, notificationService, emr);

        boolean successful = service
                .launchClusterAndWaitForStepCompletion(METADATA_TABLE_NAME, EXPORT_DATE, DEPENDENCY);

        verify(pipelineMetadataService, times(1)).successfulDependencies(any(), any(), any());
        verifyNoMoreInteractions(pipelineMetadataService);

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
        PipelineMetadataService pipelineMetadataService = mock(PipelineMetadataService.class);
        when(pipelineMetadataService.successfulDependencies(METADATA_TABLE_NAME, EXPORT_DATE, DEPENDENCY))
                .thenThrow(new RuntimeException("FAILURE"));

        EmrLauncherLambdaService emrLauncherLambdaService = mock(EmrLauncherLambdaService.class);
        EmrProgressService emrProgressService = mock(EmrProgressService.class);
        NotificationService notificationService = mock(NotificationService.class);
        AmazonElasticMapReduce emr = mock(AmazonElasticMapReduce.class);

        CompositeService service = new CompositeService(pipelineMetadataService, emrLauncherLambdaService,
                emrProgressService, notificationService, emr);
        boolean successful = service
                .launchClusterAndWaitForStepCompletion(METADATA_TABLE_NAME, EXPORT_DATE, DEPENDENCY);

        verify(pipelineMetadataService, times(1)).successfulDependencies(any(), any(), any());
        verifyNoMoreInteractions(pipelineMetadataService);
        verify(notificationService, times(1)).notifyStarted();
        verify(notificationService, times(1)).notifyFailed();
        verifyNoMoreInteractions(notificationService);
        assertFalse(successful);
    }

    private PipelineMetadataService successfulPipelineMetadataService() {
        PipelineMetadataService pipelineMetadataService = mock(PipelineMetadataService.class);
        List<Map<String, AttributeValue>> completedDependencies = new ArrayList<>();
        Map<String, AttributeValue> completedDependency = new HashMap<>();
        completedDependencies.add(completedDependency);
        when(pipelineMetadataService.successfulDependencies(METADATA_TABLE_NAME, EXPORT_DATE, DEPENDENCY))
                .thenReturn(Optional.of(completedDependencies));
        return pipelineMetadataService;
    }

    private final static String METADATA_TABLE_NAME = "METADATA_TABLE_NAME";
    private final static String EXPORT_DATE = "EXPORT_DATE";
    private final static String DEPENDENCY = "DEPENDENCY";
    private final static String CLUSTER_ID = "CLUSTER_ID";
}
