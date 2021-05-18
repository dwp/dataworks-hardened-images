package uk.gov.dwp.dataworks.azkaban.jobtype;

import azkaban.jobExecutor.AbstractProcessJob;
import azkaban.utils.Props;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import org.apache.log4j.Logger;
import uk.gov.dwp.dataworks.azkaban.domain.InvocationPayload;
import uk.gov.dwp.dataworks.azkaban.domain.InvocationResult;
import uk.gov.dwp.dataworks.azkaban.services.EmrLauncherLambdaService;
import uk.gov.dwp.dataworks.azkaban.services.EmrProgressService;
import uk.gov.dwp.dataworks.azkaban.services.PipelineMetadataService;
import uk.gov.dwp.dataworks.azkaban.utility.AwsUtility;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class EmrLauncherJob extends AbstractProcessJob {

    public EmrLauncherJob(final String jobId, final Props sysProps, final Props jobProps, final Logger log) {
        super(jobId, sysProps, jobProps, log);
        this.pipelineMetadataService = new PipelineMetadataService(AwsUtility.amazonDynamoDb(awsRegion()));
        this.emrLauncherLambdaService = new EmrLauncherLambdaService(AwsUtility.amazonLambda(awsRegion()),
                jobProps.getString(EMR_LAUNCHER_LAMBDA_PARAMETER_NAME));
        this.emrProgressService = new EmrProgressService(AwsUtility.amazonElasticMapReduce(awsRegion()));
    }

    @Override
    public void run() {
//        dependencyMetadata()
//                .flatMap(emrLauncherLambdaService::invokeEmrLauncher)
//                .filter(InvocationResult::wasSuccessful)
//                .map(InvocationResult::getClusterId)
//                .ifPresent(emrProgressService::observeEmr);
    }

    private Optional<InvocationPayload> dependencyMetadata() {
        return completedDependencies().filter(xs -> xs.size() > 0).map(xs -> xs.get(0)).map(InvocationPayload::from);
    }

    @Override
    public void cancel() {
        pipelineMetadataService.cancel();
        emrLauncherLambdaService.cancel();
        emrProgressService.cancel();
    }

    private Optional<List<Map<String, AttributeValue>>> completedDependencies() {
        return pipelineMetadataService.successfulDependencies(metadataTableName(), exportDate(), dependencies());
    }

    private String[] dependencies() {
        return jobProps.getString(JOB_DEPENDENCIES_PARAMETER_NAME).split(",");
    }

    private String exportDate() {
        return jobProps.getString(EXPORT_DATE_PARAMETER_NAME, new SimpleDateFormat("yyyy-MM-dd").format(new Date()));
    }

    private String awsRegion() {
        return this.getSysProps().getString("aws.region", "eu-west-2");
    }

    private String metadataTableName() {
        return this.getJobProps().getString(METADATA_TABLE_PARAMETER_NAME, "data_pipeline_metadata");
    }

    private final PipelineMetadataService pipelineMetadataService;
    private final EmrLauncherLambdaService emrLauncherLambdaService;
    private final EmrProgressService emrProgressService;
    public final static String JOB_DEPENDENCIES_PARAMETER_NAME = "job.dependencies";
    public final static String EXPORT_DATE_PARAMETER_NAME = "export.date";
    public final static String METADATA_TABLE_PARAMETER_NAME = "pipeline.metadata.table";
    public final static String EMR_LAUNCHER_LAMBDA_PARAMETER_NAME = "emr.launcher.lambda";
}
