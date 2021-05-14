package uk.gov.dwp.dataworks.azkaban.jobtype;

import azkaban.jobExecutor.AbstractProcessJob;
import azkaban.utils.Props;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import org.apache.log4j.Logger;
import uk.gov.dwp.dataworks.azkaban.services.DataPipelineMetadataService;
import uk.gov.dwp.dataworks.azkaban.utility.AwsUtility;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class EmrLauncherJob extends AbstractProcessJob {

    private final DataPipelineMetadataService dataPipelineMetadataService;

    public EmrLauncherJob(String jobId, Props sysProps, Props jobProps, Logger log) {
        super(jobId, sysProps, jobProps, log);
        this.dataPipelineMetadataService = new DataPipelineMetadataService(AwsUtility.amazonDynamoDb(awsRegion()));
    }

    @Override
    public void run() {
        List<Map<String, AttributeValue>> metadata = dataPipelineMetadataService.waitForDependencies(metadataTableName(),
                jobProps.getString(EXPORT_DATE_PARAMETER_NAME, new SimpleDateFormat("yyyy-MM-dd").format(new Date())),
                jobProps.getString(JOB_DEPENDENCIES_PARAMETER_NAME).split(","));
    }

    private String awsRegion() {
        return this.getSysProps().getString("aws.region", "eu-west-2");
    }

    private String metadataTableName() {
        return this.getJobProps().getString(METADATA_TABLE_PARAMETER_NAME, "data_pipeline_metadata");
    }

    public final static String JOB_DEPENDENCIES_PARAMETER_NAME = "job.dependencies";
    public final static String EXPORT_DATE_PARAMETER_NAME = "export.date";
    public final static String METADATA_TABLE_PARAMETER_NAME = "pipeline.metadata.table";
}
