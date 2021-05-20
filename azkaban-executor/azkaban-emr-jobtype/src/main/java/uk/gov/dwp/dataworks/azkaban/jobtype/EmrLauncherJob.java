package uk.gov.dwp.dataworks.azkaban.jobtype;

import azkaban.jobExecutor.AbstractProcessJob;
import azkaban.utils.Props;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.logs.AWSLogs;
import org.apache.log4j.Logger;
import uk.gov.dwp.dataworks.azkaban.services.CompositeService;
import uk.gov.dwp.dataworks.azkaban.services.EmrLauncherLambdaService;
import uk.gov.dwp.dataworks.azkaban.services.EmrProgressService;
import uk.gov.dwp.dataworks.azkaban.services.LogService;
import uk.gov.dwp.dataworks.azkaban.services.NotificationService;
import uk.gov.dwp.dataworks.azkaban.services.PipelineMetadataService;
import uk.gov.dwp.dataworks.azkaban.utility.ClientUtility;

import java.text.SimpleDateFormat;
import java.util.Date;

public class EmrLauncherJob extends AbstractProcessJob {

    public EmrLauncherJob(final String jobId, final Props sysProps, final Props jobProps, final Logger log) {
        super(jobId, sysProps, jobProps, log);
    }

    @Override
    public void run() {
        service().launchClusterAndWaitForStepCompletion(metadataTableName(), exportDate(), dependencies());
    }

    @Override
    public void cancel() throws Exception {
        super.cancel();
        service().cancel();
    }

    private String metadataTableName() {
        return this.getJobProps().getString(METADATA_TABLE_PARAMETER_NAME, "data_pipeline_metadata");
    }

    private String exportDate() {
        return jobProps.getString(EXPORT_DATE_PARAMETER_NAME, new SimpleDateFormat("yyyy-MM-dd").format(new Date()));
    }

    private String[] dependencies() {
        return jobProps.getString(JOB_DEPENDENCIES_PARAMETER_NAME).split(",");
    }

    private synchronized CompositeService service() {
        if (_service == null) {
            PipelineMetadataService pipelineMetadataService = new PipelineMetadataService(
                    ClientUtility.amazonDynamoDb(awsRegion()));
            EmrLauncherLambdaService emrLauncherLambdaService = new EmrLauncherLambdaService(
                    ClientUtility.amazonLambda(awsRegion()), jobProps.getString(EMR_LAUNCHER_LAMBDA_PARAMETER_NAME));

            String logGroup = this.getJobProps().getString(AWS_LOG_GROUP_PARAMETER_NAME, "/aws/emr/azkaban");
            AmazonElasticMapReduce emr = ClientUtility.amazonElasticMapReduce(awsRegion());
            AWSLogs logs = ClientUtility.awsLogs(awsRegion());

            EmrProgressService emrProgressService = new EmrProgressService(emr, new LogService(emr, logs, logGroup));

            NotificationService notificationService = new NotificationService(ClientUtility.amazonSNS(awsRegion()),
                    this.getJobProps().getString(TOPIC_PARAMETER_NAME, "Monitoring"),
                    jobProps.getString(EMR_LAUNCHER_LAMBDA_PARAMETER_NAME));

            this._service = new CompositeService(pipelineMetadataService, emrLauncherLambdaService, emrProgressService,
                    notificationService, emr);

        }
        return _service;
    }

    private String awsRegion() {
        return this.getSysProps().getString("aws.region", "eu-west-2");
    }

    public final static String JOB_DEPENDENCIES_PARAMETER_NAME = "job.dependencies";
    public final static String EXPORT_DATE_PARAMETER_NAME = "export.date";
    public final static String METADATA_TABLE_PARAMETER_NAME = "pipeline.metadata.table";
    public final static String EMR_LAUNCHER_LAMBDA_PARAMETER_NAME = "emr.launcher.lambda";
    public static final String AWS_LOG_GROUP_PARAMETER_NAME = "aws.log.group.name";
    public static final String CLUSTER_PARAMETER_NAME = "cluster.name";
    public static final String TOPIC_PARAMETER_NAME = "notification.topic.name";
    private CompositeService _service;
}
