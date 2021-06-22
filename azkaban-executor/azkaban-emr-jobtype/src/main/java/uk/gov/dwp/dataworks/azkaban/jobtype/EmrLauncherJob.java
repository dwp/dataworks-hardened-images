package uk.gov.dwp.dataworks.azkaban.jobtype;

import azkaban.jobExecutor.AbstractProcessJob;
import azkaban.utils.Props;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.logs.AWSLogs;
import org.apache.log4j.Logger;
import uk.gov.dwp.dataworks.azkaban.services.CollectionStatusService;
import uk.gov.dwp.dataworks.azkaban.services.DataProductStatusService;
import uk.gov.dwp.dataworks.azkaban.services.DependencyService;
import uk.gov.dwp.dataworks.azkaban.services.EmrLaunchAndMonitoringService;
import uk.gov.dwp.dataworks.azkaban.services.EmrProgressService;
import uk.gov.dwp.dataworks.azkaban.services.LaunchInvocationService;
import uk.gov.dwp.dataworks.azkaban.services.LogService;
import uk.gov.dwp.dataworks.azkaban.services.NotificationService;
import uk.gov.dwp.dataworks.azkaban.services.StatusService;
import uk.gov.dwp.dataworks.azkaban.utility.ClientUtility;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

public class EmrLauncherJob extends AbstractProcessJob {

    public final static String JOB_DEPENDENCIES_PARAMETER_NAME = "job.dependencies";
    public final static String COLLECTION_DEPENDENCIES_PARAMETER_NAME = "collection.dependencies";
    public final static String EXPORT_DATE_PARAMETER_NAME = "export.date";
    public final static String EMR_LAUNCHER_LAMBDA_PARAMETER_NAME = "emr.launcher.lambda";
    public final static String DATA_PRODUCT_NAME = "data.product";
    public static final String AWS_LOG_GROUP_PARAMETER_NAME = "aws.log.group.name";
    public static final String CLUSTER_PARAMETER_NAME = "cluster.name";
    public static final String TOPIC_PARAMETER_NAME = "notification.topic.name";
    public static final String SKIP_NOTIFICATIONS_PARAMETER_NAME = "skip.notifications";

    private EmrLaunchAndMonitoringService _service;

    public EmrLauncherJob(final String jobId, final Props sysProps, final Props jobProps, final Logger log) {
        super(jobId, sysProps, jobProps, log);
    }

    @Override
    public void run() throws Exception {
        List<String> collections = Arrays.stream(collectionDependencies()).filter(s -> s.length() > 0).collect(Collectors.toList());
        boolean successful = collections.size() > 0 ?
                    service().launchClusterAndWaitForStepCompletion(dependency(), collections.toArray(new String[0]))
                    : service().launchClusterAndWaitForStepCompletion(dependency());

        if (!successful) {
            throw new Exception("Job failed");
        }
    }

    @Override
    public void cancel() {
        service().cancel();
    }

    private String exportDate() {
        return jobProps.getString(EXPORT_DATE_PARAMETER_NAME, new SimpleDateFormat("yyyy-MM-dd").format(new Date()));
    }

    private String dependency() {
        return jobProps.getString(JOB_DEPENDENCIES_PARAMETER_NAME);
    }

    private String[] collectionDependencies() {
        return jobProps.getString(COLLECTION_DEPENDENCIES_PARAMETER_NAME, "").split(",");
    }

    private boolean skipNotifications() {
        return jobProps.getString(SKIP_NOTIFICATIONS_PARAMETER_NAME, "false").toLowerCase() == "true";
    }

    private synchronized EmrLaunchAndMonitoringService service() {
        if (_service == null) {
            AmazonDynamoDB dynamoDB = ClientUtility.amazonDynamoDb(awsRegion());
            String dataProduct = jobProps
                    .getString(DATA_PRODUCT_NAME, jobProps.getString(EMR_LAUNCHER_LAMBDA_PARAMETER_NAME));

            DataProductStatusService dataProductStatusService = new DataProductStatusService(dynamoDB);
            dataProductStatusService.setParent(this);

            CollectionStatusService collectionStatusService = new CollectionStatusService(dynamoDB);
            collectionStatusService.setParent(this);
            DependencyService dependencyService = new DependencyService(dynamoDB, dataProductStatusService, collectionStatusService, exportDate());
            dependencyService.setParent(this);

            StatusService statusService = new StatusService(dynamoDB, dataProduct);
            LaunchInvocationService launchInvocationService = new LaunchInvocationService(
                    ClientUtility.amazonLambda(awsRegion()), jobProps.getString(EMR_LAUNCHER_LAMBDA_PARAMETER_NAME));
            launchInvocationService.setParent(this);
            String logGroup = this.getJobProps().getString(AWS_LOG_GROUP_PARAMETER_NAME, "");
            AmazonElasticMapReduce emr = ClientUtility.amazonElasticMapReduce(awsRegion());
            AWSLogs logs = ClientUtility.awsLogs(awsRegion());
            LogService logService = new LogService(emr, logs, logGroup);
            logService.setParent(this);
            EmrProgressService emrProgressService = new EmrProgressService(emr, logService);
            emrProgressService.setParent(this);
            NotificationService notificationService = new NotificationService(ClientUtility.amazonSNS(awsRegion()),
                    this.getJobProps().getString(TOPIC_PARAMETER_NAME, "Monitoring"),
                    jobProps.getString(EMR_LAUNCHER_LAMBDA_PARAMETER_NAME), skipNotifications());

            this._service = new EmrLaunchAndMonitoringService(dependencyService, launchInvocationService,
                    emrProgressService, notificationService, statusService, emr);
            this._service.setParent(this);
        }

        return _service;
    }

    private String awsRegion() {
        return this.getSysProps().getString("aws.region", "eu-west-2");
    }
}
