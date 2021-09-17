package uk.gov.dwp.dataworks.azkaban.jobtype;

import azkaban.Constants;
import azkaban.flow.CommonJobProperties;
import azkaban.jobExecutor.AbstractProcessJob;
import azkaban.jobExecutor.utils.process.AzkabanProcess;
import azkaban.metrics.CommonMetrics;
import azkaban.utils.ExecuteAsUser;
import azkaban.utils.Pair;
import azkaban.utils.Props;
import azkaban.utils.Utils;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;
import com.amazonaws.services.elasticmapreduce.util.StepFactory;
import com.amazonaws.services.lambda.AWSLambda;
import com.amazonaws.services.lambda.AWSLambdaClientBuilder;
import com.amazonaws.services.lambda.model.InvocationType;
import com.amazonaws.services.lambda.model.InvokeRequest;
import com.amazonaws.services.lambda.model.InvokeResult;
import com.amazonaws.services.logs.AWSLogsClient;
import com.amazonaws.services.logs.model.AWSLogsException;
import com.amazonaws.services.logs.model.GetLogEventsRequest;
import com.amazonaws.services.logs.model.GetLogEventsResult;
import com.amazonaws.services.logs.model.OutputLogEvent;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import uk.gov.dwp.dataworks.azkaban.utility.EmrUtility;
import uk.gov.dwp.dataworks.azkaban.utility.UserUtility;
import uk.gov.dwp.dataworks.lambdas.EMRConfiguration;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static azkaban.Constants.ConfigurationKeys.AZKABAN_SERVER_GROUP_NAME;
import static azkaban.Constants.ConfigurationKeys.AZKABAN_SERVER_NATIVE_LIB_FOLDER;
import static azkaban.ServiceProvider.SERVICE_PROVIDER;

/**
 * A job that runs an AWS EMR step
 */
public class EMRStep extends AbstractProcessJob {

    public static final String COMMAND = "step";
    public static final String AWS_EMR_CLUSTER_NAME = "aws.emr.cluster.name";
    public static final String AWS_EMR_CLUSTER_STATEFILE = "aws.emr.cluster.statefile";
    public static final String AWS_EMR_CLUSTER_CONFIG = "aws.emr.cluster.config";
    public static final String EMR_INITIAL_WAIT = "emr.initial.wait";
    public static final String AWS_EMR_STEP_SCRIPT = "aws.emr.step.script";
    public static final String AWS_EMR_STEP_NAME = "aws.emr.step.name";
    public static final String AWS_EMR_COPY_SECCONFIG = "aws.emr.copy.secconfig";
    public static final String AZKABAN_MEMORY_CHECK = "azkaban.memory.check";
    public static final String AWS_LOG_GROUP_NAME = "aws.log.group.name";
    public static final String AWS_REGION = "aws.region";
    public static final String AZKABAN_SERVICE_USER = "azkaban.service.user";
    public static final String USE_EMR_USER = "use.emr.user";
    // Use azkaban.Constants.ConfigurationKeys.AZKABAN_SERVER_NATIVE_LIB_FOLDER instead
    @Deprecated
    public static final String NATIVE_LIB_FOLDER = "azkaban.native.lib";
    public static final String EXECUTE_AS_USER = "execute.as.user";
    public static final String KRB5CCNAME = "KRB5CCNAME";
    private static final Duration KILL_TIME = Duration.ofSeconds(30);
    private static final String MEMCHECK_ENABLED = "memCheck.enabled";
    private static final String CHOWN = "/bin/chown";
    private static final String CREATE_FILE = "touch";
    private static final int SUCCESSFUL_EXECUTION = 0;
    private static final String TEMP_FILE_NAME = "user_can_write";
    private static final int MAX_STEPS = 256;
    private static final int POLL_INTERVAL = 10000;
    private static final String BOOT_POLL_INTERVAL = "emr.boot.poll.interval";
    private static final int BOOT_POLL_INTERVAL_DEFAULT = 300000; /* 5 mins */
    private static final String BOOT_POLL_ATTEMPTS_MAX = "emr.boot.poll.attempts.max";
    private static final int BOOT_POLL_ATTEMPTS_MAX_DEFAULT = 5;
    private final CommonMetrics commonMetrics;
    private volatile AzkabanProcess process;
    private final AtomicBoolean killed = new AtomicBoolean(false);
    // For testing only. True if the job process exits successfully.
    private volatile boolean success;
    private volatile List<String> stepIds;

    public EMRStep(final String jobId, final Props sysProps, final Props jobProps, final Logger log) {
        super(jobId, sysProps, jobProps, log);
        // TODO: reallocf fully guicify CommonMetrics through ProcessJob dependents
        this.commonMetrics = SERVICE_PROVIDER.getInstance(CommonMetrics.class);
    }

    @Override
    public void run() throws Exception {
        info("Running EMR Step...");

        try {
            resolveProps();
        } catch (final Exception e) {
            handleError("Bad property definition! " + e.getMessage(), e);
        }

        final File[] propFiles = initPropsFiles();

        // determine whether to run as Azkaban or run as effectiveUser,
        // by default, run as effectiveUser
        String executeAsUserBinaryPath = null;
        String effectiveUser = null;
        final boolean isExecuteAsUser = this.getSysProps().getBoolean(EXECUTE_AS_USER, true);

        //Get list of users we never execute flows as. (ie: root, azkaban)
        final Set<String> blackListedUsers = new HashSet<>(Arrays.asList(
                this.getSysProps().getString(Constants.ConfigurationKeys.BLACK_LISTED_USERS, "root,azkaban")
                    .split(",")));

        // nativeLibFolder specifies the path for execute-as-user file,
        // which will change user from Azkaban to effectiveUser
        if (isExecuteAsUser) {
            final String nativeLibFolder = this.getSysProps().getString(AZKABAN_SERVER_NATIVE_LIB_FOLDER);
            executeAsUserBinaryPath = String.format("%s/%s", nativeLibFolder, "execute-as-user");
            effectiveUser = UserUtility.effectiveUser(jobProps, sysProps);
            info("Effective user: '" + effectiveUser + "'");
            // Throw exception if Azkaban tries to run flow as a prohibited user
            if (blackListedUsers.contains(effectiveUser)) {
                throw new RuntimeException(
                        String.format("Not permitted to proxy as '%s' through Azkaban", effectiveUser));
            }
            // Set parent directory permissions to <uid>:azkaban so user can write in their execution directory
            // if the directory is not permissioned correctly already (should happen once per execution)
            if (!canWriteInCurrentWorkingDirectory(effectiveUser)) {
                info("Changing current working directory ownership");
                assignUserFileOwnership(effectiveUser, getWorkingDirectory());
            }
            // Set property file permissions to <uid>:azkaban so user can write to their prop files
            // in order to pass properties from one job to another, except the last one
            for (int i = 0; i < 2; i++) {
                info("Changing properties files ownership");
                assignUserFileOwnership(effectiveUser, propFiles[i].getAbsolutePath());
            }
        }

        this.logJobProperties();

        String awsRegion = this.getSysProps().getString(AWS_REGION, "eu-west-2");

        AmazonElasticMapReduce emr = AmazonElasticMapReduceClientBuilder.standard().withRegion(awsRegion).build();

        String clusterId = getClusterId(emr, AWSLambdaClientBuilder.defaultClient(), this.getSysProps(), this.getJobProps(), killed, getLog());
        info("Got cluster id: '" + clusterId + "'");
        if (killed.get()) {
            info("Job has been killed so exiting run");
            return;
        }

        configureCluster(emr, clusterId);

        // Passing in the FLOW_UUID to be used as a correlation ID in EMR steps
        ArrayList<String> args = new ArrayList<>();
        args.add(this.getJobProps().getString(CommonJobProperties.FLOW_UUID));
        String emrUser = UserUtility.emrUser(sysProps, effectiveUser, this::getUserGroup);
        info("Script to be run as emr user '" + emrUser + "'");
        args.add(emrUser);
        args.addAll(retrieveScript(this.getJobProps().getString(COMMAND)));
        args.add(retrieveScriptArguments(this.getJobProps().getString(COMMAND)));

        StepFactory stepFactory = new StepFactory();

        StepConfig runBashScript = new StepConfig().withName(this.getSysProps().getString(AWS_EMR_STEP_NAME))
                                                   .withHadoopJarStep(new StepFactory(awsRegion + ".elasticmapreduce")
                                                           .newScriptRunnerStep(
                                                                   this.getSysProps().getString(AWS_EMR_STEP_SCRIPT),
                                                                   args.toArray(new String[args.size()])))
                                                   .withActionOnFailure("CONTINUE");

        if (killed.get()) {
            info("Job has been killed, aborting");
            return;
        }

        AddJobFlowStepsResult result = emr
                .addJobFlowSteps(new AddJobFlowStepsRequest().withJobFlowId(clusterId).withSteps(runBashScript));

        this.stepIds = result.getStepIds();
        String stepId = this.stepIds.get(0);

        AWSLogsClient logsClient = new AWSLogsClient().withRegion(RegionUtils.getRegion(awsRegion));

        String logGroupName = this.getSysProps().getString(AWS_LOG_GROUP_NAME, "/aws/emr/azkaban");

        boolean stepCompleted = false;

        GetLogEventsRequest getLogEventsRequest = new GetLogEventsRequest().withLogGroupName(logGroupName)
                                                                           .withLogStreamName(stepId)
                                                                           .withStartFromHead(true);

        info("Loop starting");
        String lastToken = "";

        while (!stepCompleted) {
            Thread.sleep(POLL_INTERVAL);

            if (killed.get()) {
                info("Stopping waiting for step to complete due to job being killed");
                return;
            }

            Pair<Boolean, String> completionStatus = getStepStatus(emr, clusterId, stepId);
            stepCompleted = completionStatus.getFirst();

            if (stepCompleted && !completionStatus.getSecond().equals("COMPLETED")) {
                error(String.format("Step %s did not successfully complete. Reason: %s", stepId,
                        completionStatus.getSecond()));
                throw new RuntimeException(String.format("Step %s did not successfully complete. Reason: %s", stepId,
                        completionStatus.getSecond()));
            }

            try {
                GetLogEventsResult logResult = logsClient.getLogEvents(getLogEventsRequest);
                printLogs(logResult);
                getLogEventsRequest = new GetLogEventsRequest().withLogGroupName(logGroupName).withLogStreamName(stepId)
                                                               .withStartFromHead(true)
                                                               .withNextToken(logResult.getNextForwardToken());
                lastToken = logResult.getNextForwardToken();
            } catch (AWSLogsException e) {
                info("Waiting for logs to become available");
            }
        }

        // Drain logs.
        String nextToken = "";

        try {
            nextToken = lastToken;

            getLogEventsRequest = new GetLogEventsRequest().withLogGroupName(logGroupName).withLogStreamName(stepId)
                                                           .withStartFromHead(true).withNextToken(nextToken);

            GetLogEventsResult logResult = logsClient.getLogEvents(getLogEventsRequest);
            printLogs(logResult);

        } catch (AWSLogsException e) {
            info("Error reading logs: " + e.getErrorMessage());
            nextToken = lastToken;
        }

        // Get the output properties from this job.
        generateProperties(propFiles[1]);

        info("EMR Step Complete");
    }

    private void printLogs(GetLogEventsResult logResult) {
        for (OutputLogEvent event : logResult.getEvents()) {
            info(event.getMessage());
        }
    }

    private Pair<Boolean, String> getStepStatus(AmazonElasticMapReduce emr, String clusterId, String stepId) {
        ListStepsResult steps = emr
                .listSteps(new ListStepsRequest().withClusterId(clusterId).withStepIds(Arrays.asList(stepId)));
        if (steps.getSteps().size() == 1) {
            StepSummary step = steps.getSteps().get(0);
            String stepStatus = step.getStatus().getState();
            ArrayList<String> expectedStatuses = new ArrayList<>(Arrays.asList("COMPLETED", "CANCELLED", "FAILED"));
            return new Pair<>(expectedStatuses.contains(stepStatus), stepStatus);
        } else {
            error("Failed to find step with ID: " + stepId);
            return new Pair<>(true, "FAILED");
        }
    }

    private String retrieveScriptArguments(String command) {
        if (command.contains(" ")) {
            String[] parts = command.split(" ");
            String[] scriptArgsArray = Arrays.copyOfRange(parts, 1, parts.length);
            return String.join(" ", scriptArgsArray);
        } else {
            return "";
        }
    }

    private ArrayList<String> retrieveScript(String command) {
        if (command.contains(" ")) {
            return new ArrayList<String>(Arrays.asList(command.split(" ")[0]));
        } else {
            return new ArrayList<String>(Arrays.asList(command));
        }
    }

    protected static String getClusterId(AmazonElasticMapReduce emr, AWSLambda client, Props sysProps, Props jobProps,
            AtomicBoolean killed, Logger log) {
        boolean invokedLambda = false;
        int pollTime = sysProps.getInt(BOOT_POLL_INTERVAL, BOOT_POLL_INTERVAL_DEFAULT);
        int maxAttempts = sysProps.getInt(BOOT_POLL_ATTEMPTS_MAX, BOOT_POLL_ATTEMPTS_MAX_DEFAULT);
        String clusterName = jobProps
                                 .getString(AWS_EMR_CLUSTER_NAME, sysProps.getString(AWS_EMR_CLUSTER_NAME));
        boolean copyConfig = jobProps.getBoolean(AWS_EMR_COPY_SECCONFIG,
                sysProps.getBoolean(AWS_EMR_COPY_SECCONFIG, false));

        String clusterConfig = jobProps.getString(AWS_EMR_CLUSTER_CONFIG, null);
        int initialWait = jobProps.getInt(EMR_INITIAL_WAIT, 120_000);
        log.info("Looking for cluster named '" + clusterName + "'.");
        long randomWait = new Random().nextInt(initialWait);

        Optional<String> clusterId = EmrUtility.activeClusterId(emr, clusterName);

        if (!killed.get() && !clusterId.isPresent()) {
            try {
                log.info("No cluster named '" + clusterName + "', pausing for " + randomWait + " ms.");
                Thread.sleep(randomWait);
            } catch (InterruptedException e) {
                log.warn("Sleep interrupted");
            }
        }

        while (!killed.get() && !clusterId.isPresent() && maxAttempts > 0) {
            clusterId = EmrUtility.activeClusterId(emr, clusterName);
            if (!killed.get() && !clusterId.isPresent() && !invokedLambda) {

                log.info("No active cluster named: '" + clusterName + "', starting one up.");
                EMRConfiguration batchConfig = EMRConfiguration.builder().withName(clusterName)
                                                               .withCopySecurityConfiguration(copyConfig)
                                                               .withS3Overrides(clusterConfig).build();

                String payload = "{}";

                try {
                    payload = new ObjectMapper().writeValueAsString(batchConfig);
                } catch (Exception e) {
                    log.error(e.getMessage());
                    throw new IllegalStateException(e);
                }

                InvokeRequest req = new InvokeRequest().withFunctionName("aws_analytical_env_emr_launcher")
                                                       .withPayload(payload);
                InvokeResult result = client.invoke(req);
                invokedLambda = true;
                log.info("Lambda invocation status code: '" + result.getStatusCode() + "'");
                ByteBuffer resultPayload = result.getPayload();
                if (resultPayload != null) {
                    if (resultPayload.hasArray()) {
                        String response = new String(resultPayload.array(), StandardCharsets.UTF_8);
                        log.info("Lambda invocation response from result payload: '" + response + "'");
                    } else {
                        log.info("resultPayload payload has no backing array");
                    }

                    ByteBuffer readOnlyBuffer = resultPayload.asReadOnlyBuffer();
                    if (readOnlyBuffer != null && readOnlyBuffer.hasArray()) {
                        String response = new String(readOnlyBuffer.array(), StandardCharsets.UTF_8);
                        log.info("Lambda invocation response: '" + response + "'");
                    } else {
                        log.info("readOnlyBuffer payload has no backing array");
                    }
                } else {
                    log.info("Result payload is null");
                }

                if (result.getStatusCode() != 200) {
                    log.error(result.getFunctionError());
                    throw new IllegalStateException(result.getFunctionError());
                }
            }

            if (!clusterId.isPresent()) {
                maxAttempts--;
                try {
                    log.info("Waiting " + pollTime  + "ms for presence of clusterId");
                    Thread.sleep(pollTime);
                } catch (Exception e) {
                    log.warn("Sleep interrupted");
                }
            }
        }

        if (!killed.get() && !clusterId.isPresent()) {
            throw new IllegalStateException("No batch EMR cluster available");
        }

        return clusterId.orElse("");
    }

    /**
     * Checks to see if user has write access to current working directory which many users need for
     * their jobs to store temporary data/jars on the executor.
     * <p>
     * Accomplishes this by using execute-as-user to try to create an empty file in the cwd.
     *
     * @param effectiveUser user/proxy user running the job
     * @return true if user has write permissions in current working directory otherwise false
     */
    private boolean canWriteInCurrentWorkingDirectory(final String effectiveUser) throws IOException {
        final ExecuteAsUser executeAsUser = new ExecuteAsUser(
                this.getSysProps().getString(AZKABAN_SERVER_NATIVE_LIB_FOLDER));
        final List<String> checkIfUserCanWriteCommand = Arrays
                .asList(CREATE_FILE, getWorkingDirectory() + "/" + TEMP_FILE_NAME);
        final int result = executeAsUser.execute(effectiveUser, checkIfUserCanWriteCommand);
        return result == SUCCESSFUL_EXECUTION;
    }

    /**
     * Changes permissions on file/directory so that the file/directory is owned by the user and the
     * group remains the azkaban service account name.
     * <p>
     * Leverages execute-as-user with "root" as the user to run the command.
     *
     * @param effectiveUser user/proxy user running the job
     * @param fileName      the name of the file whose permissions will be changed
     */
    private void assignUserFileOwnership(final String effectiveUser, final String fileName) throws Exception {
        final ExecuteAsUser executeAsUser = new ExecuteAsUser(
                this.getSysProps().getString(AZKABAN_SERVER_NATIVE_LIB_FOLDER));
        final String groupName = this.getSysProps().getString(AZKABAN_SERVER_GROUP_NAME, "azkaban");
        final List<String> changeOwnershipCommand = Arrays.asList(CHOWN, effectiveUser + ":" + groupName, fileName);
        info("Change ownership of " + fileName + " to " + effectiveUser + ":" + groupName + ", command '"
                + changeOwnershipCommand + "'.");
        final int result = executeAsUser.execute("root", changeOwnershipCommand);
        if (result != 0) {
            handleError("Failed to change current working directory ownership. Error code: " + Integer.toString(result),
                    null);
        }
    }

    /**
     * This is used to get the min/max memory size requirement by processes. SystemMemoryInfo can use
     * the info to determine if the memory request can be fulfilled. For Java process, this should be
     * Xms/Xmx setting.
     *
     * @return pair of min/max memory size
     */
    protected Pair<Long, Long> getProcMemoryRequirement() throws Exception {
        return new Pair<>(0L, 0L);
    }

    protected void handleError(final String errorMsg, final Exception e) throws Exception {
        error(errorMsg);
        if (e != null) {
            throw new Exception(errorMsg, e);
        } else {
            throw new Exception(errorMsg);
        }
    }

    protected List<String> getCommandList() {
        final List<String> commands = new ArrayList<>();
        commands.add(this.getJobProps().getString(COMMAND));
        for (int i = 1; this.getJobProps().containsKey(COMMAND + "." + i); i++) {
            commands.add(this.getJobProps().getString(COMMAND + "." + i));
        }

        return commands;
    }

    private String getUserGroup(String user) {
        String groupName = null;
        FileReader fileReader = null;
        BufferedReader groupsReader = null;
        try {
            fileReader = new FileReader("/etc/group");
            groupsReader = new BufferedReader(fileReader);
            while (groupsReader.ready()) {
                try {
                    String line = groupsReader.readLine();
                    String[] tokens = line.split(":");
                    if (tokens.length > 3) {
                        for (String uStr : tokens[3].split(",")) {
                            if (uStr.equals(user)) {
                                groupName = tokens[0];
                                break;
                            }
                        }
                    }
                } catch (Exception e) {
                    continue;
                }
            }
            groupsReader.close();
            fileReader.close();
        } catch (Exception e) {
            return groupName;
        }
        return groupName;
    }

    @Override
    public void cancel() {
        setJobToKilled();
        if (this.stepIds != null) {
            List<String> steps = this.stepIds.stream().filter(Objects::nonNull).collect(Collectors.toList());
            if (steps.size() > 0) {
                AmazonElasticMapReduce emr = emrClient();
                EmrUtility.activeClusterId(emr, clusterName()).ifPresent(clusterId -> {
                    info("Cancelling steps '" + steps + "' on clusterId: '" + clusterId + "'.");
                    CancelStepsResult result = emr.cancelSteps(getCancelStepsRequest(clusterId, steps));
                    result.getCancelStepsInfoList().forEach(cancelInfo -> info(
                            "Cancelled steps '" + steps + "', " + "cancellation status: '" + cancelInfo.getStatus()
                                    + "', " + "cancellation reason '" + cancelInfo.getReason() + "'."));
                });
            }
        }
    }

    private AmazonElasticMapReduce emrClient() {
        return AmazonElasticMapReduceClientBuilder.standard().withRegion(awsRegion()).build();
    }

    private String clusterName() {
        return this.getJobProps().getString(AWS_EMR_CLUSTER_NAME, this.getSysProps().getString(AWS_EMR_CLUSTER_NAME));
    }

    private String awsRegion() {
        return this.getSysProps().getString(AWS_REGION, "eu-west-2");
    }

    private CancelStepsRequest getCancelStepsRequest(String clusterId, Collection<String> stepIds) {
        CancelStepsRequest request = new CancelStepsRequest();
        request.setClusterId(clusterId);
        request.setStepIds(stepIds);
        return request;
    }

    private void setJobToKilled() {
        synchronized (this) {
            this.killed.set(true);
            this.notify();
        }
    }

    @Override
    public double getProgress() {
        return this.process != null && this.process.isComplete() ? 1.0 : 0.0;
    }

    public int getProcessId() {
        return this.process.getProcessId();
    }

    public String getPath() {
        return Utils.ifNull(this.getJobPath(), "");
    }

    private void configureCluster(AmazonElasticMapReduce emr, String clusterId) throws RuntimeException {
        DescribeClusterResult clusterDetails = emr
                .describeCluster(new DescribeClusterRequest().withClusterId(clusterId));
        if (clusterDetails.getCluster().getStepConcurrencyLevel() != MAX_STEPS) {
            // Post Startup Implementation details here.
            // We may still be initializing, so wait until we are waiting for steps.
            // Potentially maybe other executors listening, so check step concurrency also.
            while (!clusterDetails.getCluster().getStatus().getState().equals("WAITING")) {
                String currentState = clusterDetails.getCluster().getStatus().getState();
                info("Waiting for state WAITING...current state is " + currentState);

                if (currentState.contains("TERMINATED") || currentState.equals("TERMINATING")) {
                    info("Exiting with error as current state is " + currentState);
                    throw new RuntimeException("Cluster has been or is being terminated");
                }

                if (killed.get()) {
                    info("Stopping waiting for cluster configuration to complete due to job being killed");
                    return;
                }

                try {
                    Thread.sleep(POLL_INTERVAL);
                } catch (Exception e) {
                    warn("Sleep interrupted");
                }
                clusterDetails = emr.describeCluster(new DescribeClusterRequest().withClusterId(clusterId));
            }

            if (!killed.get() && clusterDetails.getCluster().getStepConcurrencyLevel() != MAX_STEPS) {
                ModifyClusterResult modifyResult = emr.modifyCluster(
                        new ModifyClusterRequest().withClusterId(clusterId).withStepConcurrencyLevel(MAX_STEPS));
                // Add additional setup here.
            }
        }
    }
}
