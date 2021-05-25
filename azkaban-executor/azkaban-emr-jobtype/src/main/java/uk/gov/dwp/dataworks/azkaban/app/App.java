package uk.gov.dwp.dataworks.azkaban.app;

import azkaban.utils.Props;
import org.apache.log4j.Logger;
import uk.gov.dwp.dataworks.azkaban.jobtype.EmrLauncherJob;

import static uk.gov.dwp.dataworks.azkaban.jobtype.EmrLauncherJob.AWS_LOG_GROUP_PARAMETER_NAME;
import static uk.gov.dwp.dataworks.azkaban.jobtype.EmrLauncherJob.CLUSTER_PARAMETER_NAME;
import static uk.gov.dwp.dataworks.azkaban.jobtype.EmrLauncherJob.COLLECTION_DEPENDENCIES_PARAMETER_NAME;
import static uk.gov.dwp.dataworks.azkaban.jobtype.EmrLauncherJob.EMR_LAUNCHER_LAMBDA_PARAMETER_NAME;
import static uk.gov.dwp.dataworks.azkaban.jobtype.EmrLauncherJob.EXPORT_DATE_PARAMETER_NAME;
import static uk.gov.dwp.dataworks.azkaban.jobtype.EmrLauncherJob.JOB_DEPENDENCIES_PARAMETER_NAME;

public class App {
    public static void main(String[] args) throws Exception {

//        boolean wtf = collectionStatusService.collectionsSucceeded(
//                "analytical_dataset_generation_analytical_dataset_generation_dev_1004",
//                "db.core.toDo", "db.core.contract", "db.core.statement", "db.core.claimantCommitment", "db.core.claimant", "poo");

        Props jobProperties = Props
                .of(JOB_DEPENDENCIES_PARAMETER_NAME, "ADG-incremental", EXPORT_DATE_PARAMETER_NAME, "2021-05-12",
                        EMR_LAUNCHER_LAMBDA_PARAMETER_NAME, "aws_clive_emr_launcher", AWS_LOG_GROUP_PARAMETER_NAME,
                        "/app/aws_clive/step_logs", CLUSTER_PARAMETER_NAME, "aws-clive"/*,
                        COLLECTION_DEPENDENCIES_PARAMETER_NAME, "db.core.toDo,db.core.contract,db.core.statement"*/);
        Props systemProperties = new Props();
        Logger jobLogger = Logger.getLogger(EmrLauncherJob.class);
        String jobId = "job-id";
        EmrLauncherJob emrLauncher = new EmrLauncherJob(jobId, systemProperties, jobProperties, jobLogger);
        emrLauncher.run();
        System.out.println("EmrLauncherJob has completed.");
    }
}
