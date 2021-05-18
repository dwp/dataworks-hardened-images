package uk.gov.dwp.dataworks.azkaban.app;

import azkaban.utils.Props;
import org.apache.log4j.Logger;
import uk.gov.dwp.dataworks.azkaban.jobtype.EmrLauncherJob;

import java.text.SimpleDateFormat;
import java.util.Date;

import static uk.gov.dwp.dataworks.azkaban.jobtype.EmrLauncherJob.*;

public class App {
    public static void main(String[] args) {
        Props jobProperties = Props.of(
                JOB_DEPENDENCIES_PARAMETER_NAME, "ADG-incremental",
                EXPORT_DATE_PARAMETER_NAME, "2021-05-12",
                EMR_LAUNCHER_LAMBDA_PARAMETER_NAME, "aws_clive_emr_launcher");

        Props systemProperties = new Props();
        Logger jobLogger = Logger.getLogger(EmrLauncherJob.class);
        String jobId = "job-id";
        EmrLauncherJob emrLauncher = new EmrLauncherJob(jobId, systemProperties, jobProperties, jobLogger);
        emrLauncher.run();
    }
}
