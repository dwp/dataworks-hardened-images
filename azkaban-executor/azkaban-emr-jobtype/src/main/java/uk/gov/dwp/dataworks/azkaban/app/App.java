package uk.gov.dwp.dataworks.azkaban.app;

import azkaban.utils.Props;
import org.apache.log4j.Logger;
import uk.gov.dwp.dataworks.azkaban.jobtype.EmrLauncherJob;

import java.text.SimpleDateFormat;
import java.util.Date;

import static uk.gov.dwp.dataworks.azkaban.jobtype.EmrLauncherJob.EXPORT_DATE_PARAMETER_NAME;
import static uk.gov.dwp.dataworks.azkaban.jobtype.EmrLauncherJob.JOB_DEPENDENCIES_PARAMETER_NAME;

public class App {
    public static void main(String[] args) {
        Props jobProperties = Props.of(
                JOB_DEPENDENCIES_PARAMETER_NAME, "ADG-incremental",
                EXPORT_DATE_PARAMETER_NAME, "2021-02-12");
        Props systemProperties = new Props();
        Logger jobLogger = Logger.getLogger(EmrLauncherJob.class);
        String jobId = "job-id";
        EmrLauncherJob emrLauncher = new EmrLauncherJob(jobId, systemProperties, jobProperties, jobLogger);
        emrLauncher.run();
    }
}
