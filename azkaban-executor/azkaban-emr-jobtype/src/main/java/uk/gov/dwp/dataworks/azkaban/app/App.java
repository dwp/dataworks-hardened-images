package uk.gov.dwp.dataworks.azkaban.app;

import azkaban.utils.Props;
import org.apache.log4j.Logger;
import uk.gov.dwp.dataworks.azkaban.jobtype.EmrLauncherJob;

public class App {
    public static void main(String[] args) {
        Props jobProperties = new Props();
        Props systemProperties = new Props();
        Logger jobLogger = Logger.getLogger(EmrLauncherJob.class);
        String jobId = "job-id";
        EmrLauncherJob emrLauncher = new EmrLauncherJob(jobId, systemProperties, jobProperties, jobLogger);
        emrLauncher.run();
    }
}
