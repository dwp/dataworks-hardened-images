package uk.gov.dwp.dataworks.azkaban.services;

import uk.gov.dwp.dataworks.azkaban.jobtype.EmrLauncherJob;

import java.util.concurrent.atomic.AtomicBoolean;

public class CancellableLoggingService {

    public void info(String message) {
        if (this.parent != null) {
            this.parent.info(message);
        }
    }

    public void warn(String message) {
        if (this.parent != null) {
            this.parent.warn(message);
        }
    }

    public void error(String message) {
        if (this.parent != null) {
            this.parent.error(message);
        }
    }

    public void error(String message, Throwable t) {
        if (this.parent != null) {
            this.parent.error(message, t);
        }
    }

    public void cancel() {
        warn("Operation has been cancelled");
        proceed.set(false);
    }

    public void setParent(EmrLauncherJob parent) {
        this.parent = parent;
    }

    final AtomicBoolean proceed = new AtomicBoolean(true);
    private EmrLauncherJob parent;
}
