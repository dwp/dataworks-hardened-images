package uk.gov.dwp.dataworks.azkaban.model;

public enum EmrClusterStatus {
    BOOTSTRAPPING(true, false), RUNNING(true, false), STARTING(true, false), TERMINATED(false,
            true), TERMINATED_WITH_ERRORS(false, true), TERMINATING(true, true), WAITING(true, true);

    final boolean active;
    final boolean completed;

    EmrClusterStatus(boolean active, boolean completed) {
        this.active = active;
        this.completed = completed;
    }

    public boolean isActive() {
        return active;
    }

    public boolean hasCompleted() {
        return completed;
    }
}
