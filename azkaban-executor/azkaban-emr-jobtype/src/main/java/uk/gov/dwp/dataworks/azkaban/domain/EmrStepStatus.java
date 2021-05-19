package uk.gov.dwp.dataworks.azkaban.domain;

public enum EmrStepStatus {
    PENDING(true), CANCEL_PENDING(true), RUNNING(true), COMPLETED(false), CANCELLED(false), FAILED(false), INTERRUPTED(
            false);

    private final boolean active;

    EmrStepStatus(boolean active) {
        this.active = active;
    }

    public boolean isActive() {
        return active;
    }
}
