package org.digga.bidb;

public class UpdateResult<ID> {

    private ID documentId;
    private UpdateStatus updateStatus;
    private String message;

    public UpdateResult(UpdateStatus updateStatus, ID documentId) {
        this.documentId = documentId;
        this.updateStatus = updateStatus;
    }

    public UpdateResult(UpdateStatus updateStatus, ID documentId, String message) {
        this.documentId = documentId;
        this.updateStatus = updateStatus;
        this.message = message;
    }

    public ID getDocumentId() {
        return documentId;
    }

    public UpdateStatus getUpdateStatus() {
        return updateStatus;
    }

    public String getMessage() {
        return message;
    }

    public enum UpdateStatus {
        INSERTED, UPDATED, DELETED, ERROR
    }

    @Override
    public String toString() {
        return "UpdateResult [" +
                documentId + "] " +
                "updateStatus=" + updateStatus +
                ", message='" + message + '\'' +
                '}';
    }
}
