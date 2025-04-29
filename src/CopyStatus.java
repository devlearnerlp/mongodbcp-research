public class CopyStatus {
    private String collection;
    private long totalDocs;
    private long copiedDocs;
    private String status; // "IN_PROGRESS", "PAUSED", "SUCCESS", "FAILED"
    private String errorMessage;
    private boolean pauseRequested = false;
}

