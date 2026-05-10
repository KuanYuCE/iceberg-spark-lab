package framework.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class SnapshotEvent {
    public final long snapshotId;
    public final Long parentSnapshotId;  // null for the root snapshot
    public final String operation;
    public final int addedDataFiles;
    public final int deletedDataFiles;
    public final long timestampMs;

    @JsonCreator
    public SnapshotEvent(
            @JsonProperty("snapshotId")       long snapshotId,
            @JsonProperty("parentSnapshotId") Long parentSnapshotId,
            @JsonProperty("operation")        String operation,
            @JsonProperty("addedDataFiles")   int addedDataFiles,
            @JsonProperty("deletedDataFiles") int deletedDataFiles,
            @JsonProperty("timestampMs")      long timestampMs) {
        this.snapshotId = snapshotId;
        this.parentSnapshotId = parentSnapshotId;
        this.operation = operation;
        this.addedDataFiles = addedDataFiles;
        this.deletedDataFiles = deletedDataFiles;
        this.timestampMs = timestampMs;
    }
}
