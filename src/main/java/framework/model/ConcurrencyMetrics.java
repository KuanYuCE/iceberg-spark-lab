package framework.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public class ConcurrencyMetrics {
    public final int ingestionCommitsAttempted;
    public final int ingestionCommitsSucceeded;
    public final int ingestionCommitsFailed;
    public final int rewriteCommitRetries;
    public final int rewriteCommitConflicts;
    public final int fileGroupsSkipped;
    public final long totalSnapshotsCreated;
    public final List<SnapshotEvent> snapshotLineage;

    @JsonCreator
    public ConcurrencyMetrics(
            @JsonProperty("ingestionCommitsAttempted") int ingestionCommitsAttempted,
            @JsonProperty("ingestionCommitsSucceeded") int ingestionCommitsSucceeded,
            @JsonProperty("ingestionCommitsFailed")    int ingestionCommitsFailed,
            @JsonProperty("rewriteCommitRetries")      int rewriteCommitRetries,
            @JsonProperty("rewriteCommitConflicts")    int rewriteCommitConflicts,
            @JsonProperty("fileGroupsSkipped")         int fileGroupsSkipped,
            @JsonProperty("totalSnapshotsCreated")     long totalSnapshotsCreated,
            @JsonProperty("snapshotLineage")           List<SnapshotEvent> snapshotLineage) {
        this.ingestionCommitsAttempted = ingestionCommitsAttempted;
        this.ingestionCommitsSucceeded = ingestionCommitsSucceeded;
        this.ingestionCommitsFailed = ingestionCommitsFailed;
        this.rewriteCommitRetries = rewriteCommitRetries;
        this.rewriteCommitConflicts = rewriteCommitConflicts;
        this.fileGroupsSkipped = fileGroupsSkipped;
        this.totalSnapshotsCreated = totalSnapshotsCreated;
        this.snapshotLineage = snapshotLineage;
    }
}
