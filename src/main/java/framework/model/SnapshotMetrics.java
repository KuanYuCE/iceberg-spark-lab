package framework.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;

public class SnapshotMetrics {
    public final String label;
    public final long fileCount;
    public final long totalSizeBytes;
    public final long avgFileSizeBytes;
    public final long minFileSizeBytes;
    public final long maxFileSizeBytes;
    public final long snapshotCount;
    public final long manifestCount;
    public final Map<String, Long> perPartitionFileCounts;

    @JsonCreator
    public SnapshotMetrics(
            @JsonProperty("label")                  String label,
            @JsonProperty("fileCount")              long fileCount,
            @JsonProperty("totalSizeBytes")         long totalSizeBytes,
            @JsonProperty("avgFileSizeBytes")       long avgFileSizeBytes,
            @JsonProperty("minFileSizeBytes")       long minFileSizeBytes,
            @JsonProperty("maxFileSizeBytes")       long maxFileSizeBytes,
            @JsonProperty("snapshotCount")          long snapshotCount,
            @JsonProperty("manifestCount")          long manifestCount,
            @JsonProperty("perPartitionFileCounts") Map<String, Long> perPartitionFileCounts) {
        this.label = label;
        this.fileCount = fileCount;
        this.totalSizeBytes = totalSizeBytes;
        this.avgFileSizeBytes = avgFileSizeBytes;
        this.minFileSizeBytes = minFileSizeBytes;
        this.maxFileSizeBytes = maxFileSizeBytes;
        this.snapshotCount = snapshotCount;
        this.manifestCount = manifestCount;
        this.perPartitionFileCounts = perPartitionFileCounts;
    }
}
