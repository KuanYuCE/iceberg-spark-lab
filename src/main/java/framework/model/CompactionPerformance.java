package framework.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class CompactionPerformance {
    public final long durationMs;
    public final int filesRewritten;
    public final int filesAdded;
    public final int fileGroupsRewritten;
    public final double throughputMBps;

    @JsonCreator
    public CompactionPerformance(
            @JsonProperty("durationMs")         long durationMs,
            @JsonProperty("filesRewritten")     int filesRewritten,
            @JsonProperty("filesAdded")         int filesAdded,
            @JsonProperty("fileGroupsRewritten") int fileGroupsRewritten,
            @JsonProperty("throughputMBps")     double throughputMBps) {
        this.durationMs = durationMs;
        this.filesRewritten = filesRewritten;
        this.filesAdded = filesAdded;
        this.fileGroupsRewritten = fileGroupsRewritten;
        this.throughputMBps = throughputMBps;
    }
}
