package framework.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class ExperimentResult {
    public final String experimentName;
    public final String timestamp;
    public final String runMode;
    public final Map<String, String> compactionOptions;
    public final Map<String, String> sparkConfig;
    public final SnapshotMetrics beforeFileMetrics;
    public final SnapshotMetrics afterFileMetrics;
    public final CompactionPerformance compactionPerf;
    public final SparkResourceMetrics sparkResources;
    public final List<QueryBenchmarkResult> queryBenchmark;
    public final ConcurrencyMetrics concurrencyMetrics;  // null for Exp01–06

    @JsonCreator
    public ExperimentResult(
            @JsonProperty("experimentName")    String experimentName,
            @JsonProperty("timestamp")         String timestamp,
            @JsonProperty("runMode")           String runMode,
            @JsonProperty("compactionOptions") Map<String, String> compactionOptions,
            @JsonProperty("sparkConfig")       Map<String, String> sparkConfig,
            @JsonProperty("beforeFileMetrics") SnapshotMetrics beforeFileMetrics,
            @JsonProperty("afterFileMetrics")  SnapshotMetrics afterFileMetrics,
            @JsonProperty("compactionPerf")    CompactionPerformance compactionPerf,
            @JsonProperty("sparkResources")    SparkResourceMetrics sparkResources,
            @JsonProperty("queryBenchmark")    List<QueryBenchmarkResult> queryBenchmark,
            @JsonProperty("concurrencyMetrics") ConcurrencyMetrics concurrencyMetrics) {
        this.experimentName = experimentName;
        this.timestamp = timestamp;
        this.runMode = runMode;
        this.compactionOptions = compactionOptions;
        this.sparkConfig = sparkConfig;
        this.beforeFileMetrics = beforeFileMetrics;
        this.afterFileMetrics = afterFileMetrics;
        this.compactionPerf = compactionPerf;
        this.sparkResources = sparkResources;
        this.queryBenchmark = queryBenchmark;
        this.concurrencyMetrics = concurrencyMetrics;
    }
}
