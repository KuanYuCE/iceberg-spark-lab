package framework.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;

public class SparkResourceMetrics {
    public final Map<String, String> sparkConfig;
    public final Map<String, String> icebergOptions;
    public final long taskCount;
    public final long totalCpuTimeMs;
    public final long totalGcTimeMs;
    public final long bytesRead;
    public final long bytesWritten;
    public final double gcPressureRatio;
    public final double parallelismEfficiency;
    public final double rewriteThroughputMBps;

    @JsonCreator
    public SparkResourceMetrics(
            @JsonProperty("sparkConfig")            Map<String, String> sparkConfig,
            @JsonProperty("icebergOptions")         Map<String, String> icebergOptions,
            @JsonProperty("taskCount")              long taskCount,
            @JsonProperty("totalCpuTimeMs")         long totalCpuTimeMs,
            @JsonProperty("totalGcTimeMs")          long totalGcTimeMs,
            @JsonProperty("bytesRead")              long bytesRead,
            @JsonProperty("bytesWritten")           long bytesWritten,
            @JsonProperty("gcPressureRatio")        double gcPressureRatio,
            @JsonProperty("parallelismEfficiency")  double parallelismEfficiency,
            @JsonProperty("rewriteThroughputMBps")  double rewriteThroughputMBps) {
        this.sparkConfig = sparkConfig;
        this.icebergOptions = icebergOptions;
        this.taskCount = taskCount;
        this.totalCpuTimeMs = totalCpuTimeMs;
        this.totalGcTimeMs = totalGcTimeMs;
        this.bytesRead = bytesRead;
        this.bytesWritten = bytesWritten;
        this.gcPressureRatio = gcPressureRatio;
        this.parallelismEfficiency = parallelismEfficiency;
        this.rewriteThroughputMBps = rewriteThroughputMBps;
    }
}
