package framework;

import framework.model.SparkResourceMetrics;
import org.apache.spark.scheduler.*;
import org.apache.spark.executor.TaskMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class SparkMetricsListener extends SparkListener {

    private static final Logger log = LoggerFactory.getLogger(SparkMetricsListener.class);

    private final AtomicLong totalCpuTimeNs = new AtomicLong(0);
    private final AtomicLong totalGcTimeMs  = new AtomicLong(0);
    private final AtomicLong taskCount      = new AtomicLong(0);
    private final AtomicLong bytesRead      = new AtomicLong(0);
    private final AtomicLong bytesWritten   = new AtomicLong(0);

    /** Zero all counters before starting the compaction job. */
    public void reset() {
        totalCpuTimeNs.set(0);
        totalGcTimeMs.set(0);
        taskCount.set(0);
        bytesRead.set(0);
        bytesWritten.set(0);
        log.debug("SparkMetricsListener reset");
    }

    @Override
    public void onTaskEnd(SparkListenerTaskEnd taskEnd) {
        // "Success$" is the JVM class name of Spark's Success case object
        if (!"Success$".equals(taskEnd.reason().getClass().getSimpleName())) return;

        TaskMetrics m = taskEnd.taskMetrics();
        totalCpuTimeNs.addAndGet(m.executorCpuTime());
        totalGcTimeMs.addAndGet(m.jvmGCTime());
        taskCount.incrementAndGet();
        bytesRead.addAndGet(m.inputMetrics().bytesRead());
        bytesWritten.addAndGet(m.outputMetrics().bytesWritten());
    }

    /**
     * Snapshot collected metrics into a {@link SparkResourceMetrics} record.
     * Call after compaction completes (before or after session close).
     *
     * @param sparkConfig    the Spark config map used for this experiment
     * @param icebergOptions the Iceberg rewrite options used
     * @param durationMs     wall-clock duration of the compaction call
     */
    public SparkResourceMetrics flush(Map<String, String> sparkConfig,
                                      Map<String, String> icebergOptions,
                                      long durationMs) {
        long cpuMs  = totalCpuTimeNs.get() / 1_000_000;
        long gcMs   = totalGcTimeMs.get();
        long tasks  = taskCount.get();
        long br     = bytesRead.get();
        long bw     = bytesWritten.get();
        int  cores  = LakehouseConfig.cores();

        double gcPressure     = cpuMs > 0 ? (double) gcMs / cpuMs : 0.0;
        double parallelismEff = (durationMs > 0 && cores > 0)
            ? (double) tasks / (cores * (durationMs / 1000.0)) : 0.0;
        double throughput     = (durationMs > 0 && bw > 0)
            ? ((double) bw / (1024.0 * 1024.0)) / (durationMs / 1000.0) : 0.0;

        log.info("SparkMetrics: tasks={} cpuMs={} gcMs={} gcPressure={:.3f} parallelismEff={:.3f}",
            tasks, cpuMs, gcMs, gcPressure, parallelismEff);

        return new SparkResourceMetrics(sparkConfig, icebergOptions,
            tasks, cpuMs, gcMs, br, bw,
            gcPressure, parallelismEff, throughput);
    }
}
