package framework;

import framework.model.*;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.RewriteDataFiles;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.actions.SparkActions;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public abstract class BaseExperiment {

    private static final Logger log = LoggerFactory.getLogger(BaseExperiment.class);
    private static final DateTimeFormatter ISO = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");

    protected SparkSession spark;
    private final SparkMetricsListener metricsListener = new SparkMetricsListener();
    private final MetricsCollector     metricsCollector = new MetricsCollector();
    private final TrinoQueryBenchmark  trinoBenchmark   = new TrinoQueryBenchmark();
    private final ReportWriter         reportWriter     = new ReportWriter("results");

    // ── Subclass contract ─────────────────────────────────────────────────────

    public abstract String experimentName();

    /** Iceberg rewrite options passed to {@link RewriteDataFiles}. */
    protected abstract Map<String, String> compactionOptions();

    /** Additional Spark config overrides (merged on top of base config). Default: empty. */
    protected Map<String, String> sparkOverrides() { return Map.of(); }

    /**
     * Execute compaction and return its result.
     * Default: calls RewriteDataFiles with {@link #compactionOptions()}.
     * Override for experiments needing custom rewrite logic (e.g. partition filter).
     */
    protected RewriteDataFiles.Result runCompaction(SparkSession spark, Table table) {
        RewriteDataFiles rewrite = SparkActions.get(spark).rewriteDataFiles(table);
        compactionOptions().forEach(rewrite::option);
        return rewrite.execute();
    }

    // ── Template method ───────────────────────────────────────────────────────

    public final void run() throws Exception {
        log.info("=== {} [{}] ===", experimentName(), LakehouseConfig.RUN_MODE);

        // 1. Build SparkSession
        spark = buildSpark();
        spark.sparkContext().addSparkListener(metricsListener);

        // 2. Drop + recreate table for clean isolation
        setupTable();
        Table table = loadTable();

        // 3. Generate small files
        new DataGenerator(spark).generate();
        table.refresh();

        // 4. Collect before metrics (reset Spark listener first)
        metricsListener.reset();
        SnapshotMetrics before = metricsCollector.collect(table, "before");

        // 5. Trino benchmark BEFORE (Spark is idle, not running a job)
        List<Long> trinoBeforeMedians = trinoBenchmark.runAll("before");

        // 6. Run compaction, measure wall-clock duration
        long t0 = System.currentTimeMillis();
        RewriteDataFiles.Result compactionResult = runCompaction(spark, table);
        long durationMs = System.currentTimeMillis() - t0;
        table.refresh();

        // 7. Collect after metrics + flush Spark resource snapshot
        SnapshotMetrics after = metricsCollector.collect(table, "after");
        SparkResourceMetrics sparkResources = metricsListener.flush(
            LakehouseConfig.baseSparkConfig(), compactionOptions(), durationMs);

        // 8. Optional pause for Spark UI inspection (before closing session)
        if (LakehouseConfig.PAUSE_FOR_UI) {
            log.info(">>> Spark UI: http://localhost:4040 — press Enter to close session...");
            System.in.read();
        }

        // 9. Close SparkSession, then run Trino benchmark AFTER (no CPU competition)
        spark.close();
        List<Long> trinoAfterMedians = trinoBenchmark.runAll("after");
        List<QueryBenchmarkResult> queryBenchmark =
            trinoBenchmark.pair(trinoBeforeMedians, trinoAfterMedians);

        // 10. Build result and write report
        CompactionPerformance compactionPerf =
            buildCompactionPerf(compactionResult, before, durationMs);

        ExperimentResult result = new ExperimentResult(
            experimentName(),
            LocalDateTime.now().format(ISO),
            LakehouseConfig.RUN_MODE.name(),
            compactionOptions(),
            LakehouseConfig.baseSparkConfig(),
            before, after,
            compactionPerf, sparkResources,
            queryBenchmark,
            null);

        ReportWriter.OutputPaths paths = reportWriter.write(result);
        printSummary(result, paths);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    protected SparkSession buildSpark() {
        SparkSession.Builder builder = SparkSession.builder()
            .appName(experimentName());
        Map<String, String> cfg = new LinkedHashMap<>(LakehouseConfig.baseSparkConfig());
        cfg.putAll(sparkOverrides());
        cfg.forEach(builder::config);
        return builder.getOrCreate();
    }

    private void setupTable() {
        spark.sql("CREATE DATABASE IF NOT EXISTS "
            + LakehouseConfig.CATALOG + "." + LakehouseConfig.DATABASE);
        spark.sql("DROP TABLE IF EXISTS " + LakehouseConfig.FULL_TABLE);
        spark.sql("CREATE TABLE " + LakehouseConfig.FULL_TABLE + " (\n"
            + "  event_id   STRING    NOT NULL,\n"
            + "  user_id    STRING    NOT NULL,\n"
            + "  event_type STRING    NOT NULL,\n"
            + "  payload    STRING,\n"
            + "  region     STRING    NOT NULL,\n"
            + "  ts         TIMESTAMP NOT NULL\n"
            + ") USING iceberg\n"
            + "PARTITIONED BY (region, days(ts))\n"
            + "TBLPROPERTIES (\n"
            + "  'write.format.default' = 'parquet',\n"
            + "  'commit.retry.num-retries' = '5'\n"
            + ")");
        log.info("Table {} created", LakehouseConfig.FULL_TABLE);
    }

    private Table loadTable() throws Exception {
        return Spark3Util.loadIcebergTable(spark, LakehouseConfig.FULL_TABLE);
    }

    private CompactionPerformance buildCompactionPerf(RewriteDataFiles.Result result,
                                                      SnapshotMetrics before,
                                                      long durationMs) {
        int filesRewritten  = result.rewrittenDataFilesCount();
        int filesAdded      = result.addedDataFilesCount();
        // resultMap() is not in the RewriteDataFiles.Result interface in Iceberg 1.6;
        // use filesAdded as a proxy for file groups (each group produces ≥1 output file)
        int fileGroups      = filesAdded;
        // Throughput = bytes read (input files) per second
        double throughput = (durationMs > 0 && before.totalSizeBytes > 0)
            ? ((double) before.totalSizeBytes / 1_048_576.0) / (durationMs / 1000.0)
            : 0.0;
        return new CompactionPerformance(durationMs, filesRewritten, filesAdded, fileGroups, throughput);
    }

    private void printSummary(ExperimentResult r, ReportWriter.OutputPaths paths) {
        System.out.println();
        System.out.println("╔══════════════════════════════════════════════════╗");
        System.out.printf( "║  %-48s║%n", r.experimentName);
        System.out.println("╠══════════════════════════════════════════════════╣");
        System.out.printf( "║  Files:     %d → %d  (-%d)%n",
            r.beforeFileMetrics.fileCount, r.afterFileMetrics.fileCount,
            r.beforeFileMetrics.fileCount - r.afterFileMetrics.fileCount);
        System.out.printf( "║  Avg size:  %s → %s%n",
            humanSize(r.beforeFileMetrics.avgFileSizeBytes),
            humanSize(r.afterFileMetrics.avgFileSizeBytes));
        System.out.printf( "║  Snapshots: %d → %d%n",
            r.beforeFileMetrics.snapshotCount, r.afterFileMetrics.snapshotCount);
        System.out.printf( "║  Duration:  %dms   Throughput: %.1f MB/s%n",
            r.compactionPerf.durationMs, r.compactionPerf.throughputMBps);
        System.out.printf( "║  GC:        %.1f%%   Parallelism eff: %.2f%n",
            r.sparkResources.gcPressureRatio * 100, r.sparkResources.parallelismEfficiency);
        System.out.println("╠══════════════════════════════════════════════════╣");
        System.out.println( "║  JSON: " + paths.jsonPath());
        System.out.println( "║  HTML: " + paths.htmlPath());
        System.out.println("╚══════════════════════════════════════════════════╝");
        System.out.println();
    }

    private String humanSize(long bytes) {
        if (bytes >= 1_048_576) return String.format("%.1f MB", bytes / 1_048_576.0);
        if (bytes >= 1_024)     return String.format("%.1f KB", bytes / 1_024.0);
        return bytes + " B";
    }
}
