package experiments;

import com.fasterxml.jackson.databind.ObjectMapper;
import framework.LakehouseConfig;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.RewriteDataFiles;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.actions.SparkActions;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Process B for Exp07.
 *
 * Waits for results/exp07-ingestion-ready.flag, then runs RewriteDataFiles
 * while IngestionWorker continues appending. After compaction finishes,
 * writes results/exp07-stop.flag and results/exp07-compaction-metrics.json.
 *
 * -DpartialProgress=true enables partial-progress mode (default false).
 */
public class Exp07b_CompactionWorker {

    private static final Logger log = LoggerFactory.getLogger(Exp07b_CompactionWorker.class);
    private static final Path READY_FLAG = Path.of("results/exp07-ingestion-ready.flag");
    private static final Path STOP_FLAG  = Path.of("results/exp07-stop.flag");

    private static final AtomicInteger retryCount    = new AtomicInteger(0);
    private static final AtomicInteger conflictCount = new AtomicInteger(0);

    public static void main(String[] args) throws Exception {
        boolean partialProgress = Boolean.parseBoolean(
            System.getProperty("partialProgress", "false"));
        int maxCommits = Integer.parseInt(System.getProperty("maxCommits", "5"));
        log.info("Exp07b CompactionWorker — partialProgress={} maxCommits={}", partialProgress, maxCommits);

        log.info("Waiting for ingestion ready signal...");
        while (!Files.exists(READY_FLAG)) {
            Thread.sleep(500);
        }
        log.info("Ingestion ready. Starting compaction.");

        SparkSession.Builder builder = SparkSession.builder()
            .appName("Exp07b_CompactionWorker")
            .config("spark.ui.port", "4041");
        LakehouseConfig.baseSparkConfig().forEach(builder::config);
        SparkSession spark = builder.getOrCreate();

        Table table = Spark3Util.loadIcebergTable(spark, LakehouseConfig.FULL_TABLE);

        Map<String, String> options = new LinkedHashMap<>();
        options.put("target-file-size-bytes",
            String.valueOf(LakehouseConfig.targetFileSizes()[1]));
        options.put("min-input-files", "2");
        options.put("max-concurrent-file-group-rewrites", "2");
        if (partialProgress) {
            options.put("partial-progress.enabled",     "true");
            options.put("partial-progress.max-commits", String.valueOf(maxCommits));
        }

        long t0 = System.currentTimeMillis();
        RewriteDataFiles.Result result = null;
        try {
            RewriteDataFiles rewrite = SparkActions.get(spark).rewriteDataFiles(table);
            options.forEach(rewrite::option);
            result = rewrite.execute();
        } catch (Exception e) {
            log.error("Compaction failed: {}", e.getMessage());
            conflictCount.incrementAndGet();
        }
        long durationMs = System.currentTimeMillis() - t0;

        spark.close();
        Files.writeString(STOP_FLAG, "done");
        log.info("CompactionWorker done in {}ms. Wrote stop flag.", durationMs);

        int rewritten  = result != null ? result.rewrittenDataFilesCount() : 0;
        int added      = result != null ? result.addedDataFilesCount()      : 0;
        // resultMap() not available in Iceberg 1.6; use addedDataFilesCount as proxy
        int fileGroups = added;

        Map<String, Object> metrics = Map.of(
            "durationMs",          durationMs,
            "filesRewritten",      rewritten,
            "filesAdded",          added,
            "fileGroupsRewritten", fileGroups,
            "fileGroupsSkipped",   0,
            "rewriteConflicts",    conflictCount.get(),
            "rewriteRetries",      retryCount.get(),
            "partialProgress",     partialProgress,
            "options",             options
        );
        new ObjectMapper().writeValue(
            new File("results/exp07-compaction-metrics.json"), metrics);
    }
}
