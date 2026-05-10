package experiments;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import framework.*;
import framework.model.*;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * Process C for Exp07.
 * Reads exp07-ingestion-metrics.json and exp07-compaction-metrics.json,
 * loads the final Iceberg table state, and produces a combined report.
 *
 * Run AFTER both Exp07a and Exp07b have completed.
 */
public class Exp07_ResultAnalyzer {

    private static final Logger log = LoggerFactory.getLogger(Exp07_ResultAnalyzer.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final DateTimeFormatter ISO = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {
        log.info("Exp07 ResultAnalyzer starting");

        Map<String, Object> ingMetrics = MAPPER.readValue(
            new File("results/exp07-ingestion-metrics.json"),
            new TypeReference<>() {});
        Map<String, Object> cmpMetrics = MAPPER.readValue(
            new File("results/exp07-compaction-metrics.json"),
            new TypeReference<>() {});

        SparkSession.Builder builder = SparkSession.builder().appName("Exp07_ResultAnalyzer");
        LakehouseConfig.baseSparkConfig().forEach(builder::config);
        SparkSession spark = builder.getOrCreate();

        var table = Spark3Util.loadIcebergTable(spark, LakehouseConfig.FULL_TABLE);

        MetricsCollector collector = new MetricsCollector();
        SnapshotMetrics finalMetrics = collector.collect(table, "final");

        ConflictObserver observer = new ConflictObserver();
        ConcurrencyMetrics concurrencyMetrics = observer.analyze(
            table,
            ((Number) ingMetrics.get("ingestionCommitsAttempted")).intValue(),
            ((Number) ingMetrics.get("ingestionCommitsSucceeded")).intValue(),
            ((Number) ingMetrics.get("ingestionCommitsFailed")).intValue(),
            ((Number) cmpMetrics.get("rewriteRetries")).intValue(),
            ((Number) cmpMetrics.get("rewriteConflicts")).intValue(),
            ((Number) cmpMetrics.get("fileGroupsSkipped")).intValue()
        );

        spark.close();

        SnapshotMetrics stub = new SnapshotMetrics("before-unavailable",
            0, 0, 0, 0, 0, 0, 0, Map.of());

        CompactionPerformance compPerf = new CompactionPerformance(
            ((Number) cmpMetrics.get("durationMs")).longValue(),
            ((Number) cmpMetrics.get("filesRewritten")).intValue(),
            ((Number) cmpMetrics.get("filesAdded")).intValue(),
            ((Number) cmpMetrics.get("fileGroupsRewritten")).intValue(),
            0.0
        );

        Map<String, String> options = (Map<String, String>) (Object) cmpMetrics.get("options");

        ExperimentResult result = new ExperimentResult(
            "Exp07_ConcurrentConflicts",
            LocalDateTime.now().format(ISO),
            LakehouseConfig.RUN_MODE.name(),
            options,
            LakehouseConfig.baseSparkConfig(),
            stub, finalMetrics,
            compPerf,
            new SparkResourceMetrics(Map.of(), Map.of(), 0, 0, 0, 0, 0, 0.0, 0.0, 0.0),
            List.of(),
            concurrencyMetrics
        );

        ReportWriter writer = new ReportWriter("results");
        ReportWriter.OutputPaths paths = writer.write(result);
        log.info("Exp07 report written: {}", paths.htmlPath());
    }
}
