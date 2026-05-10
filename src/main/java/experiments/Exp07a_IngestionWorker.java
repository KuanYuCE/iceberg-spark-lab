package experiments;

import com.fasterxml.jackson.databind.ObjectMapper;
import framework.DataGenerator;
import framework.LakehouseConfig;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.*;
import java.nio.file.*;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;

/**
 * Process A for Exp07.
 *
 * 1. Creates the table and generates initial small files (same as other experiments).
 * 2. Writes results/exp07-ingestion-ready.flag to signal Exp07b to start.
 * 3. Loops: appends a small batch every INTERVAL_MS until results/exp07-stop.flag appears.
 * 4. Writes results/exp07-ingestion-metrics.json and exits.
 *
 * Scenario is selected via system property: -DscenarioIntervalMs=200 (default) or 2000.
 */
public class Exp07a_IngestionWorker {

    private static final Logger log = LoggerFactory.getLogger(Exp07a_IngestionWorker.class);
    private static final Path READY_FLAG = Path.of("results/exp07-ingestion-ready.flag");
    private static final Path STOP_FLAG  = Path.of("results/exp07-stop.flag");
    private static final int  BATCH_ROWS = 200;

    public static void main(String[] args) throws Exception {
        long intervalMs = Long.parseLong(System.getProperty("scenarioIntervalMs", "200"));
        log.info("Exp07a IngestionWorker starting — interval={}ms", intervalMs);

        Files.createDirectories(Path.of("results"));
        Files.deleteIfExists(READY_FLAG);
        Files.deleteIfExists(STOP_FLAG);

        SparkSession spark = buildSpark();
        setupTable(spark);
        new DataGenerator(spark).generate();

        log.info("Initial data loaded. Signalling ready.");
        Files.writeString(READY_FLAG, Instant.now().toString());

        int attempted = 0, succeeded = 0, failed = 0;
        Random rng = new Random();
        String[] regions    = {"us-east", "eu-west", "ap-south"};
        String[] eventTypes = {"click", "view", "purchase", "error"};

        while (!Files.exists(STOP_FLAG)) {
            attempted++;
            try {
                List<Row> rows = new ArrayList<>(BATCH_ROWS);
                Instant now = Instant.now();
                for (int i = 0; i < BATCH_ROWS; i++) {
                    rows.add(RowFactory.create(
                        UUID.randomUUID().toString(),
                        "user_" + rng.nextInt(10_000),
                        eventTypes[rng.nextInt(4)],
                        "{\"seq\":" + i + "}",
                        regions[rng.nextInt(3)],
                        Timestamp.from(now.minusSeconds(rng.nextInt(3600)))
                    ));
                }
                spark.createDataFrame(rows, DataGenerator.SCHEMA)
                     .writeTo(LakehouseConfig.FULL_TABLE)
                     .append();
                succeeded++;
                log.debug("Ingestion commit #{} succeeded", succeeded);
            } catch (Exception e) {
                failed++;
                log.warn("Ingestion commit failed ({}): {}", failed, e.getMessage());
            }
            Thread.sleep(intervalMs);
        }

        spark.close();
        log.info("IngestionWorker done — attempted={} succeeded={} failed={}",
            attempted, succeeded, failed);

        Map<String, Object> metrics = Map.of(
            "ingestionCommitsAttempted", attempted,
            "ingestionCommitsSucceeded", succeeded,
            "ingestionCommitsFailed",    failed,
            "intervalMs",               intervalMs
        );
        new ObjectMapper().writeValue(
            new File("results/exp07-ingestion-metrics.json"), metrics);
    }

    private static SparkSession buildSpark() {
        SparkSession.Builder builder = SparkSession.builder().appName("Exp07a_IngestionWorker");
        LakehouseConfig.baseSparkConfig().forEach(builder::config);
        return builder.getOrCreate();
    }

    private static void setupTable(SparkSession spark) {
        spark.sql("CREATE DATABASE IF NOT EXISTS " + LakehouseConfig.CATALOG
            + "." + LakehouseConfig.DATABASE);
        spark.sql("DROP TABLE IF EXISTS " + LakehouseConfig.FULL_TABLE);
        spark.sql(
            "CREATE TABLE " + LakehouseConfig.FULL_TABLE + " ("
            + "  event_id STRING NOT NULL, user_id STRING NOT NULL,"
            + "  event_type STRING NOT NULL, payload STRING,"
            + "  region STRING NOT NULL, ts TIMESTAMP NOT NULL"
            + ") USING iceberg"
            + " PARTITIONED BY (region, days(ts))"
            + " TBLPROPERTIES ("
            + "  'write.format.default' = 'parquet',"
            + "  'commit.retry.num-retries' = '5'"
            + ")");
    }
}
