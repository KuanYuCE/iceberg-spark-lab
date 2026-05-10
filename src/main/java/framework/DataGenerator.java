package framework;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;

public class DataGenerator {

    private static final Logger log = LoggerFactory.getLogger(DataGenerator.class);

    public static final StructType SCHEMA = new StructType()
        .add("event_id",   DataTypes.StringType,    false)
        .add("user_id",    DataTypes.StringType,    false)
        .add("event_type", DataTypes.StringType,    false)
        .add("payload",    DataTypes.StringType,    true)
        .add("region",     DataTypes.StringType,    false)
        .add("ts",         DataTypes.TimestampType, false);

    private static final String[] REGIONS     = {"us-east", "eu-west", "ap-south"};
    private static final String[] EVENT_TYPES = {"click", "view", "purchase", "error"};

    private final SparkSession spark;

    public DataGenerator(SparkSession spark) {
        this.spark = spark;
    }

    /**
     * Phase 1: bulk write with high repartition count to produce many small files.
     * Phase 2: repeated tiny appends simulating streaming ingestion.
     */
    public void generate() {
        log.info("DataGenerator: phase 1 — bulk {} rows, repartition({})",
            LakehouseConfig.bulkRows(), LakehouseConfig.bulkRepartitions());
        bulkWrite();

        log.info("DataGenerator: phase 2 — {} batches × {} rows",
            LakehouseConfig.smallBatchIterations(), LakehouseConfig.smallBatchRows());
        smallBatchLoop();

        log.info("DataGenerator: done");
    }

    private void bulkWrite() {
        Dataset<Row> df = spark.createDataFrame(
            buildRows(LakehouseConfig.bulkRows()), SCHEMA);
        try {
            df.repartition(LakehouseConfig.bulkRepartitions())
              .writeTo(LakehouseConfig.FULL_TABLE)
              .append();
        } catch (Exception e) {
            throw new RuntimeException("Bulk write failed", e);
        }
    }

    private void smallBatchLoop() {
        for (int i = 0; i < LakehouseConfig.smallBatchIterations(); i++) {
            try {
                spark.createDataFrame(buildRows(LakehouseConfig.smallBatchRows()), SCHEMA)
                     .writeTo(LakehouseConfig.FULL_TABLE)
                     .append();
            } catch (Exception e) {
                throw new RuntimeException("Small batch write failed at iteration " + i, e);
            }
            if ((i + 1) % 10 == 0) {
                log.info("DataGenerator: {}/{} small batches written",
                    i + 1, LakehouseConfig.smallBatchIterations());
            }
        }
    }

    private List<Row> buildRows(int count) {
        Random rng = new Random();
        Instant now = Instant.now();
        List<Row> rows = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            rows.add(RowFactory.create(
                UUID.randomUUID().toString(),
                "user_" + rng.nextInt(10_000),
                EVENT_TYPES[rng.nextInt(EVENT_TYPES.length)],
                "{\"seq\":" + i + ",\"v\":\"" + Long.toHexString(rng.nextLong()) + "\"}",
                REGIONS[rng.nextInt(REGIONS.length)],
                Timestamp.from(now.minusSeconds(rng.nextInt(7 * 24 * 3600)))
            ));
        }
        return rows;
    }
}
