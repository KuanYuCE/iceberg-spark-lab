package framework;

import java.util.LinkedHashMap;
import java.util.Map;

public final class LakehouseConfig {

    public enum RunMode { LOCAL, CLOUD }

    // ── Connection ────────────────────────────────────────────────────────────
    public static final String MINIO_ENDPOINT = "http://localhost:9000";
    public static final String MINIO_ACCESS   = "minioadmin";
    public static final String MINIO_SECRET   = "minioadmin";
    public static final String HMS_URI        = "thrift://localhost:9083";
    public static final String WAREHOUSE      = "s3a://my-raw/iceberg-experiments";
    public static final String CATALOG        = "iceberg";
    public static final String DATABASE       = "iceberg_lab";
    public static final String TABLE          = "events";
    public static final String FULL_TABLE     = CATALOG + "." + DATABASE + "." + TABLE;
    public static final String TRINO_JDBC     = "jdbc:trino://localhost:8080/iceberg/" + DATABASE;
    public static final String TRINO_USER     = "admin";

    // ── Run mode (read from system properties set by Gradle -PrunMode=...) ───
    public static final RunMode RUN_MODE;
    public static final boolean PAUSE_FOR_UI;

    static {
        String mode = System.getProperty("runMode", "local");
        RUN_MODE     = "cloud".equalsIgnoreCase(mode) ? RunMode.CLOUD : RunMode.LOCAL;
        PAUSE_FOR_UI = Boolean.parseBoolean(System.getProperty("pauseForUi", "false"));
    }

    // ── Spark base configuration ───────────────────────────────────────────────
    public static Map<String, String> baseSparkConfig() {
        Map<String, String> cfg = new LinkedHashMap<>();

        if (RUN_MODE == RunMode.LOCAL) {
            cfg.put("spark.master",                 "local[2]");
            cfg.put("spark.driver.memory",          "1g");
            cfg.put("spark.sql.shuffle.partitions", "4");
            cfg.put("spark.default.parallelism",    "4");
        } else {
            cfg.put("spark.master",                 "local[8]");
            cfg.put("spark.driver.memory",          "8g");
            cfg.put("spark.sql.shuffle.partitions", "16");
            cfg.put("spark.default.parallelism",    "16");
        }

        // S3A → MinIO
        cfg.put("spark.hadoop.fs.s3a.endpoint",               MINIO_ENDPOINT);
        cfg.put("spark.hadoop.fs.s3a.access.key",             MINIO_ACCESS);
        cfg.put("spark.hadoop.fs.s3a.secret.key",             MINIO_SECRET);
        cfg.put("spark.hadoop.fs.s3a.path.style.access",      "true");
        cfg.put("spark.hadoop.fs.s3a.connection.ssl.enabled", "false");
        cfg.put("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem");

        // Iceberg catalog → HMS
        cfg.put("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions");
        cfg.put("spark.sql.catalog." + CATALOG,
                "org.apache.iceberg.spark.SparkCatalog");
        cfg.put("spark.sql.catalog." + CATALOG + ".type",      "hive");
        cfg.put("spark.sql.catalog." + CATALOG + ".uri",       HMS_URI);
        cfg.put("spark.sql.catalog." + CATALOG + ".warehouse", WAREHOUSE);
        cfg.put("spark.sql.catalog." + CATALOG + ".io-impl",
                "org.apache.iceberg.hadoop.HadoopFileIO");

        return cfg;
    }

    // ── DataGenerator parameters ───────────────────────────────────────────────
    public static int bulkRows()             { return RUN_MODE == RunMode.LOCAL ? 100_000   : 1_000_000; }
    public static int bulkRepartitions()     { return RUN_MODE == RunMode.LOCAL ? 60        : 300; }
    public static int smallBatchIterations() { return 50; }
    public static int smallBatchRows()       { return RUN_MODE == RunMode.LOCAL ? 300       : 3_000; }

    // ── Experiment parameter ranges ────────────────────────────────────────────
    public static long[] targetFileSizes() {
        return RUN_MODE == RunMode.LOCAL
            ? new long[]{ 4_194_304L, 16_777_216L, 67_108_864L }      // 4 MB / 16 MB / 64 MB
            : new long[]{ 33_554_432L, 134_217_728L, 536_870_912L };  // 32 MB / 128 MB / 512 MB
    }

    public static int[] concurrentRewrites() {
        return RUN_MODE == RunMode.LOCAL ? new int[]{ 1, 2, 4 } : new int[]{ 1, 5, 10 };
    }

    public static int cores() {
        return RUN_MODE == RunMode.LOCAL ? 2 : 8;
    }

    private LakehouseConfig() {}
}
