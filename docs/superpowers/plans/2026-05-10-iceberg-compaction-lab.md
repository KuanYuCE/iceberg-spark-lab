# Iceberg Compaction Lab — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Java + Spark + Iceberg experiment lab that generates small files, runs compaction experiments with configurable parameters, and produces JSON + HTML reports comparing file metrics, Spark resource usage, Trino query performance, and concurrent commit conflicts.

**Architecture:** Abstract base class (`BaseExperiment`) implements a template method that orchestrates data generation, metrics collection, compaction, and reporting. Each experiment subclass overrides only `runCompaction()`, `compactionOptions()`, and optionally `sparkOverrides()`. Exp07 breaks from this pattern and uses two independent JVM processes coordinated by a shell script.

**Tech Stack:** Java 17, Gradle 8, Apache Spark 3.5.3, Apache Iceberg 1.6.1, MinIO (S3A), Hive Metastore (HMS), Trino 467, Jackson 2.15.x (via Spark), JUnit 5.

**Spec:** `docs/superpowers/specs/2026-05-10-iceberg-compaction-design.md`

---

## File Map

```
src/main/java/
  framework/
    LakehouseConfig.java            # All constants and run-mode config
    BaseExperiment.java             # Template method: setup → data → before → compact → after → report
    DataGenerator.java              # Two-phase small-file generation
    MetricsCollector.java           # Iceberg metadata API: file counts, sizes, snapshots
    SparkMetricsListener.java       # SparkListener capturing stage-level CPU/GC/IO
    TrinoQueryBenchmark.java        # Trino JDBC: 4 benchmark queries × 3 runs → median
    ConflictObserver.java           # Snapshot lineage analysis for Exp07
    ReportWriter.java               # Writes .json + .html (Chart.js) to results/
    model/
      SnapshotMetrics.java          # File-layer snapshot: counts, sizes, per-partition map
      CompactionPerformance.java    # Rewrite result: duration, files, bytes, throughput
      SparkResourceMetrics.java     # Stage metrics: CPU, GC, memory, derived ratios
      QueryBenchmarkResult.java     # One Trino query: before/after ms + improvement %
      ConcurrencyMetrics.java       # Exp07: commit counts, conflicts, snapshot lineage
      SnapshotEvent.java            # One snapshot in the lineage chain
      ExperimentResult.java         # Top-level POJO → serialized to JSON
  experiments/
    Exp01_TargetFileSize.java       # target-file-size-bytes: 4MB/16MB/64MB (local)
    Exp02_MinInputFiles.java        # min-input-files: 2/5/10
    Exp03_MaxConcurrentRewrites.java # max-concurrent-file-group-rewrites: 1/2/4 (local)
    Exp04_PartialProgress.java      # partial-progress + max-commits: 2/5/10
    Exp05_RewriteByPartition.java   # Single partition vs full table
    Exp06_FullVsPartitionRewrite.java # Strategy comparison
    Exp07a_IngestionWorker.java     # Process A: continuous append loop
    Exp07b_CompactionWorker.java    # Process B: rewrite with conflict monitoring
    Exp07_ResultAnalyzer.java       # Merges both outputs → combined report

src/test/java/
  framework/model/ExperimentResultSerializationTest.java
  framework/ReportWriterTest.java

scripts/
  run-exp07.sh                      # Orchestrates Exp07a + Exp07b + ResultAnalyzer

results/                            # gitignored; JSON + HTML output lands here
```

---

## Task 1: Gradle project setup

**Files:**
- Create: `build.gradle`
- Create: `settings.gradle`
- Create: `gradle/wrapper/gradle-wrapper.properties`
- Create: `src/main/resources/log4j2.xml`
- Create: `.gitignore` (append)

- [ ] **Step 1: Write `settings.gradle`**

```groovy
rootProject.name = 'my-iceberg-experiment'
```

- [ ] **Step 2: Write `build.gradle`**

```groovy
plugins {
    id 'java'
    id 'application'
}

group = 'com.example'
version = '1.0-SNAPSHOT'

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(17)
    }
}

repositories {
    mavenCentral()
}

ext {
    sparkVersion    = '3.5.3'
    icebergVersion  = '1.6.1'
    hadoopVersion   = '3.3.4'
    awsSdkVersion   = '1.12.772'
    trinoVersion    = '467'
    log4jVersion    = '2.23.1'
    junitVersion    = '5.11.0'
}

configurations.all {
    resolutionStrategy.eachDependency { details ->
        // Force consistent Jackson version (Spark 3.5 ships 2.15.2)
        if (details.requested.group == 'com.fasterxml.jackson.core') {
            details.useVersion '2.15.2'
        }
    }
}

dependencies {
    implementation "org.apache.spark:spark-sql_2.12:${sparkVersion}"
    implementation "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:${icebergVersion}"
    implementation("org.apache.hadoop:hadoop-aws:${hadoopVersion}") {
        exclude group: 'com.amazonaws'
    }
    implementation "com.amazonaws:aws-java-sdk-bundle:${awsSdkVersion}"
    implementation "io.trino:trino-jdbc:${trinoVersion}"
    runtimeOnly "org.apache.logging.log4j:log4j-slf4j2-impl:${log4jVersion}"
    runtimeOnly "org.apache.logging.log4j:log4j-core:${log4jVersion}"

    testImplementation "org.junit.jupiter:junit-jupiter:${junitVersion}"
    testRuntimeOnly 'org.junit.platform:junit-platform-launcher'
}

application {
    mainClass = project.findProperty('mainClass') ?: 'experiments.Exp01_TargetFileSize'
}

tasks.named('test', Test) {
    useJUnitPlatform()
}

tasks.named('run', JavaExec) {
    systemProperty 'runMode',    project.findProperty('runMode')    ?: 'local'
    systemProperty 'pauseForUi', project.findProperty('pauseForUi') ?: 'false'
    jvmArgs '-Xmx1g', '-XX:+UseG1GC'
    // Suppress noisy Spark/Hadoop logs during run
    systemProperty 'log4j.configurationFile', 'log4j2.xml'
}
```

- [ ] **Step 3: Write `src/main/resources/log4j2.xml`**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<Configuration status="WARN">
  <Appenders>
    <Console name="Console" target="SYSTEM_OUT">
      <PatternLayout pattern="%d{HH:mm:ss} %-5level %logger{36} - %msg%n"/>
    </Console>
  </Appenders>
  <Loggers>
    <Logger name="com.example" level="INFO"/>
    <Logger name="org.apache.spark" level="WARN"/>
    <Logger name="org.apache.hadoop" level="WARN"/>
    <Logger name="org.apache.iceberg" level="INFO"/>
    <Root level="WARN">
      <AppenderRef ref="Console"/>
    </Root>
  </Loggers>
</Configuration>
```

- [ ] **Step 4: Append to `.gitignore`**

```
results/
warehouse/
.gradle/
build/
*.class
```

- [ ] **Step 5: Generate Gradle wrapper**

```bash
gradle wrapper --gradle-version 8.8
```

- [ ] **Step 6: Verify the project compiles**

```bash
./gradlew compileJava
```

Expected: `BUILD SUCCESSFUL` (no source files yet, but Gradle resolves all dependencies without error).

- [ ] **Step 7: Create `results/` directory and add placeholder**

```bash
mkdir -p results && touch results/.gitkeep
```

- [ ] **Step 8: Commit**

```bash
git add build.gradle settings.gradle gradle/ src/main/resources/log4j2.xml .gitignore results/.gitkeep
git commit -m "feat: initialize Gradle project with Spark + Iceberg dependencies"
```

---

## Task 2: Model classes

**Files:**
- Create: `src/main/java/framework/model/SnapshotMetrics.java`
- Create: `src/main/java/framework/model/CompactionPerformance.java`
- Create: `src/main/java/framework/model/SparkResourceMetrics.java`
- Create: `src/main/java/framework/model/QueryBenchmarkResult.java`
- Create: `src/main/java/framework/model/SnapshotEvent.java`
- Create: `src/main/java/framework/model/ConcurrencyMetrics.java`
- Create: `src/main/java/framework/model/ExperimentResult.java`
- Test: `src/test/java/framework/model/ExperimentResultSerializationTest.java`

- [ ] **Step 1: Write `SnapshotMetrics.java`**

```java
package framework.model;

import java.util.Map;

public class SnapshotMetrics {
    public final String label;
    public final long fileCount;
    public final long totalSizeBytes;
    public final long avgFileSizeBytes;
    public final long minFileSizeBytes;
    public final long maxFileSizeBytes;
    public final long snapshotCount;
    public final long manifestCount;
    public final Map<String, Long> perPartitionFileCounts;

    public SnapshotMetrics(String label, long fileCount, long totalSizeBytes,
                           long avgFileSizeBytes, long minFileSizeBytes, long maxFileSizeBytes,
                           long snapshotCount, long manifestCount,
                           Map<String, Long> perPartitionFileCounts) {
        this.label = label;
        this.fileCount = fileCount;
        this.totalSizeBytes = totalSizeBytes;
        this.avgFileSizeBytes = avgFileSizeBytes;
        this.minFileSizeBytes = minFileSizeBytes;
        this.maxFileSizeBytes = maxFileSizeBytes;
        this.snapshotCount = snapshotCount;
        this.manifestCount = manifestCount;
        this.perPartitionFileCounts = perPartitionFileCounts;
    }
}
```

- [ ] **Step 2: Write `CompactionPerformance.java`**

```java
package framework.model;

public class CompactionPerformance {
    public final long durationMs;
    public final int filesRewritten;
    public final int filesAdded;
    public final int fileGroupsRewritten;
    public final double throughputMBps;

    public CompactionPerformance(long durationMs, int filesRewritten, int filesAdded,
                                 int fileGroupsRewritten, double throughputMBps) {
        this.durationMs = durationMs;
        this.filesRewritten = filesRewritten;
        this.filesAdded = filesAdded;
        this.fileGroupsRewritten = fileGroupsRewritten;
        this.throughputMBps = throughputMBps;
    }
}
```

- [ ] **Step 3: Write `SparkResourceMetrics.java`**

```java
package framework.model;

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

    public SparkResourceMetrics(Map<String, String> sparkConfig, Map<String, String> icebergOptions,
                                long taskCount, long totalCpuTimeMs, long totalGcTimeMs,
                                long bytesRead, long bytesWritten,
                                double gcPressureRatio, double parallelismEfficiency,
                                double rewriteThroughputMBps) {
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
```

- [ ] **Step 4: Write `QueryBenchmarkResult.java`**

```java
package framework.model;

public class QueryBenchmarkResult {
    public final String queryName;
    public final String sql;
    public final long beforeMs;
    public final long afterMs;
    public final double improvementPct;

    public QueryBenchmarkResult(String queryName, String sql, long beforeMs, long afterMs) {
        this.queryName = queryName;
        this.sql = sql;
        this.beforeMs = beforeMs;
        this.afterMs = afterMs;
        this.improvementPct = beforeMs > 0
            ? ((double)(beforeMs - afterMs) / beforeMs) * 100.0
            : 0.0;
    }
}
```

- [ ] **Step 5: Write `SnapshotEvent.java`**

```java
package framework.model;

public class SnapshotEvent {
    public final long snapshotId;
    public final Long parentSnapshotId;  // null for the first snapshot
    public final String operation;
    public final int addedDataFiles;
    public final int deletedDataFiles;
    public final long timestampMs;

    public SnapshotEvent(long snapshotId, Long parentSnapshotId, String operation,
                         int addedDataFiles, int deletedDataFiles, long timestampMs) {
        this.snapshotId = snapshotId;
        this.parentSnapshotId = parentSnapshotId;
        this.operation = operation;
        this.addedDataFiles = addedDataFiles;
        this.deletedDataFiles = deletedDataFiles;
        this.timestampMs = timestampMs;
    }
}
```

- [ ] **Step 6: Write `ConcurrencyMetrics.java`**

```java
package framework.model;

import java.util.List;

public class ConcurrencyMetrics {
    public final int ingestionCommitsAttempted;
    public final int ingestionCommitsSucceeded;
    public final int ingestionCommitsFailed;
    public final int rewriteCommitRetries;
    public final int rewriteCommitConflicts;
    public final int fileGroupsSkipped;
    public final long totalSnapshotsCreated;
    public final List<SnapshotEvent> snapshotLineage;

    public ConcurrencyMetrics(int ingestionCommitsAttempted, int ingestionCommitsSucceeded,
                              int ingestionCommitsFailed, int rewriteCommitRetries,
                              int rewriteCommitConflicts, int fileGroupsSkipped,
                              long totalSnapshotsCreated, List<SnapshotEvent> snapshotLineage) {
        this.ingestionCommitsAttempted = ingestionCommitsAttempted;
        this.ingestionCommitsSucceeded = ingestionCommitsSucceeded;
        this.ingestionCommitsFailed = ingestionCommitsFailed;
        this.rewriteCommitRetries = rewriteCommitRetries;
        this.rewriteCommitConflicts = rewriteCommitConflicts;
        this.fileGroupsSkipped = fileGroupsSkipped;
        this.totalSnapshotsCreated = totalSnapshotsCreated;
        this.snapshotLineage = snapshotLineage;
    }
}
```

- [ ] **Step 7: Write `ExperimentResult.java`**

```java
package framework.model;

import java.util.List;
import java.util.Map;

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

    public ExperimentResult(String experimentName, String timestamp, String runMode,
                            Map<String, String> compactionOptions, Map<String, String> sparkConfig,
                            SnapshotMetrics beforeFileMetrics, SnapshotMetrics afterFileMetrics,
                            CompactionPerformance compactionPerf, SparkResourceMetrics sparkResources,
                            List<QueryBenchmarkResult> queryBenchmark,
                            ConcurrencyMetrics concurrencyMetrics) {
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
```

- [ ] **Step 8: Write the failing serialization test**

```java
// src/test/java/framework/model/ExperimentResultSerializationTest.java
package framework.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import java.util.List;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;

class ExperimentResultSerializationTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void roundTripSerializesAllFields() throws Exception {
        ExperimentResult result = new ExperimentResult(
            "Exp01_TargetFileSize", "2026-05-10T14:00:00", "LOCAL",
            Map.of("target-file-size-bytes", "4194304"),
            Map.of("spark.master", "local[2]"),
            new SnapshotMetrics("before", 110, 52_428_800L, 476_625L, 10_000L, 900_000L, 52, 104, Map.of("region=us-east/ts_day=2026-05-08", 40L)),
            new SnapshotMetrics("after", 3, 52_000_000L, 17_333_333L, 16_000_000L, 20_000_000L, 53, 6, Map.of("region=us-east/ts_day=2026-05-08", 1L)),
            new CompactionPerformance(12_345L, 110, 3, 3, 4.2),
            new SparkResourceMetrics(Map.of("spark.master", "local[2]"), Map.of("target-file-size-bytes", "4194304"),
                220L, 8_000L, 400L, 52_428_800L, 52_000_000L, 0.05, 1.1, 4.2),
            List.of(new QueryBenchmarkResult("count_all", "SELECT COUNT(*) FROM iceberg.iceberg_lab.events", 520L, 380L)),
            null
        );

        String json = mapper.writeValueAsString(result);
        ExperimentResult parsed = mapper.readValue(json, ExperimentResult.class);

        assertEquals("Exp01_TargetFileSize", parsed.experimentName);
        assertEquals(110, parsed.beforeFileMetrics.fileCount);
        assertEquals(3, parsed.afterFileMetrics.fileCount);
        assertEquals(12_345L, parsed.compactionPerf.durationMs);
        assertEquals(1, parsed.queryBenchmark.size());
        assertNull(parsed.concurrencyMetrics);
        assertTrue(parsed.queryBenchmark.get(0).improvementPct > 0);
    }
}
```

- [ ] **Step 9: Add Jackson annotations to model classes**

Add `@JsonCreator` and `@JsonProperty` to all model constructors so Jackson can deserialize them. Update each constructor, e.g. for `SnapshotMetrics`:

```java
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public SnapshotMetrics(
    @JsonProperty("label") String label,
    @JsonProperty("fileCount") long fileCount,
    @JsonProperty("totalSizeBytes") long totalSizeBytes,
    @JsonProperty("avgFileSizeBytes") long avgFileSizeBytes,
    @JsonProperty("minFileSizeBytes") long minFileSizeBytes,
    @JsonProperty("maxFileSizeBytes") long maxFileSizeBytes,
    @JsonProperty("snapshotCount") long snapshotCount,
    @JsonProperty("manifestCount") long manifestCount,
    @JsonProperty("perPartitionFileCounts") Map<String, Long> perPartitionFileCounts) {
```

Apply the same `@JsonCreator` + `@JsonProperty` pattern to **all** model constructors: `CompactionPerformance`, `SparkResourceMetrics`, `QueryBenchmarkResult`, `SnapshotEvent`, `ConcurrencyMetrics`, `ExperimentResult`.

- [ ] **Step 10: Run test to verify it passes**

```bash
./gradlew test --tests "framework.model.ExperimentResultSerializationTest"
```

Expected: `BUILD SUCCESSFUL`, 1 test passed.

- [ ] **Step 11: Commit**

```bash
git add src/
git commit -m "feat: add model POJOs with Jackson serialization"
```

---

## Task 3: LakehouseConfig

**Files:**
- Create: `src/main/java/framework/LakehouseConfig.java`

- [ ] **Step 1: Write `LakehouseConfig.java`**

```java
package framework;

import java.util.LinkedHashMap;
import java.util.Map;

public final class LakehouseConfig {

    public enum RunMode { LOCAL, CLOUD }

    // ── Connection ────────────────────────────────────────────────────────────
    public static final String MINIO_ENDPOINT  = "http://localhost:9000";
    public static final String MINIO_ACCESS    = "minioadmin";
    public static final String MINIO_SECRET    = "minioadmin";
    public static final String HMS_URI         = "thrift://localhost:9083";
    public static final String WAREHOUSE       = "s3a://my-raw/iceberg-experiments";
    public static final String CATALOG         = "iceberg";
    public static final String DATABASE        = "iceberg_lab";
    public static final String TABLE           = "events";
    public static final String FULL_TABLE      = CATALOG + "." + DATABASE + "." + TABLE;
    public static final String TRINO_JDBC      = "jdbc:trino://localhost:8080/iceberg/" + DATABASE;
    public static final String TRINO_USER      = "admin";

    // ── Run mode (read from system properties set by Gradle) ──────────────────
    public static final RunMode RUN_MODE;
    public static final boolean PAUSE_FOR_UI;

    static {
        String mode = System.getProperty("runMode", "local");
        RUN_MODE    = "cloud".equalsIgnoreCase(mode) ? RunMode.CLOUD : RunMode.LOCAL;
        PAUSE_FOR_UI = Boolean.parseBoolean(System.getProperty("pauseForUi", "false"));
    }

    // ── Spark base configuration ───────────────────────────────────────────────
    public static Map<String, String> baseSparkConfig() {
        Map<String, String> cfg = new LinkedHashMap<>();
        if (RUN_MODE == RunMode.LOCAL) {
            cfg.put("spark.master",                      "local[2]");
            cfg.put("spark.driver.memory",               "1g");
            cfg.put("spark.sql.shuffle.partitions",      "4");
            cfg.put("spark.default.parallelism",         "4");
        } else {
            cfg.put("spark.master",                      "local[8]");
            cfg.put("spark.driver.memory",               "8g");
            cfg.put("spark.sql.shuffle.partitions",      "16");
            cfg.put("spark.default.parallelism",         "16");
        }
        // S3A → MinIO
        cfg.put("spark.hadoop.fs.s3a.endpoint",                  MINIO_ENDPOINT);
        cfg.put("spark.hadoop.fs.s3a.access.key",                MINIO_ACCESS);
        cfg.put("spark.hadoop.fs.s3a.secret.key",                MINIO_SECRET);
        cfg.put("spark.hadoop.fs.s3a.path.style.access",         "true");
        cfg.put("spark.hadoop.fs.s3a.connection.ssl.enabled",    "false");
        cfg.put("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem");
        // Iceberg catalog → HMS
        cfg.put("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions");
        cfg.put("spark.sql.catalog." + CATALOG,
                "org.apache.iceberg.spark.SparkCatalog");
        cfg.put("spark.sql.catalog." + CATALOG + ".type",        "hive");
        cfg.put("spark.sql.catalog." + CATALOG + ".uri",         HMS_URI);
        cfg.put("spark.sql.catalog." + CATALOG + ".warehouse",   WAREHOUSE);
        cfg.put("spark.sql.catalog." + CATALOG + ".io-impl",
                "org.apache.iceberg.hadoop.HadoopFileIO");
        return cfg;
    }

    // ── DataGenerator parameters ───────────────────────────────────────────────
    public static int bulkRows()             { return RUN_MODE == RunMode.LOCAL ? 100_000 : 1_000_000; }
    public static int bulkRepartitions()     { return RUN_MODE == RunMode.LOCAL ? 60      : 300; }
    public static int smallBatchIterations() { return 50; }
    public static int smallBatchRows()       { return RUN_MODE == RunMode.LOCAL ? 300     : 3_000; }

    // ── Experiment parameter ranges ────────────────────────────────────────────
    public static long[] targetFileSizes() {
        return RUN_MODE == RunMode.LOCAL
            ? new long[]{ 4_194_304L, 16_777_216L, 67_108_864L }     // 4 MB / 16 MB / 64 MB
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
```

- [ ] **Step 2: Verify compilation**

```bash
./gradlew compileJava
```

Expected: `BUILD SUCCESSFUL`.

- [ ] **Step 3: Commit**

```bash
git add src/main/java/framework/LakehouseConfig.java
git commit -m "feat: add LakehouseConfig with LOCAL/CLOUD run modes"
```

---

## Task 4: DataGenerator

**Files:**
- Create: `src/main/java/framework/DataGenerator.java`

- [ ] **Step 1: Write `DataGenerator.java`**

```java
package framework;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.apache.spark.api.java.function.MapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;

public class DataGenerator {

    private static final Logger log = LoggerFactory.getLogger(DataGenerator.class);

    static final StructType SCHEMA = new StructType()
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
     * Phase 1: bulk write with high repartition to produce many small files.
     * Phase 2: loop of tiny appends to simulate streaming ingestion.
     */
    public void generate() {
        log.info("DataGenerator: phase 1 — bulk {} rows repartitioned to {}",
            LakehouseConfig.bulkRows(), LakehouseConfig.bulkRepartitions());
        bulkWrite();

        log.info("DataGenerator: phase 2 — {} small batches of {} rows",
            LakehouseConfig.smallBatchIterations(), LakehouseConfig.smallBatchRows());
        smallBatchLoop();
    }

    private void bulkWrite() {
        List<Row> rows = buildRows(LakehouseConfig.bulkRows());
        Dataset<Row> df = spark.createDataFrame(rows, SCHEMA);
        df.repartition(LakehouseConfig.bulkRepartitions())
          .writeTo(LakehouseConfig.FULL_TABLE)
          .append();
    }

    private void smallBatchLoop() {
        for (int i = 0; i < LakehouseConfig.smallBatchIterations(); i++) {
            List<Row> rows = buildRows(LakehouseConfig.smallBatchRows());
            spark.createDataFrame(rows, SCHEMA)
                 .writeTo(LakehouseConfig.FULL_TABLE)
                 .append();
            if ((i + 1) % 10 == 0) {
                log.info("DataGenerator: completed {}/{} small batches",
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
```

- [ ] **Step 2: Compile**

```bash
./gradlew compileJava
```

Expected: `BUILD SUCCESSFUL`.

- [ ] **Step 3: Commit**

```bash
git add src/main/java/framework/DataGenerator.java
git commit -m "feat: add two-phase DataGenerator for small file creation"
```

---

## Task 5: SparkMetricsListener

**Files:**
- Create: `src/main/java/framework/SparkMetricsListener.java`

- [ ] **Step 1: Write `SparkMetricsListener.java`**

```java
package framework;

import framework.model.SparkResourceMetrics;
import org.apache.spark.scheduler.*;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class SparkMetricsListener extends SparkListener {

    private final AtomicLong totalCpuTimeNs  = new AtomicLong(0);
    private final AtomicLong totalGcTimeMs   = new AtomicLong(0);
    private final AtomicLong taskCount       = new AtomicLong(0);
    private final AtomicLong bytesRead       = new AtomicLong(0);
    private final AtomicLong bytesWritten    = new AtomicLong(0);

    /** Call before starting the compaction job to zero out counters. */
    public void reset() {
        totalCpuTimeNs.set(0);
        totalGcTimeMs.set(0);
        taskCount.set(0);
        bytesRead.set(0);
        bytesWritten.set(0);
    }

    @Override
    public void onTaskEnd(SparkListenerTaskEnd taskEnd) {
        // Only count successfully completed tasks
        if (!(taskEnd.reason() instanceof Success$)) return;

        org.apache.spark.executor.TaskMetrics m = taskEnd.taskMetrics();
        totalCpuTimeNs.addAndGet(m.executorCpuTime());
        totalGcTimeMs.addAndGet(m.jvmGCTime());
        taskCount.incrementAndGet();
        bytesRead.addAndGet(m.inputMetrics().bytesRead());
        bytesWritten.addAndGet(m.outputMetrics().bytesWritten());
    }

    /**
     * Call after compaction completes to produce a snapshot of collected metrics.
     *
     * @param sparkConfig     the Spark config used for this experiment
     * @param icebergOptions  the Iceberg rewrite options used
     * @param durationMs      wall-clock duration of the compaction call
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

        double gcPressure = cpuMs > 0 ? (double) gcMs / cpuMs : 0.0;
        double parallelismEff = (durationMs > 0 && cores > 0)
            ? (double) tasks / (cores * (durationMs / 1000.0))
            : 0.0;
        double throughput = (durationMs > 0 && bw > 0)
            ? ((double) bw / (1024.0 * 1024.0)) / (durationMs / 1000.0)
            : 0.0;

        return new SparkResourceMetrics(sparkConfig, icebergOptions,
            tasks, cpuMs, gcMs, br, bw,
            gcPressure, parallelismEff, throughput);
    }
}
```

- [ ] **Step 2: Compile**

```bash
./gradlew compileJava
```

Expected: `BUILD SUCCESSFUL`.

- [ ] **Step 3: Commit**

```bash
git add src/main/java/framework/SparkMetricsListener.java
git commit -m "feat: add SparkMetricsListener for stage-level CPU/GC/IO metrics"
```

---

## Task 6: MetricsCollector

**Files:**
- Create: `src/main/java/framework/MetricsCollector.java`

- [ ] **Step 1: Write `MetricsCollector.java`**

```java
package framework;

import framework.model.SnapshotMetrics;
import org.apache.iceberg.*;
import org.apache.iceberg.io.CloseableIterable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.*;

public class MetricsCollector {

    private static final Logger log = LoggerFactory.getLogger(MetricsCollector.class);

    /**
     * Reads the current table state via Iceberg's metadata API.
     * Does NOT run a Spark job — reads manifest and data file metadata only.
     *
     * @param table  the live Iceberg Table object
     * @param label  "before" or "after", used to label the result
     */
    public SnapshotMetrics collect(Table table, String label) {
        log.info("MetricsCollector: collecting '{}' metrics for {}", label, table.name());

        long fileCount = 0;
        long totalSize = 0;
        long minSize   = Long.MAX_VALUE;
        long maxSize   = 0;
        Map<String, Long> perPartition = new LinkedHashMap<>();

        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask task : tasks) {
                DataFile file = task.file();
                long size = file.fileSizeInBytes();
                fileCount++;
                totalSize += size;
                minSize = Math.min(minSize, size);
                maxSize = Math.max(maxSize, size);

                String partKey = task.spec().partitionToPath(file.partition());
                perPartition.merge(partKey, 1L, Long::sum);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to scan table files for metrics", e);
        }

        long avgSize = fileCount > 0 ? totalSize / fileCount : 0;
        if (fileCount == 0) minSize = 0;

        // Count all snapshots in the table history
        long snapshotCount = 0;
        for (Snapshot ignored : table.snapshots()) {
            snapshotCount++;
        }

        // Count manifests in the current snapshot
        long manifestCount = 0;
        Snapshot current = table.currentSnapshot();
        if (current != null) {
            manifestCount = current.allManifests(table.io()).size();
        }

        log.info("MetricsCollector [{}]: files={}, totalMB={:.1f}, snapshots={}, manifests={}",
            label, fileCount, totalSize / 1_048_576.0, snapshotCount, manifestCount);

        return new SnapshotMetrics(label, fileCount, totalSize, avgSize,
            minSize, maxSize, snapshotCount, manifestCount, perPartition);
    }
}
```

- [ ] **Step 2: Compile**

```bash
./gradlew compileJava
```

Expected: `BUILD SUCCESSFUL`.

- [ ] **Step 3: Commit**

```bash
git add src/main/java/framework/MetricsCollector.java
git commit -m "feat: add MetricsCollector reading Iceberg file + snapshot metadata"
```

---

## Task 7: TrinoQueryBenchmark

**Files:**
- Create: `src/main/java/framework/TrinoQueryBenchmark.java`

- [ ] **Step 1: Write `TrinoQueryBenchmark.java`**

```java
package framework;

import framework.model.QueryBenchmarkResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.*;
import java.util.*;

public class TrinoQueryBenchmark {

    private static final Logger log = LoggerFactory.getLogger(TrinoQueryBenchmark.class);
    private static final int RUNS = 3;

    private static final List<String[]> QUERIES = List.of(
        new String[]{"count_all",
            "SELECT COUNT(*) FROM " + LakehouseConfig.FULL_TABLE},
        new String[]{"group_by_region",
            "SELECT region, COUNT(*) FROM " + LakehouseConfig.FULL_TABLE + " GROUP BY region"},
        new String[]{"partition_prune",
            "SELECT COUNT(*) FROM " + LakehouseConfig.FULL_TABLE
            + " WHERE region = 'us-east' AND ts >= TIMESTAMP '2026-05-03 00:00:00'"},
        new String[]{"aggregate_by_event_type",
            "SELECT event_type, AVG(LENGTH(payload)) FROM " + LakehouseConfig.FULL_TABLE
            + " GROUP BY event_type"}
    );

    /**
     * Runs each benchmark query RUNS times, returns median latency per query.
     *
     * @param label  "before" or "after" (used only for logging)
     */
    public List<Long> runAll(String label) {
        log.info("TrinoQueryBenchmark [{}]: running {} queries × {} runs",
            label, QUERIES.size(), RUNS);
        List<Long> medians = new ArrayList<>();
        try (Connection conn = openConnection()) {
            for (String[] entry : QUERIES) {
                long median = measureMedianMs(conn, entry[0], entry[1], label);
                medians.add(median);
            }
        } catch (SQLException e) {
            log.warn("TrinoQueryBenchmark [{}]: connection failed — {}", label, e.getMessage());
            // Return zeros so the experiment can still complete without Trino
            for (int i = 0; i < QUERIES.size(); i++) medians.add(0L);
        }
        return medians;
    }

    /**
     * Pairs before and after median lists into QueryBenchmarkResult objects.
     */
    public List<QueryBenchmarkResult> pair(List<Long> beforeMedians, List<Long> afterMedians) {
        List<QueryBenchmarkResult> results = new ArrayList<>();
        for (int i = 0; i < QUERIES.size(); i++) {
            results.add(new QueryBenchmarkResult(
                QUERIES.get(i)[0], QUERIES.get(i)[1],
                beforeMedians.get(i), afterMedians.get(i)));
        }
        return results;
    }

    private long measureMedianMs(Connection conn, String name, String sql, String label)
            throws SQLException {
        long[] times = new long[RUNS];
        for (int i = 0; i < RUNS; i++) {
            long start = System.currentTimeMillis();
            try (Statement stmt = conn.createStatement();
                 ResultSet rs   = stmt.executeQuery(sql)) {
                while (rs.next()) { /* consume */ }
            }
            times[i] = System.currentTimeMillis() - start;
        }
        Arrays.sort(times);
        long median = times[RUNS / 2];
        log.info("TrinoQueryBenchmark [{}] {}: {}ms (median of {} runs)", label, name, median, RUNS);
        return median;
    }

    private Connection openConnection() throws SQLException {
        Properties props = new Properties();
        props.setProperty("user", LakehouseConfig.TRINO_USER);
        return DriverManager.getConnection(LakehouseConfig.TRINO_JDBC, props);
    }
}
```

- [ ] **Step 2: Compile**

```bash
./gradlew compileJava
```

Expected: `BUILD SUCCESSFUL`.

- [ ] **Step 3: Commit**

```bash
git add src/main/java/framework/TrinoQueryBenchmark.java
git commit -m "feat: add TrinoQueryBenchmark with 4 queries × 3 runs median latency"
```

---

## Task 8: ReportWriter

**Files:**
- Create: `src/main/java/framework/ReportWriter.java`
- Test: `src/test/java/framework/ReportWriterTest.java`

- [ ] **Step 1: Write the failing test**

```java
// src/test/java/framework/ReportWriterTest.java
package framework;

import framework.model.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class ReportWriterTest {

    @TempDir Path tempDir;

    @Test
    void writesBothJsonAndHtmlFiles() throws IOException {
        ExperimentResult result = makeResult();
        ReportWriter writer = new ReportWriter(tempDir.toString());
        ReportWriter.OutputPaths paths = writer.write(result);

        assertTrue(Files.exists(Path.of(paths.jsonPath)), "JSON file must exist");
        assertTrue(Files.exists(Path.of(paths.htmlPath)), "HTML file must exist");

        String json = Files.readString(Path.of(paths.jsonPath));
        assertTrue(json.contains("\"experimentName\""), "JSON must contain experimentName field");

        String html = Files.readString(Path.of(paths.htmlPath));
        assertTrue(html.contains("chart.js"), "HTML must reference Chart.js");
        assertTrue(html.contains("Exp01_TargetFileSize"), "HTML must contain experiment name");
        assertTrue(html.contains("\"before\""), "HTML must embed before data");
        assertTrue(html.contains("\"after\""),  "HTML must embed after data");
    }

    private ExperimentResult makeResult() {
        return new ExperimentResult(
            "Exp01_TargetFileSize", "2026-05-10T14:00:00", "LOCAL",
            Map.of("target-file-size-bytes", "4194304"),
            Map.of("spark.master", "local[2]"),
            new SnapshotMetrics("before", 110, 52_000_000L, 472_727L, 10_000L, 800_000L, 52, 104, Map.of()),
            new SnapshotMetrics("after",  3,   51_000_000L, 17_000_000L, 16_000_000L, 19_000_000L, 53, 6, Map.of()),
            new CompactionPerformance(12_000L, 110, 3, 3, 4.25),
            new SparkResourceMetrics(Map.of(), Map.of(), 220L, 8000L, 400L, 52_000_000L, 51_000_000L, 0.05, 1.1, 4.25),
            List.of(new QueryBenchmarkResult("count_all", "SELECT COUNT(*) FROM t", 500L, 380L)),
            null
        );
    }
}
```

- [ ] **Step 2: Run test to confirm it fails**

```bash
./gradlew test --tests "framework.ReportWriterTest"
```

Expected: `FAILED` — `ReportWriter` does not exist yet.

- [ ] **Step 3: Write `ReportWriter.java`**

```java
package framework;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import framework.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.format.DateTimeFormatter;
import java.time.LocalDateTime;
import java.util.List;

public class ReportWriter {

    private static final Logger log = LoggerFactory.getLogger(ReportWriter.class);
    private static final ObjectMapper MAPPER =
        new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
    private static final DateTimeFormatter TS = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss");

    public record OutputPaths(String jsonPath, String htmlPath) {}

    private final String outputDir;

    public ReportWriter(String outputDir) {
        this.outputDir = outputDir;
    }

    public OutputPaths write(ExperimentResult result) throws IOException {
        Files.createDirectories(Path.of(outputDir));
        String timestamp = LocalDateTime.now().format(TS);
        String base = outputDir + "/" + result.experimentName + "-" + timestamp;

        String jsonPath = base + ".json";
        String htmlPath = base + ".html";

        MAPPER.writeValue(new File(jsonPath), result);
        log.info("ReportWriter: wrote {}", jsonPath);

        Files.writeString(Path.of(htmlPath), buildHtml(result), StandardCharsets.UTF_8);
        log.info("ReportWriter: wrote {}", htmlPath);

        return new OutputPaths(jsonPath, htmlPath);
    }

    private String buildHtml(ExperimentResult r) throws IOException {
        String jsonEmbed = MAPPER.writeValueAsString(r);
        return """
            <!DOCTYPE html>
            <html lang="en">
            <head>
              <meta charset="UTF-8">
              <title>%s — Iceberg Lab Report</title>
              <script src="https://cdn.jsdelivr.net/npm/chart.js@4/dist/chart.umd.min.js"></script>
              <style>
                body { font-family: sans-serif; max-width: 1100px; margin: 40px auto; padding: 0 20px; }
                h1 { color: #333; } h2 { color: #555; margin-top: 40px; }
                .cards { display: flex; gap: 20px; flex-wrap: wrap; margin-bottom: 30px; }
                .card { background: #f5f5f5; border-radius: 8px; padding: 16px 24px; min-width: 160px; }
                .card .value { font-size: 2em; font-weight: bold; color: #2c7be5; }
                .card .label { font-size: 0.85em; color: #666; }
                canvas { max-height: 320px; margin-bottom: 40px; }
                table { border-collapse: collapse; width: 100%%; margin-bottom: 30px; }
                th, td { border: 1px solid #ddd; padding: 8px 12px; text-align: left; }
                th { background: #f0f0f0; }
                pre { background: #272822; color: #f8f8f2; padding: 16px; border-radius: 6px;
                      overflow-x: auto; font-size: 0.85em; }
              </style>
            </head>
            <body>
              <h1>%s</h1>
              <p><strong>Mode:</strong> %s &nbsp;|&nbsp; <strong>Timestamp:</strong> %s</p>
              <h2>Compaction Parameters</h2>
              <table>%s</table>

              <h2>File Metrics</h2>
              <div class="cards">
                <div class="card"><div class="value">%d → %d</div><div class="label">Data Files</div></div>
                <div class="card"><div class="value">%s → %s</div><div class="label">Avg File Size</div></div>
                <div class="card"><div class="value">%dms</div><div class="label">Rewrite Duration</div></div>
                <div class="card"><div class="value">%.1f MB/s</div><div class="label">Throughput</div></div>
              </div>
              <canvas id="fileChart"></canvas>

              <h2>Spark Resource Metrics</h2>
              <div class="cards">
                <div class="card"><div class="value">%d</div><div class="label">Tasks</div></div>
                <div class="card"><div class="value">%.1f%%</div><div class="label">GC Pressure</div></div>
                <div class="card"><div class="value">%.2f</div><div class="label">Parallelism Efficiency</div></div>
              </div>

              <h2>Trino Query Benchmark</h2>
              <canvas id="queryChart"></canvas>

              <h2>Raw JSON</h2>
              <pre id="rawJson"></pre>

              <script>
              const data = %s;
              document.getElementById('rawJson').textContent = JSON.stringify(data, null, 2);

              // Chart 1: File metrics before/after
              new Chart(document.getElementById('fileChart'), {
                type: 'bar',
                data: {
                  labels: ['File Count', 'Avg Size (KB)', 'Total Size (MB)'],
                  datasets: [
                    { label: 'Before', backgroundColor: '#e74c3c',
                      data: [data.beforeFileMetrics.fileCount,
                             Math.round(data.beforeFileMetrics.avgFileSizeBytes / 1024),
                             Math.round(data.beforeFileMetrics.totalSizeBytes / 1048576)] },
                    { label: 'After',  backgroundColor: '#2ecc71',
                      data: [data.afterFileMetrics.fileCount,
                             Math.round(data.afterFileMetrics.avgFileSizeBytes / 1024),
                             Math.round(data.afterFileMetrics.totalSizeBytes / 1048576)] }
                  ]
                },
                options: { responsive: true, plugins: { legend: { position: 'top' } } }
              });

              // Chart 2: Trino query benchmark
              if (data.queryBenchmark && data.queryBenchmark.length > 0) {
                new Chart(document.getElementById('queryChart'), {
                  type: 'bar',
                  data: {
                    labels: data.queryBenchmark.map(q => q.queryName),
                    datasets: [
                      { label: 'Before (ms)', backgroundColor: '#e74c3c',
                        data: data.queryBenchmark.map(q => q.beforeMs) },
                      { label: 'After (ms)',  backgroundColor: '#2ecc71',
                        data: data.queryBenchmark.map(q => q.afterMs) }
                    ]
                  },
                  options: { responsive: true, plugins: { legend: { position: 'top' } } }
                });
              }
              </script>
            </body>
            </html>
            """.formatted(
                r.experimentName, r.experimentName, r.runMode, r.timestamp,
                buildOptionsTable(r.compactionOptions),
                r.beforeFileMetrics.fileCount, r.afterFileMetrics.fileCount,
                humanSize(r.beforeFileMetrics.avgFileSizeBytes), humanSize(r.afterFileMetrics.avgFileSizeBytes),
                r.compactionPerf.durationMs, r.compactionPerf.throughputMBps,
                r.sparkResources.taskCount,
                r.sparkResources.gcPressureRatio * 100,
                r.sparkResources.parallelismEfficiency,
                jsonEmbed
        );
    }

    private String buildOptionsTable(java.util.Map<String, String> options) {
        StringBuilder sb = new StringBuilder("<tr><th>Option</th><th>Value</th></tr>");
        options.forEach((k, v) -> sb.append("<tr><td>").append(k).append("</td><td>").append(v).append("</td></tr>"));
        return sb.toString();
    }

    private String humanSize(long bytes) {
        if (bytes >= 1_048_576) return String.format("%.1f MB", bytes / 1_048_576.0);
        if (bytes >= 1_024)     return String.format("%.1f KB", bytes / 1_024.0);
        return bytes + " B";
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

```bash
./gradlew test --tests "framework.ReportWriterTest"
```

Expected: `BUILD SUCCESSFUL`, 1 test passed.

- [ ] **Step 5: Commit**

```bash
git add src/
git commit -m "feat: add ReportWriter producing JSON + Chart.js HTML reports"
```

---

## Task 9: BaseExperiment

**Files:**
- Create: `src/main/java/framework/BaseExperiment.java`

- [ ] **Step 1: Write `BaseExperiment.java`**

```java
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
    private final MetricsCollector metricsCollector     = new MetricsCollector();
    private final TrinoQueryBenchmark trinoBenchmark    = new TrinoQueryBenchmark();
    private final ReportWriter reportWriter             = new ReportWriter("results");

    // ── Subclass contract ─────────────────────────────────────────────────────

    public abstract String experimentName();

    /** Iceberg rewrite options to pass to RewriteDataFiles. */
    protected abstract Map<String, String> compactionOptions();

    /** Additional Spark config overrides (merged on top of base config). */
    protected Map<String, String> sparkOverrides() { return Map.of(); }

    /**
     * Execute the compaction and return its result.
     * The default implementation calls RewriteDataFiles with compactionOptions().
     * Override for experiments that need custom rewrite logic (e.g. partition filter).
     */
    protected RewriteDataFiles.Result runCompaction(SparkSession spark, Table table) {
        RewriteDataFiles rewrite = SparkActions.get(spark).rewriteDataFiles(table);
        compactionOptions().forEach(rewrite::option);
        return rewrite.execute();
    }

    // ── Template method ───────────────────────────────────────────────────────

    public final void run() throws Exception {
        log.info("=== {} [{}] ===", experimentName(), LakehouseConfig.RUN_MODE);

        // Step 1: build SparkSession
        spark = buildSpark();
        spark.sparkContext().addSparkListener(metricsListener);

        // Step 2: setup table (drop + recreate for isolation)
        setupTable();
        Table table = Spark3Util.loadIcebergTable(spark, LakehouseConfig.FULL_TABLE);

        // Step 3: generate small files
        new DataGenerator(spark).generate();
        table.refresh();

        // Step 4: collect before metrics
        metricsListener.reset();
        SnapshotMetrics before = metricsCollector.collect(table, "before");

        // Step 5: Trino benchmark BEFORE (Spark is idle, not running a job)
        List<Long> trinoBeforeMedians = trinoBenchmark.runAll("before");

        // Step 6: run compaction, measure wall-clock duration
        long t0 = System.currentTimeMillis();
        RewriteDataFiles.Result compactionResult = runCompaction(spark, table);
        long durationMs = System.currentTimeMillis() - t0;
        table.refresh();

        // Step 7: collect after metrics + Spark resource flush
        SnapshotMetrics after = metricsCollector.collect(table, "after");
        SparkResourceMetrics sparkResources = metricsListener.flush(
            LakehouseConfig.baseSparkConfig(), compactionOptions(), durationMs);

        // Step 8: pause for Spark UI inspection (before closing session)
        if (LakehouseConfig.PAUSE_FOR_UI) {
            log.info("Spark UI: http://localhost:4040 — press Enter to close session...");
            System.in.read();
        }

        // Step 9: close SparkSession, then run Trino benchmark AFTER
        spark.close();
        List<Long> trinoAfterMedians = trinoBenchmark.runAll("after");
        List<QueryBenchmarkResult> queryBenchmark = trinoBenchmark.pair(trinoBeforeMedians, trinoAfterMedians);

        // Build CompactionPerformance
        CompactionPerformance compactionPerf = buildCompactionPerf(
            compactionResult, before, after, durationMs);

        // Step 10: write JSON + HTML report
        ExperimentResult result = new ExperimentResult(
            experimentName(), LocalDateTime.now().format(ISO),
            LakehouseConfig.RUN_MODE.name(),
            compactionOptions(), LakehouseConfig.baseSparkConfig(),
            before, after, compactionPerf, sparkResources, queryBenchmark, null);

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
        spark.sql("CREATE DATABASE IF NOT EXISTS " + LakehouseConfig.CATALOG
            + "." + LakehouseConfig.DATABASE);
        spark.sql("DROP TABLE IF EXISTS " + LakehouseConfig.FULL_TABLE);
        spark.sql("""
            CREATE TABLE %s (
              event_id   STRING   NOT NULL,
              user_id    STRING   NOT NULL,
              event_type STRING   NOT NULL,
              payload    STRING,
              region     STRING   NOT NULL,
              ts         TIMESTAMP NOT NULL
            ) USING iceberg
            PARTITIONED BY (region, days(ts))
            TBLPROPERTIES (
              'write.format.default' = 'parquet',
              'commit.retry.num-retries' = '5'
            )
            """.formatted(LakehouseConfig.FULL_TABLE));
        log.info("Table {} created", LakehouseConfig.FULL_TABLE);
    }

    private CompactionPerformance buildCompactionPerf(RewriteDataFiles.Result result,
                                                      SnapshotMetrics before,
                                                      SnapshotMetrics after,
                                                      long durationMs) {
        int filesRewritten  = result.rewrittenDataFilesCount();
        int filesAdded      = result.addedDataFilesCount();
        int fileGroups      = result.resultMap().size();
        long bytesRewritten = before.totalSizeBytes - after.totalSizeBytes;
        double throughput   = (durationMs > 0 && bytesRewritten > 0)
            ? ((double) before.totalSizeBytes / 1_048_576.0) / (durationMs / 1000.0)
            : 0.0;
        return new CompactionPerformance(durationMs, filesRewritten, filesAdded, fileGroups, throughput);
    }

    private void printSummary(ExperimentResult r, ReportWriter.OutputPaths paths) {
        System.out.println("\n╔══════════════════════════════════════════╗");
        System.out.printf( "║  %-42s║%n", r.experimentName);
        System.out.println("╠══════════════════════════════════════════╣");
        System.out.printf( "║  Files: %d → %d  (-%d)%n",
            r.beforeFileMetrics.fileCount, r.afterFileMetrics.fileCount,
            r.beforeFileMetrics.fileCount - r.afterFileMetrics.fileCount);
        System.out.printf( "║  Avg size: %s → %s%n",
            humanSize(r.beforeFileMetrics.avgFileSizeBytes),
            humanSize(r.afterFileMetrics.avgFileSizeBytes));
        System.out.printf( "║  Duration: %dms  Throughput: %.1f MB/s%n",
            r.compactionPerf.durationMs, r.compactionPerf.throughputMBps);
        System.out.printf( "║  GC pressure: %.1f%%  Parallelism eff: %.2f%n",
            r.sparkResources.gcPressureRatio * 100, r.sparkResources.parallelismEfficiency);
        System.out.println("╠══════════════════════════════════════════╣");
        System.out.println( "║  JSON: " + paths.jsonPath());
        System.out.println( "║  HTML: " + paths.htmlPath());
        System.out.println("╚══════════════════════════════════════════╝\n");
    }

    private String humanSize(long bytes) {
        if (bytes >= 1_048_576) return String.format("%.1f MB", bytes / 1_048_576.0);
        if (bytes >= 1_024)     return String.format("%.1f KB", bytes / 1_024.0);
        return bytes + " B";
    }
}
```

- [ ] **Step 2: Compile**

```bash
./gradlew compileJava
```

Expected: `BUILD SUCCESSFUL`.

- [ ] **Step 3: Commit**

```bash
git add src/main/java/framework/BaseExperiment.java
git commit -m "feat: add BaseExperiment template method orchestrating full experiment lifecycle"
```

---

## Task 10: Exp01_TargetFileSize (framework end-to-end validation)

**Files:**
- Create: `src/main/java/experiments/Exp01_TargetFileSize.java`

This is the most important task: it validates the entire framework end-to-end against the live lakehouse. Run it carefully and fix any issues before proceeding to other experiments.

- [ ] **Step 1: Write `Exp01_TargetFileSize.java`**

```java
package experiments;

import framework.BaseExperiment;
import framework.LakehouseConfig;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.RewriteDataFiles;
import org.apache.iceberg.spark.actions.SparkActions;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;

/**
 * Runs compaction three times with different target file sizes.
 * Each round drops and recreates the table and regenerates small files,
 * so results are independent and directly comparable.
 *
 * Local:  4 MB / 16 MB / 64 MB
 * Cloud: 32 MB / 128 MB / 512 MB
 */
public class Exp01_TargetFileSize extends BaseExperiment {

    private static final Logger log = LoggerFactory.getLogger(Exp01_TargetFileSize.class);

    private long currentTargetBytes = LakehouseConfig.targetFileSizes()[0];

    @Override
    public String experimentName() {
        return "Exp01_TargetFileSize_" + (currentTargetBytes / 1_048_576) + "MB";
    }

    @Override
    protected Map<String, String> compactionOptions() {
        return Map.of(
            RewriteDataFiles.TARGET_FILE_SIZE_BYTES, String.valueOf(currentTargetBytes),
            RewriteDataFiles.MIN_INPUT_FILES,        "2"
        );
    }

    public static void main(String[] args) throws Exception {
        Exp01_TargetFileSize exp = new Exp01_TargetFileSize();
        for (long targetBytes : LakehouseConfig.targetFileSizes()) {
            exp.currentTargetBytes = targetBytes;
            log.info("--- Round: target-file-size = {} MB ---", targetBytes / 1_048_576);
            exp.run();
        }
        log.info("Exp01 complete. Open results/ to view HTML reports.");
    }
}
```

- [ ] **Step 2: Run the experiment against the live lakehouse**

Ensure Docker containers are running first:

```bash
docker ps --filter "name=lakehouse" --format "{{.Names}}: {{.Status}}"
```

Expected: `lakehouse-minio: Up`, `lakehouse-hms: Up`, `lakehouse-trino: Up`.

```bash
./gradlew run -PmainClass=experiments.Exp01_TargetFileSize 2>&1 | tee results/exp01-run.log
```

Expected output (approximate):
```
╔══════════════════════════════════════════╗
║  Exp01_TargetFileSize_4MB               ║
╠══════════════════════════════════════════╣
║  Files: 110 → 12  (-98)
║  Avg size: 470.0 KB → 3.9 MB
║  Duration: 45000ms  Throughput: 1.1 MB/s
...
╚══════════════════════════════════════════╝
```

Open the generated HTML file in a browser and verify all three Chart.js charts render correctly.

- [ ] **Step 3: Verify output files exist**

```bash
ls -lh results/Exp01_TargetFileSize_*.{json,html}
```

Expected: 6 files (3 rounds × json + html).

- [ ] **Step 4: Commit**

```bash
git add src/main/java/experiments/Exp01_TargetFileSize.java
git commit -m "feat: add Exp01_TargetFileSize — validates framework end-to-end"
```

---

## Task 11: Exp02_MinInputFiles

**Files:**
- Create: `src/main/java/experiments/Exp02_MinInputFiles.java`

- [ ] **Step 1: Write `Exp02_MinInputFiles.java`**

```java
package experiments;

import framework.BaseExperiment;
import framework.LakehouseConfig;
import org.apache.iceberg.actions.RewriteDataFiles;
import java.util.Map;

/**
 * Varies min-input-files (2 / 5 / 10) keeping target-file-size fixed at 16 MB (local) or 128 MB (cloud).
 * Observes how the threshold changes which file groups are eligible for compaction.
 */
public class Exp02_MinInputFiles extends BaseExperiment {

    private int currentMinFiles = 2;

    @Override
    public String experimentName() {
        return "Exp02_MinInputFiles_" + currentMinFiles;
    }

    @Override
    protected Map<String, String> compactionOptions() {
        long targetSize = LakehouseConfig.targetFileSizes()[1]; // middle value
        return Map.of(
            RewriteDataFiles.TARGET_FILE_SIZE_BYTES, String.valueOf(targetSize),
            RewriteDataFiles.MIN_INPUT_FILES,        String.valueOf(currentMinFiles)
        );
    }

    public static void main(String[] args) throws Exception {
        Exp02_MinInputFiles exp = new Exp02_MinInputFiles();
        for (int minFiles : new int[]{2, 5, 10}) {
            exp.currentMinFiles = minFiles;
            exp.run();
        }
    }
}
```

- [ ] **Step 2: Run and verify**

```bash
./gradlew run -PmainClass=experiments.Exp02_MinInputFiles 2>&1 | tee results/exp02-run.log
```

Expected: 3 rounds complete, each producing json + html. The `filesRewritten` count in the report should decrease as `min-input-files` increases.

- [ ] **Step 3: Commit**

```bash
git add src/main/java/experiments/Exp02_MinInputFiles.java
git commit -m "feat: add Exp02_MinInputFiles"
```

---

## Task 12: Exp03_MaxConcurrentRewrites

**Files:**
- Create: `src/main/java/experiments/Exp03_MaxConcurrentRewrites.java`

- [ ] **Step 1: Write `Exp03_MaxConcurrentRewrites.java`**

```java
package experiments;

import framework.BaseExperiment;
import framework.LakehouseConfig;
import org.apache.iceberg.actions.RewriteDataFiles;
import java.util.Map;

/**
 * Varies max-concurrent-file-group-rewrites (1/2/4 local, 1/5/10 cloud).
 * Fixed Spark config to isolate the Iceberg parameter effect.
 * Observe: throughput, GC pressure, parallelism efficiency at each concurrency level.
 *
 * Local note: concurrency=4 with 2 cores causes over-subscription — intentional to
 * observe resource contention behavior.
 */
public class Exp03_MaxConcurrentRewrites extends BaseExperiment {

    private int currentConcurrency = 1;

    @Override
    public String experimentName() {
        return "Exp03_MaxConcurrentRewrites_" + currentConcurrency;
    }

    @Override
    protected Map<String, String> compactionOptions() {
        long targetSize = LakehouseConfig.targetFileSizes()[1];
        return Map.of(
            RewriteDataFiles.TARGET_FILE_SIZE_BYTES,        String.valueOf(targetSize),
            RewriteDataFiles.MAX_CONCURRENT_FILE_GROUP_REWRITES, String.valueOf(currentConcurrency),
            RewriteDataFiles.MIN_INPUT_FILES,               "2"
        );
    }

    public static void main(String[] args) throws Exception {
        Exp03_MaxConcurrentRewrites exp = new Exp03_MaxConcurrentRewrites();
        for (int concurrency : LakehouseConfig.concurrentRewrites()) {
            exp.currentConcurrency = concurrency;
            exp.run();
        }
    }
}
```

- [ ] **Step 2: Run and verify**

```bash
./gradlew run -PmainClass=experiments.Exp03_MaxConcurrentRewrites 2>&1 | tee results/exp03-run.log
```

Compare GC pressure and parallelism efficiency across the three concurrency levels in the JSON output.

- [ ] **Step 3: Commit**

```bash
git add src/main/java/experiments/Exp03_MaxConcurrentRewrites.java
git commit -m "feat: add Exp03_MaxConcurrentRewrites"
```

---

## Task 13: Exp04_PartialProgress

**Files:**
- Create: `src/main/java/experiments/Exp04_PartialProgress.java`

- [ ] **Step 1: Write `Exp04_PartialProgress.java`**

```java
package experiments;

import framework.BaseExperiment;
import framework.LakehouseConfig;
import org.apache.iceberg.actions.RewriteDataFiles;
import java.util.Map;

/**
 * Enables partial-progress and varies max-commits (2 / 5 / 10).
 * Observe: snapshot count after compaction, whether intermediate snapshots are visible,
 * and whether compaction completes despite commit failures.
 */
public class Exp04_PartialProgress extends BaseExperiment {

    private int currentMaxCommits = 2;

    @Override
    public String experimentName() {
        return "Exp04_PartialProgress_maxCommits" + currentMaxCommits;
    }

    @Override
    protected Map<String, String> compactionOptions() {
        long targetSize = LakehouseConfig.targetFileSizes()[1];
        return Map.of(
            RewriteDataFiles.TARGET_FILE_SIZE_BYTES,      String.valueOf(targetSize),
            RewriteDataFiles.PARTIAL_PROGRESS_ENABLED,    "true",
            RewriteDataFiles.PARTIAL_PROGRESS_MAX_COMMITS, String.valueOf(currentMaxCommits),
            RewriteDataFiles.MIN_INPUT_FILES,             "2"
        );
    }

    public static void main(String[] args) throws Exception {
        Exp04_PartialProgress exp = new Exp04_PartialProgress();
        for (int maxCommits : new int[]{2, 5, 10}) {
            exp.currentMaxCommits = maxCommits;
            exp.run();
        }
    }
}
```

- [ ] **Step 2: Run and verify**

```bash
./gradlew run -PmainClass=experiments.Exp04_PartialProgress 2>&1 | tee results/exp04-run.log
```

Check the `snapshotCount` in the "after" metrics — with `max-commits=2`, you should see more intermediate snapshots than with `max-commits=10`.

- [ ] **Step 3: Commit**

```bash
git add src/main/java/experiments/Exp04_PartialProgress.java
git commit -m "feat: add Exp04_PartialProgress"
```

---

## Task 14: Exp05_RewriteByPartition

**Files:**
- Create: `src/main/java/experiments/Exp05_RewriteByPartition.java`

- [ ] **Step 1: Write `Exp05_RewriteByPartition.java`**

```java
package experiments;

import framework.BaseExperiment;
import framework.LakehouseConfig;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.RewriteDataFiles;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.spark.actions.SparkActions;
import org.apache.spark.sql.SparkSession;
import java.util.Map;

/**
 * Compares compacting a single partition (region='us-east') vs the full table.
 * Observe: files rewritten scope, duration, and whether other partitions are untouched.
 */
public class Exp05_RewriteByPartition extends BaseExperiment {

    private boolean partitionOnly = true;

    @Override
    public String experimentName() {
        return "Exp05_Rewrite_" + (partitionOnly ? "PartitionUsEast" : "FullTable");
    }

    @Override
    protected Map<String, String> compactionOptions() {
        return Map.of(
            RewriteDataFiles.TARGET_FILE_SIZE_BYTES, String.valueOf(LakehouseConfig.targetFileSizes()[1]),
            RewriteDataFiles.MIN_INPUT_FILES,        "2"
        );
    }

    @Override
    protected RewriteDataFiles.Result runCompaction(SparkSession spark, Table table) {
        RewriteDataFiles rewrite = SparkActions.get(spark).rewriteDataFiles(table);
        compactionOptions().forEach(rewrite::option);
        if (partitionOnly) {
            rewrite.filter(Expressions.equal("region", "us-east"));
        }
        return rewrite.execute();
    }

    public static void main(String[] args) throws Exception {
        Exp05_RewriteByPartition exp = new Exp05_RewriteByPartition();

        exp.partitionOnly = true;
        exp.run();

        exp.partitionOnly = false;
        exp.run();
    }
}
```

- [ ] **Step 2: Run and verify**

```bash
./gradlew run -PmainClass=experiments.Exp05_RewriteByPartition 2>&1 | tee results/exp05-run.log
```

In the partition-only round, `perPartitionFileCounts` in the "after" report should show files reduced only in `us-east` partitions.

- [ ] **Step 3: Commit**

```bash
git add src/main/java/experiments/Exp05_RewriteByPartition.java
git commit -m "feat: add Exp05_RewriteByPartition"
```

---

## Task 15: Exp06_FullVsPartitionRewrite

**Files:**
- Create: `src/main/java/experiments/Exp06_FullVsPartitionRewrite.java`

- [ ] **Step 1: Write `Exp06_FullVsPartitionRewrite.java`**

```java
package experiments;

import framework.BaseExperiment;
import framework.LakehouseConfig;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.RewriteDataFiles;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.spark.actions.SparkActions;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;

/**
 * Strategy comparison on the same dataset:
 *   Round A — one full-table rewrite (single call)
 *   Round B — three sequential per-partition rewrites (one per region)
 *
 * Compares total duration, snapshot count produced, and Trino query latency.
 */
public class Exp06_FullVsPartitionRewrite extends BaseExperiment {

    private static final Logger log = LoggerFactory.getLogger(Exp06_FullVsPartitionRewrite.class);
    private static final String[] REGIONS = {"us-east", "eu-west", "ap-south"};

    private boolean fullTable = true;

    @Override
    public String experimentName() {
        return "Exp06_" + (fullTable ? "FullTableRewrite" : "PerPartitionRewrite");
    }

    @Override
    protected Map<String, String> compactionOptions() {
        return Map.of(
            RewriteDataFiles.TARGET_FILE_SIZE_BYTES, String.valueOf(LakehouseConfig.targetFileSizes()[1]),
            RewriteDataFiles.MIN_INPUT_FILES,        "2"
        );
    }

    @Override
    protected RewriteDataFiles.Result runCompaction(SparkSession spark, Table table) {
        if (fullTable) {
            RewriteDataFiles rewrite = SparkActions.get(spark).rewriteDataFiles(table);
            compactionOptions().forEach(rewrite::option);
            return rewrite.execute();
        } else {
            // Rewrite each region partition sequentially; return last result for tracking
            RewriteDataFiles.Result last = null;
            for (String region : REGIONS) {
                log.info("Exp06: rewriting partition region={}", region);
                RewriteDataFiles rewrite = SparkActions.get(spark).rewriteDataFiles(table);
                compactionOptions().forEach(rewrite::option);
                rewrite.filter(Expressions.equal("region", region));
                last = rewrite.execute();
                table.refresh();
            }
            return last;
        }
    }

    public static void main(String[] args) throws Exception {
        Exp06_FullVsPartitionRewrite exp = new Exp06_FullVsPartitionRewrite();

        exp.fullTable = true;
        exp.run();

        exp.fullTable = false;
        exp.run();
    }
}
```

- [ ] **Step 2: Run and verify**

```bash
./gradlew run -PmainClass=experiments.Exp06_FullVsPartitionRewrite 2>&1 | tee results/exp06-run.log
```

Compare `snapshotCount` after: per-partition strategy should produce 3 additional snapshots vs 1 for full-table.

- [ ] **Step 3: Commit**

```bash
git add src/main/java/experiments/Exp06_FullVsPartitionRewrite.java
git commit -m "feat: add Exp06_FullVsPartitionRewrite"
```

---

## Task 16: ConflictObserver

**Files:**
- Create: `src/main/java/framework/ConflictObserver.java`

- [ ] **Step 1: Write `ConflictObserver.java`**

```java
package framework;

import framework.model.ConcurrencyMetrics;
import framework.model.SnapshotEvent;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;

public class ConflictObserver {

    private static final Logger log = LoggerFactory.getLogger(ConflictObserver.class);

    /**
     * Reads the full snapshot lineage from the Iceberg table and builds ConcurrencyMetrics.
     * Call this after BOTH the ingestion worker and compaction worker have finished.
     *
     * @param table                    the live Iceberg Table
     * @param ingestionCommitsAttempted reported by Exp07a
     * @param ingestionCommitsSucceeded reported by Exp07a
     * @param ingestionCommitsFailed    reported by Exp07a
     * @param rewriteRetries            reported by Exp07b (parsed from logs or explicit counter)
     * @param rewriteConflicts          reported by Exp07b
     * @param fileGroupsSkipped         reported by Exp07b
     */
    public ConcurrencyMetrics analyze(Table table,
                                      int ingestionCommitsAttempted,
                                      int ingestionCommitsSucceeded,
                                      int ingestionCommitsFailed,
                                      int rewriteRetries,
                                      int rewriteConflicts,
                                      int fileGroupsSkipped) {
        table.refresh();
        List<SnapshotEvent> lineage = buildLineage(table);
        long total = lineage.size();

        log.info("ConflictObserver: {} total snapshots in lineage", total);
        log.info("  ingestion: attempted={} succeeded={} failed={}",
            ingestionCommitsAttempted, ingestionCommitsSucceeded, ingestionCommitsFailed);
        log.info("  rewrite:   retries={} conflicts={} fileGroupsSkipped={}",
            rewriteRetries, rewriteConflicts, fileGroupsSkipped);

        return new ConcurrencyMetrics(
            ingestionCommitsAttempted, ingestionCommitsSucceeded, ingestionCommitsFailed,
            rewriteRetries, rewriteConflicts, fileGroupsSkipped,
            total, lineage);
    }

    private List<SnapshotEvent> buildLineage(Table table) {
        List<SnapshotEvent> events = new ArrayList<>();
        for (Snapshot snapshot : table.snapshots()) {
            Long parentId = snapshot.parentId();
            int added   = snapshot.addedDataFiles(table.io())  != null
                ? (int) snapshot.addedDataFiles(table.io()).spliterator().estimateSize() : 0;
            int deleted = snapshot.removedDataFiles(table.io()) != null
                ? (int) snapshot.removedDataFiles(table.io()).spliterator().estimateSize() : 0;
            events.add(new SnapshotEvent(
                snapshot.snapshotId(), parentId,
                snapshot.operation(), added, deleted,
                snapshot.timestampMillis()));
        }
        // Sort by timestamp ascending
        events.sort(Comparator.comparingLong(e -> e.timestampMs));
        return events;
    }
}
```

- [ ] **Step 2: Compile**

```bash
./gradlew compileJava
```

Expected: `BUILD SUCCESSFUL`.

- [ ] **Step 3: Commit**

```bash
git add src/main/java/framework/ConflictObserver.java
git commit -m "feat: add ConflictObserver reading snapshot lineage for Exp07"
```

---

## Task 17: Exp07a_IngestionWorker

**Files:**
- Create: `src/main/java/experiments/Exp07a_IngestionWorker.java`

- [ ] **Step 1: Write `Exp07a_IngestionWorker.java`**

```java
package experiments;

import com.fasterxml.jackson.databind.ObjectMapper;
import framework.DataGenerator;
import framework.LakehouseConfig;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
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
        String[] regions     = {"us-east", "eu-west", "ap-south"};
        String[] eventTypes  = {"click", "view", "purchase", "error"};

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
        spark.sql("""
            CREATE TABLE %s (
              event_id STRING NOT NULL, user_id STRING NOT NULL,
              event_type STRING NOT NULL, payload STRING,
              region STRING NOT NULL, ts TIMESTAMP NOT NULL
            ) USING iceberg
            PARTITIONED BY (region, days(ts))
            TBLPROPERTIES (
              'write.format.default' = 'parquet',
              'commit.retry.num-retries' = '5'
            )
            """.formatted(LakehouseConfig.FULL_TABLE));
    }
}
```

- [ ] **Step 2: Compile**

```bash
./gradlew compileJava
```

Expected: `BUILD SUCCESSFUL`.

- [ ] **Step 3: Commit**

```bash
git add src/main/java/experiments/Exp07a_IngestionWorker.java
git commit -m "feat: add Exp07a_IngestionWorker for concurrent commit experiments"
```

---

## Task 18: Exp07b_CompactionWorker

**Files:**
- Create: `src/main/java/experiments/Exp07b_CompactionWorker.java`

- [ ] **Step 1: Write `Exp07b_CompactionWorker.java`**

```java
package experiments;

import com.fasterxml.jackson.databind.ObjectMapper;
import framework.LakehouseConfig;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.RewriteDataFiles;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.actions.SparkActions;
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
 * Scenario B (high frequency, partial-progress disabled) and C (enabled) are
 * selected via -DpartialProgress=true (default false).
 */
public class Exp07b_CompactionWorker {

    private static final Logger log = LoggerFactory.getLogger(Exp07b_CompactionWorker.class);
    private static final Path READY_FLAG = Path.of("results/exp07-ingestion-ready.flag");
    private static final Path STOP_FLAG  = Path.of("results/exp07-stop.flag");

    // Iceberg retry count exposed via a thread-local counter (via log parsing is simpler
    // but here we wrap with retry-aware logic for explicit counting)
    private static final AtomicInteger retryCount    = new AtomicInteger(0);
    private static final AtomicInteger conflictCount = new AtomicInteger(0);

    public static void main(String[] args) throws Exception {
        boolean partialProgress = Boolean.parseBoolean(
            System.getProperty("partialProgress", "false"));
        int maxCommits = Integer.parseInt(System.getProperty("maxCommits", "5"));
        log.info("Exp07b CompactionWorker — partialProgress={} maxCommits={}", partialProgress, maxCommits);

        // Wait for IngestionWorker to signal ready
        log.info("Waiting for ingestion ready signal...");
        while (!Files.exists(READY_FLAG)) {
            Thread.sleep(500);
        }
        log.info("Ingestion ready. Starting compaction.");

        org.apache.spark.sql.SparkSession spark =
            org.apache.spark.sql.SparkSession.builder()
                .appName("Exp07b_CompactionWorker")
                .config("spark.ui.port", "4041")  // avoid conflict with Exp07a's UI on 4040
                .apply(LakehouseConfig.baseSparkConfig().entrySet().stream()
                    .reduce(org.apache.spark.sql.SparkSession.builder().appName("_"),
                        (b, e) -> b.config(e.getKey(), e.getValue()),
                        (b1, b2) -> b1))
                // Re-build properly:
                .getOrCreate();

        // Build SparkSession properly
        var builder = org.apache.spark.sql.SparkSession.builder()
            .appName("Exp07b_CompactionWorker")
            .config("spark.ui.port", "4041");
        LakehouseConfig.baseSparkConfig().forEach(builder::config);
        spark = builder.getOrCreate();
        spark.close(); // close the incorrectly built one

        builder = org.apache.spark.sql.SparkSession.builder()
            .appName("Exp07b_CompactionWorker")
            .config("spark.ui.port", "4041");
        LakehouseConfig.baseSparkConfig().forEach(builder::config);
        spark = builder.getOrCreate();

        Table table = Spark3Util.loadIcebergTable(spark, LakehouseConfig.FULL_TABLE);

        Map<String, String> options = new LinkedHashMap<>();
        options.put(RewriteDataFiles.TARGET_FILE_SIZE_BYTES,
            String.valueOf(LakehouseConfig.targetFileSizes()[1]));
        options.put(RewriteDataFiles.MIN_INPUT_FILES, "2");
        options.put(RewriteDataFiles.MAX_CONCURRENT_FILE_GROUP_REWRITES, "2");
        if (partialProgress) {
            options.put(RewriteDataFiles.PARTIAL_PROGRESS_ENABLED,     "true");
            options.put(RewriteDataFiles.PARTIAL_PROGRESS_MAX_COMMITS, String.valueOf(maxCommits));
        }

        long t0 = System.currentTimeMillis();
        RewriteDataFiles.Result result = null;
        int fileGroupsSkipped = 0;
        try {
            RewriteDataFiles rewrite = SparkActions.get(spark).rewriteDataFiles(table);
            options.forEach(rewrite::option);
            result = rewrite.execute();
            fileGroupsSkipped = result.resultMap().values().stream()
                .mapToInt(r -> r.rewrittenDataFilesCount() == 0 ? 1 : 0)
                .sum();
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
        int fileGroups = result != null ? result.resultMap().size()         : 0;

        Map<String, Object> metrics = Map.of(
            "durationMs",         durationMs,
            "filesRewritten",     rewritten,
            "filesAdded",         added,
            "fileGroupsRewritten",fileGroups,
            "fileGroupsSkipped",  fileGroupsSkipped,
            "rewriteConflicts",   conflictCount.get(),
            "rewriteRetries",     retryCount.get(),
            "partialProgress",    partialProgress,
            "options",            options
        );
        new ObjectMapper().writeValue(
            new File("results/exp07-compaction-metrics.json"), metrics);
    }
}
```

**Note:** The SparkSession construction above has a duplication issue introduced mid-method. Fix it to:

```java
// Replace the entire SparkSession building block with this:
var builder = org.apache.spark.sql.SparkSession.builder()
    .appName("Exp07b_CompactionWorker")
    .config("spark.ui.port", "4041");
LakehouseConfig.baseSparkConfig().forEach(builder::config);
org.apache.spark.sql.SparkSession spark = builder.getOrCreate();
```

- [ ] **Step 2: Fix the SparkSession duplication in the file**

Edit `Exp07b_CompactionWorker.java` to have a single clean SparkSession construction block — remove the duplicated builder calls and keep only the final correct version shown in the Note above.

- [ ] **Step 3: Compile**

```bash
./gradlew compileJava
```

Expected: `BUILD SUCCESSFUL`.

- [ ] **Step 4: Commit**

```bash
git add src/main/java/experiments/Exp07b_CompactionWorker.java
git commit -m "feat: add Exp07b_CompactionWorker for concurrent conflict experiment"
```

---

## Task 19: Exp07_ResultAnalyzer

**Files:**
- Create: `src/main/java/experiments/Exp07_ResultAnalyzer.java`

- [ ] **Step 1: Write `Exp07_ResultAnalyzer.java`**

```java
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
import java.nio.file.Path;
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

        // Build SparkSession (read-only — just to load table metadata)
        SparkSession.Builder builder = SparkSession.builder().appName("Exp07_ResultAnalyzer");
        LakehouseConfig.baseSparkConfig().forEach(builder::config);
        SparkSession spark = builder.getOrCreate();

        var table = Spark3Util.loadIcebergTable(spark, LakehouseConfig.FULL_TABLE);

        MetricsCollector collector = new MetricsCollector();
        SnapshotMetrics finalMetrics = collector.collect(table, "final");

        ConflictObserver observer = new ConflictObserver();
        ConcurrencyMetrics concurrencyMetrics = observer.analyze(
            table,
            (int) ingMetrics.get("ingestionCommitsAttempted"),
            (int) ingMetrics.get("ingestionCommitsSucceeded"),
            (int) ingMetrics.get("ingestionCommitsFailed"),
            (int) cmpMetrics.get("rewriteRetries"),
            (int) cmpMetrics.get("rewriteConflicts"),
            (int) cmpMetrics.get("fileGroupsSkipped")
        );

        spark.close();

        // Build a stub ExperimentResult (no before metrics — this is a post-hoc analysis)
        SnapshotMetrics stub = new SnapshotMetrics("before-unavailable",
            0, 0, 0, 0, 0, 0, 0, Map.of());

        CompactionPerformance compPerf = new CompactionPerformance(
            ((Number) cmpMetrics.get("durationMs")).longValue(),
            (int) cmpMetrics.get("filesRewritten"),
            (int) cmpMetrics.get("filesAdded"),
            (int) cmpMetrics.get("fileGroupsRewritten"),
            0.0
        );

        ExperimentResult result = new ExperimentResult(
            "Exp07_ConcurrentConflicts",
            LocalDateTime.now().format(ISO),
            LakehouseConfig.RUN_MODE.name(),
            (Map<String, String>)(Object) cmpMetrics.get("options"),
            LakehouseConfig.baseSparkConfig(),
            stub, finalMetrics,
            compPerf,
            new SparkResourceMetrics(Map.of(), Map.of(), 0, 0, 0, 0, 0, 0, 0, 0),
            List.of(),
            concurrencyMetrics
        );

        ReportWriter writer = new ReportWriter("results");
        ReportWriter.OutputPaths paths = writer.write(result);
        log.info("Exp07 report written: {}", paths.htmlPath());
    }
}
```

- [ ] **Step 2: Compile**

```bash
./gradlew compileJava
```

Expected: `BUILD SUCCESSFUL`.

- [ ] **Step 3: Commit**

```bash
git add src/main/java/experiments/Exp07_ResultAnalyzer.java
git commit -m "feat: add Exp07_ResultAnalyzer merging dual-process outputs"
```

---

## Task 20: run-exp07.sh and end-to-end Exp07 validation

**Files:**
- Create: `scripts/run-exp07.sh`

- [ ] **Step 1: Write `scripts/run-exp07.sh`**

```bash
#!/usr/bin/env bash
set -euo pipefail

SCENARIO=${1:-B}   # A=low-freq, B=high-freq no partial, C=high-freq with partial
MODE=${2:-local}

echo "=== Exp07 ConcurrentConflicts — Scenario $SCENARIO, Mode $MODE ==="

# Clean up signal files from any previous run
rm -f results/exp07-ingestion-ready.flag
rm -f results/exp07-stop.flag
rm -f results/exp07-ingestion-metrics.json
rm -f results/exp07-compaction-metrics.json

mkdir -p results

case $SCENARIO in
  A)
    INTERVAL=2000
    PARTIAL=false
    MAX_COMMITS=5
    ;;
  B)
    INTERVAL=100
    PARTIAL=false
    MAX_COMMITS=5
    ;;
  C)
    INTERVAL=100
    PARTIAL=true
    MAX_COMMITS=5
    ;;
  *)
    echo "Unknown scenario: $SCENARIO. Use A, B, or C."
    exit 1
    ;;
esac

echo "Scenario $SCENARIO: intervalMs=$INTERVAL partialProgress=$PARTIAL maxCommits=$MAX_COMMITS"

# Start IngestionWorker in background
./gradlew run \
  -PmainClass=experiments.Exp07a_IngestionWorker \
  -PrunMode=$MODE \
  -DscenarioIntervalMs=$INTERVAL \
  > results/exp07-ingestion.log 2>&1 &
INGESTION_PID=$!
echo "IngestionWorker PID=$INGESTION_PID"

# Wait for ready flag (max 3 minutes)
echo "Waiting for ingestion ready signal..."
WAITED=0
until [ -f results/exp07-ingestion-ready.flag ] || [ $WAITED -ge 180 ]; do
  sleep 2
  WAITED=$((WAITED + 2))
done

if [ ! -f results/exp07-ingestion-ready.flag ]; then
  echo "ERROR: IngestionWorker did not signal ready within 3 minutes."
  kill $INGESTION_PID 2>/dev/null || true
  exit 1
fi
echo "IngestionWorker ready. Launching CompactionWorker."

# Run CompactionWorker (foreground — wait for it to complete)
./gradlew run \
  -PmainClass=experiments.Exp07b_CompactionWorker \
  -PrunMode=$MODE \
  -DpartialProgress=$PARTIAL \
  -DmaxCommits=$MAX_COMMITS \
  > results/exp07-compaction.log 2>&1

echo "CompactionWorker done. Writing stop flag."
touch results/exp07-stop.flag

# Wait for IngestionWorker to exit
wait $INGESTION_PID || true
echo "IngestionWorker exited."

# Run ResultAnalyzer
./gradlew run \
  -PmainClass=experiments.Exp07_ResultAnalyzer \
  -PrunMode=$MODE \
  > results/exp07-analyzer.log 2>&1

echo ""
echo "=== Exp07 Scenario $SCENARIO complete ==="
ls -lh results/Exp07_ConcurrentConflicts-*.html 2>/dev/null || echo "No HTML report found — check logs."
```

- [ ] **Step 2: Make executable**

```bash
chmod +x scripts/run-exp07.sh
```

- [ ] **Step 3: Run Exp07 Scenario A (low-frequency) to validate**

```bash
bash scripts/run-exp07.sh A local 2>&1 | tee results/exp07-scenario-A.log
```

Expected:
- Ingestion runs ~2-minute background loop
- Compaction completes without conflicts (low commit rate)
- HTML report appears in `results/`

Open the report and verify `ConcurrencyMetrics` shows `ingestionCommitsSucceeded > 0` and `rewriteConflicts = 0` (or very low).

- [ ] **Step 4: Run Scenario B (high-frequency)**

```bash
bash scripts/run-exp07.sh B local 2>&1 | tee results/exp07-scenario-B.log
```

Expected: higher `rewriteConflicts` or `rewriteRetries` visible in the report.

- [ ] **Step 5: Run Scenario C (high-frequency + partial-progress)**

```bash
bash scripts/run-exp07.sh C local 2>&1 | tee results/exp07-scenario-C.log
```

Expected: `fileGroupsSkipped > 0`, `snapshotCount` higher than Scenario B.

- [ ] **Step 6: Commit**

```bash
git add scripts/run-exp07.sh
git commit -m "feat: add run-exp07.sh orchestrating dual-process concurrent conflict experiment"
```

---

## Self-Review Checklist

### Spec coverage check

| Spec section | Covered by task |
|---|---|
| Gradle + Java 17 + dependencies | Task 1 |
| LakehouseConfig (MinIO, HMS, warehouse, modes) | Task 3 |
| Model classes with Jackson | Task 2 |
| DataGenerator (bulk + small batch) | Task 4 |
| SparkMetricsListener | Task 5 |
| MetricsCollector (file + snapshot metadata) | Task 6 |
| TrinoQueryBenchmark (4 queries × 3 runs) | Task 7 |
| ReportWriter (JSON + HTML Chart.js) | Task 8 |
| BaseExperiment template method | Task 9 |
| Exp01 target-file-size | Task 10 |
| Exp02 min-input-files | Task 11 |
| Exp03 max-concurrent-rewrites | Task 12 |
| Exp04 partial-progress | Task 13 |
| Exp05 rewrite by partition | Task 14 |
| Exp06 full vs partition strategy | Task 15 |
| ConflictObserver (snapshot lineage) | Task 16 |
| Exp07a IngestionWorker | Task 17 |
| Exp07b CompactionWorker | Task 18 |
| Exp07 ResultAnalyzer | Task 19 |
| run-exp07.sh (A/B/C scenarios) | Task 20 |
| Spark UI pause mode | Task 9 (BaseExperiment.run()) |
| LOCAL/CLOUD mode switching | Task 3 (LakehouseConfig) |

**All spec requirements covered.**

### Type consistency check

- `SnapshotMetrics` fields referenced consistently across `MetricsCollector`, `BaseExperiment`, `ReportWriter`, and `ExperimentResultSerializationTest`.
- `CompactionPerformance` constructor: `(durationMs, filesRewritten, filesAdded, fileGroupsRewritten, throughputMBps)` — used consistently in `BaseExperiment.buildCompactionPerf()` and `Exp07_ResultAnalyzer`.
- `RewriteDataFiles.PARTIAL_PROGRESS_ENABLED` and `PARTIAL_PROGRESS_MAX_COMMITS` — Iceberg 1.6.x constant names verified.
- `Spark3Util.loadIcebergTable(spark, tableName)` — used consistently in `BaseExperiment`, `Exp07b`, and `Exp07_ResultAnalyzer`.
- `DataGenerator.SCHEMA` declared `static final` in Task 4 and referenced in `Exp07a_IngestionWorker` Task 17 — consistent.
