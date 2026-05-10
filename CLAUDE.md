# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Installed Plugins

- `superpowers@claude-plugins-official` — provides skills for planning, TDD, debugging, code review, and other structured workflows

## Build & Run

```bash
# Compile
./gradlew build

# Run tests
./gradlew test

# Run a single test class
./gradlew test --tests "framework.ReportWriterTest"

# Run a specific experiment (default: Exp01)
./gradlew run -PmainClass=experiments.Exp01_TargetFileSize

# Run in cloud mode (larger data sizes, more parallelism)
./gradlew run -PmainClass=experiments.Exp02_MinInputFiles -PrunMode=cloud

# Pause after compaction to inspect Spark UI at http://localhost:4040
./gradlew run -PmainClass=experiments.Exp01_TargetFileSize -PpauseForUi=true

# Exp07 concurrent conflict scenarios (A=low-freq, B=high-freq, C=high-freq+partialProgress)
./scripts/run-exp07.sh B local
```

Supported `-P` flags passed as system properties via the `run` task: `mainClass`, `runMode`, `pauseForUi`, `scenarioIntervalMs`, `partialProgress`, `maxCommits`.

## Infrastructure Dependencies

All experiments require these services running locally:
- **MinIO** at `http://localhost:9000` (credentials: `minioadmin`/`minioadmin`, bucket `my-raw`)
- **Hive Metastore** at `thrift://localhost:9083`
- **Trino** at `http://localhost:8080` (for query benchmarks; optional — benchmark is skipped if unavailable)

Results (JSON + HTML with Chart.js charts) are written to `results/`.

## Architecture

### Framework (template method pattern)

`BaseExperiment` defines the fixed experiment lifecycle in `run()`:
1. Build SparkSession (merging `LakehouseConfig.baseSparkConfig()` with `sparkOverrides()`)
2. Drop + recreate the Iceberg table for isolation
3. Generate small files via `DataGenerator`
4. Collect `SnapshotMetrics` before compaction; run Trino benchmarks while Spark is idle
5. Call `runCompaction()` — default uses `RewriteDataFiles` with `compactionOptions()`
6. Collect after metrics + flush `SparkMetricsListener`
7. Close Spark, re-run Trino benchmarks (no CPU contention)
8. Build `ExperimentResult` and write JSON + HTML via `ReportWriter`

Experiments Exp01–Exp06 subclass `BaseExperiment` and only need to implement `experimentName()` and `compactionOptions()`. Override `runCompaction()` for custom logic (e.g. Exp05 adds a partition filter).

### Exp07 — two-process concurrency test

Exp07 does **not** use `BaseExperiment`. It runs as two separate JVM processes coordinated by flag files in `results/`:
- `Exp07a_IngestionWorker` — creates the table, generates data, signals `exp07-ingestion-ready.flag`, then loops appending small batches until `exp07-stop.flag` appears
- `Exp07b_CompactionWorker` — waits for the ready flag, runs repeated compaction passes, writes `exp07-stop.flag` when done
- `Exp07_ResultAnalyzer` — reads both workers' metric JSON files and writes the final HTML report

Use `scripts/run-exp07.sh` to orchestrate; it handles process lifecycle, flag cleanup, and log capture.

### Configuration

`LakehouseConfig` is the single source of truth for all connection strings, Spark settings, and experiment parameter ranges. Run mode (`LOCAL`/`CLOUD`) is read from the `runMode` system property at startup. LOCAL uses `local[2]` with 1 GB driver memory and smaller data sizes; CLOUD uses `local[8]` with 8 GB and production-scale sizes.

### Model

`framework/model/` contains plain Java records: `ExperimentResult` (top-level), `CompactionPerformance`, `SnapshotMetrics`, `SparkResourceMetrics`, `QueryBenchmarkResult`, `ConcurrencyMetrics`, `SnapshotEvent`. These are serialized to JSON by Jackson.

### Key dependency notes

- Jackson is forced to `2.15.2` project-wide to match Spark 3.5's bundled version (conflict resolution in `build.gradle`).
- Hive Metastore deps (`hive-exec`, `hive-metastore`) require several excludes to avoid classpath conflicts with Spark's bundled libraries.
- Java 17 with many `--add-opens` flags is required for Spark to run in local mode.
