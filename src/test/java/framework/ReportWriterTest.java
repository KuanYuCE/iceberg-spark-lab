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

        assertTrue(Files.exists(Path.of(paths.jsonPath())), "JSON file must exist");
        assertTrue(Files.exists(Path.of(paths.htmlPath())), "HTML file must exist");
    }

    @Test
    void jsonContainsExperimentName() throws IOException {
        String json = Files.readString(Path.of(
            new ReportWriter(tempDir.toString()).write(makeResult()).jsonPath()));
        assertTrue(json.contains("\"experimentName\""));
        assertTrue(json.contains("Exp01_TargetFileSize_4MB"));
    }

    @Test
    void htmlContainsChartJsAndExperimentData() throws IOException {
        String html = Files.readString(Path.of(
            new ReportWriter(tempDir.toString()).write(makeResult()).htmlPath()));
        assertTrue(html.contains("chart.js"),              "must reference Chart.js CDN");
        assertTrue(html.contains("Exp01_TargetFileSize_4MB"), "must contain experiment name");
        assertTrue(html.contains("\"before\""),            "must embed before metrics");
        assertTrue(html.contains("\"after\""),             "must embed after metrics");
        assertTrue(html.contains("fileChart"),             "must have file metrics chart canvas");
        assertTrue(html.contains("queryChart"),            "must have query benchmark chart canvas");
    }

    @Test
    void nullConcurrencyMetricsProducesNoConcurrencySection() throws IOException {
        String html = Files.readString(Path.of(
            new ReportWriter(tempDir.toString()).write(makeResult()).htmlPath()));
        assertFalse(html.contains("Concurrency Metrics"),
            "concurrency section must not appear when concurrencyMetrics is null");
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private ExperimentResult makeResult() {
        return new ExperimentResult(
            "Exp01_TargetFileSize_4MB", "2026-05-10T14:00:00", "LOCAL",
            Map.of("target-file-size-bytes", "4194304"),
            Map.of("spark.master", "local[2]"),
            makeSnapshotMetrics("before"),
            makeSnapshotMetrics("after"),
            new CompactionPerformance(12_000L, 110, 3, 3, 4.25),
            new SparkResourceMetrics(Map.of(), Map.of(),
                220L, 8_000L, 400L, 52_000_000L, 51_000_000L, 0.05, 1.1, 4.25),
            List.of(new QueryBenchmarkResult("count_all",
                "SELECT COUNT(*) FROM iceberg.iceberg_lab.events", 500L, 380L)),
            null);
    }

    private SnapshotMetrics makeSnapshotMetrics(String label) {
        boolean isBefore = "before".equals(label);
        return new SnapshotMetrics(label,
            isBefore ? 110 : 3,
            52_000_000L,
            isBefore ? 472_727L : 17_333_333L,
            isBefore ? 10_000L  : 16_000_000L,
            isBefore ? 800_000L : 19_000_000L,
            isBefore ? 52 : 53,
            isBefore ? 104 : 6,
            Map.of());
    }
}
