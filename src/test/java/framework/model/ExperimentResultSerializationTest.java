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
        ExperimentResult original = makeResult();

        String json = mapper.writeValueAsString(original);
        ExperimentResult parsed = mapper.readValue(json, ExperimentResult.class);

        assertEquals("Exp01_TargetFileSize_4MB", parsed.experimentName);
        assertEquals("LOCAL", parsed.runMode);
        assertEquals(110, parsed.beforeFileMetrics.fileCount);
        assertEquals(3, parsed.afterFileMetrics.fileCount);
        assertEquals(12_345L, parsed.compactionPerf.durationMs);
        assertEquals(110, parsed.compactionPerf.filesRewritten);
        assertEquals(220L, parsed.sparkResources.taskCount);
        assertEquals(1, parsed.queryBenchmark.size());
        assertEquals("count_all", parsed.queryBenchmark.get(0).queryName);
        assertTrue(parsed.queryBenchmark.get(0).improvementPct > 0,
            "improvementPct should be positive when afterMs < beforeMs");
        assertNull(parsed.concurrencyMetrics, "concurrencyMetrics should be null for Exp01");
    }

    @Test
    void nullConcurrencyMetricsIsOmittedFromJson() throws Exception {
        String json = mapper.writeValueAsString(makeResult());
        assertFalse(json.contains("concurrencyMetrics"),
            "null concurrencyMetrics must be omitted from JSON due to @JsonInclude(NON_NULL)");
    }

    @Test
    void concurrencyMetricsRoundTrips() throws Exception {
        ConcurrencyMetrics cm = new ConcurrencyMetrics(
            100, 98, 2, 3, 1, 2, 55L,
            List.of(new SnapshotEvent(1L, null, "append", 5, 0, 1_000L)));
        ExperimentResult withConc = new ExperimentResult(
            "Exp07", "2026-05-10T14:00:00", "LOCAL",
            Map.of(), Map.of(),
            makeSnapshotMetrics("before"), makeSnapshotMetrics("after"),
            new CompactionPerformance(5_000L, 50, 2, 2, 2.0),
            new SparkResourceMetrics(Map.of(), Map.of(), 100L, 4000L, 200L, 10_000L, 9_000L, 0.05, 0.9, 2.0),
            List.of(), cm);

        String json = mapper.writeValueAsString(withConc);
        ExperimentResult parsed = mapper.readValue(json, ExperimentResult.class);

        assertNotNull(parsed.concurrencyMetrics);
        assertEquals(100, parsed.concurrencyMetrics.ingestionCommitsAttempted);
        assertEquals(1, parsed.concurrencyMetrics.snapshotLineage.size());
        assertNull(parsed.concurrencyMetrics.snapshotLineage.get(0).parentSnapshotId);
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private ExperimentResult makeResult() {
        return new ExperimentResult(
            "Exp01_TargetFileSize_4MB", "2026-05-10T14:00:00", "LOCAL",
            Map.of("target-file-size-bytes", "4194304"),
            Map.of("spark.master", "local[2]"),
            makeSnapshotMetrics("before"),
            makeSnapshotMetrics("after"),
            new CompactionPerformance(12_345L, 110, 3, 3, 4.2),
            new SparkResourceMetrics(
                Map.of("spark.master", "local[2]"),
                Map.of("target-file-size-bytes", "4194304"),
                220L, 8_000L, 400L, 52_428_800L, 52_000_000L, 0.05, 1.1, 4.2),
            List.of(new QueryBenchmarkResult("count_all",
                "SELECT COUNT(*) FROM iceberg.iceberg_lab.events", 520L, 380L)),
            null);
    }

    private SnapshotMetrics makeSnapshotMetrics(String label) {
        boolean isBefore = "before".equals(label);
        return new SnapshotMetrics(label,
            isBefore ? 110 : 3,
            52_428_800L,
            isBefore ? 476_625L : 17_476_266L,
            isBefore ? 10_000L : 16_000_000L,
            isBefore ? 900_000L : 20_000_000L,
            isBefore ? 52 : 53,
            isBefore ? 104 : 6,
            Map.of("region=us-east/ts_day=2026-05-08", isBefore ? 40L : 1L));
    }
}
