package framework;

import framework.model.QueryBenchmarkResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.*;
import java.util.*;

public class TrinoQueryBenchmark {

    private static final Logger log = LoggerFactory.getLogger(TrinoQueryBenchmark.class);
    private static final int RUNS = 3;

    // Each entry: { queryName, sql }
    private static final List<String[]> QUERIES = List.of(
        new String[]{"count_all",
            "SELECT COUNT(*) FROM " + LakehouseConfig.FULL_TABLE},
        new String[]{"group_by_region",
            "SELECT region, COUNT(*) FROM " + LakehouseConfig.FULL_TABLE + " GROUP BY region"},
        new String[]{"partition_prune",
            "SELECT COUNT(*) FROM " + LakehouseConfig.FULL_TABLE
            + " WHERE region = 'us-east' AND ts >= TIMESTAMP '2026-05-04 00:00:00'"},
        new String[]{"aggregate_payload",
            "SELECT event_type, AVG(LENGTH(payload)) FROM " + LakehouseConfig.FULL_TABLE
            + " GROUP BY event_type"}
    );

    /**
     * Runs all benchmark queries and returns median latency per query.
     * If Trino is unavailable, logs a warning and returns zeros (experiment continues).
     *
     * @param label "before" or "after" (used for logging only)
     * @return list of median latencies in ms, one per query, in QUERIES order
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
            log.warn("TrinoQueryBenchmark [{}]: Trino unavailable — {}. Returning zeros.", label, e.getMessage());
            for (int i = 0; i < QUERIES.size(); i++) medians.add(0L);
        }
        return medians;
    }

    /**
     * Pairs before and after median latency lists into {@link QueryBenchmarkResult} records.
     */
    public List<QueryBenchmarkResult> pair(List<Long> beforeMedians, List<Long> afterMedians) {
        List<QueryBenchmarkResult> results = new ArrayList<>();
        for (int i = 0; i < QUERIES.size(); i++) {
            results.add(new QueryBenchmarkResult(
                QUERIES.get(i)[0],
                QUERIES.get(i)[1],
                beforeMedians.get(i),
                afterMedians.get(i)));
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
                while (rs.next()) { /* consume all rows */ }
            }
            times[i] = System.currentTimeMillis() - start;
        }
        Arrays.sort(times);
        long median = times[RUNS / 2];
        log.info("TrinoQueryBenchmark [{}] {}: {}ms (median of {} runs)",
            label, name, median, RUNS);
        return median;
    }

    private Connection openConnection() throws SQLException {
        Properties props = new Properties();
        props.setProperty("user", LakehouseConfig.TRINO_USER);
        return DriverManager.getConnection(LakehouseConfig.TRINO_JDBC, props);
    }
}
