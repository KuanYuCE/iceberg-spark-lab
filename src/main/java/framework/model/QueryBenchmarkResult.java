package framework.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class QueryBenchmarkResult {
    public final String queryName;
    public final String sql;
    public final long beforeMs;
    public final long afterMs;
    public final double improvementPct;

    @JsonCreator
    public QueryBenchmarkResult(
            @JsonProperty("queryName")      String queryName,
            @JsonProperty("sql")            String sql,
            @JsonProperty("beforeMs")       long beforeMs,
            @JsonProperty("afterMs")        long afterMs,
            @JsonProperty("improvementPct") double improvementPct) {
        this.queryName = queryName;
        this.sql = sql;
        this.beforeMs = beforeMs;
        this.afterMs = afterMs;
        this.improvementPct = improvementPct;
    }

    /** Convenience constructor that calculates improvementPct automatically. */
    public QueryBenchmarkResult(String queryName, String sql, long beforeMs, long afterMs) {
        this(queryName, sql, beforeMs, afterMs,
            beforeMs > 0 ? ((double)(beforeMs - afterMs) / beforeMs) * 100.0 : 0.0);
    }
}
