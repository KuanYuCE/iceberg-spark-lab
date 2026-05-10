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
 *   Round A — one full-table rewrite (single call, one new snapshot)
 *   Round B — three sequential per-partition rewrites (one per region, three new snapshots)
 *
 * Observe: total duration, snapshot count produced, per-partition file isolation,
 * and Trino query latency before vs after each strategy.
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
            "target-file-size-bytes", String.valueOf(LakehouseConfig.targetFileSizes()[1]),
            "min-input-files",        "2"
        );
    }

    @Override
    protected RewriteDataFiles.Result runCompaction(SparkSession spark, Table table) {
        if (fullTable) {
            RewriteDataFiles rewrite = SparkActions.get(spark).rewriteDataFiles(table);
            compactionOptions().forEach(rewrite::option);
            return rewrite.execute();
        }
        // Sequential per-partition rewrite; return the last result for metrics
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

    public static void main(String[] args) throws Exception {
        Exp06_FullVsPartitionRewrite exp = new Exp06_FullVsPartitionRewrite();

        exp.fullTable = true;
        exp.run();

        exp.fullTable = false;
        exp.run();
    }
}
