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
 * Compares two strategies on the same dataset:
 *   Round A — partition-scoped rewrite (region = 'us-east' only)
 *   Round B — full-table rewrite
 *
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
            "target-file-size-bytes", String.valueOf(LakehouseConfig.targetFileSizes()[1]),
            "min-input-files",        "2"
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
