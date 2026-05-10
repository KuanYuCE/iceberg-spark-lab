package experiments;

import framework.BaseExperiment;
import framework.LakehouseConfig;
import java.util.Map;

/**
 * Varies max-concurrent-file-group-rewrites.
 * Local: 1 / 2 / 4   (4 intentionally over-subscribes 2 cores — observe GC pressure)
 * Cloud: 1 / 5 / 10
 *
 * Key metrics: throughput MB/s, GC pressure ratio, parallelism efficiency.
 */
public class Exp03_MaxConcurrentRewrites extends BaseExperiment {

    private int currentConcurrency = 1;

    @Override
    public String experimentName() {
        return "Exp03_MaxConcurrentRewrites_" + currentConcurrency;
    }

    @Override
    protected Map<String, String> compactionOptions() {
        return Map.of(
            "target-file-size-bytes",
                String.valueOf(LakehouseConfig.targetFileSizes()[1]),
            "max-concurrent-file-group-rewrites",
                String.valueOf(currentConcurrency),
            "min-input-files", "2"
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
