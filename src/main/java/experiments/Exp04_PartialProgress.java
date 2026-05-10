package experiments;

import framework.BaseExperiment;
import framework.LakehouseConfig;
import java.util.Map;

/**
 * Enables partial-progress and varies max-commits (2 / 5 / 10).
 * Observes: snapshot count after compaction, intermediate commit behaviour,
 * and whether compaction completes despite potential commit failures.
 */
public class Exp04_PartialProgress extends BaseExperiment {

    private int currentMaxCommits = 2;

    @Override
    public String experimentName() {
        return "Exp04_PartialProgress_maxCommits" + currentMaxCommits;
    }

    @Override
    protected Map<String, String> compactionOptions() {
        return Map.of(
            "target-file-size-bytes",       String.valueOf(LakehouseConfig.targetFileSizes()[1]),
            "partial-progress.enabled",     "true",
            "partial-progress.max-commits", String.valueOf(currentMaxCommits),
            "min-input-files",              "2"
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
