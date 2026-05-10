package experiments;

import framework.BaseExperiment;
import framework.LakehouseConfig;
import java.util.Map;

/**
 * Varies min-input-files (2 / 5 / 10) with a fixed target file size.
 * Observes how the eligibility threshold changes compaction scope:
 * higher thresholds mean fewer file groups are selected for rewrite.
 */
public class Exp02_MinInputFiles extends BaseExperiment {

    private int currentMinFiles = 2;

    @Override
    public String experimentName() {
        return "Exp02_MinInputFiles_" + currentMinFiles;
    }

    @Override
    protected Map<String, String> compactionOptions() {
        return Map.of(
            "target-file-size-bytes", String.valueOf(LakehouseConfig.targetFileSizes()[1]),
            "min-input-files",        String.valueOf(currentMinFiles)
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
