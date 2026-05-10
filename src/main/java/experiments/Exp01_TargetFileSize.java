package experiments;

import framework.BaseExperiment;
import framework.LakehouseConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;

/**
 * Runs compaction three rounds with different target file sizes.
 * Each round drops and recreates the table, so results are independent.
 *
 * Local:  4 MB / 16 MB / 64 MB
 * Cloud: 32 MB / 128 MB / 512 MB
 */
public class Exp01_TargetFileSize extends BaseExperiment {

    private static final Logger log = LoggerFactory.getLogger(Exp01_TargetFileSize.class);

    private long currentTargetBytes = LakehouseConfig.targetFileSizes()[0];

    @Override
    public String experimentName() {
        return "Exp01_TargetFileSize_" + (currentTargetBytes / 1_048_576) + "MB";
    }

    @Override
    protected Map<String, String> compactionOptions() {
        return Map.of(
            "target-file-size-bytes", String.valueOf(currentTargetBytes),
            "min-input-files",        "2"
        );
    }

    public static void main(String[] args) throws Exception {
        Exp01_TargetFileSize exp = new Exp01_TargetFileSize();
        for (long targetBytes : LakehouseConfig.targetFileSizes()) {
            exp.currentTargetBytes = targetBytes;
            log.info("--- Round: target-file-size = {}MB ---", targetBytes / 1_048_576);
            exp.run();
        }
        log.info("Exp01 complete — open results/ to view HTML reports.");
    }
}
