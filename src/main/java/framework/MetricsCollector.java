package framework;

import framework.model.SnapshotMetrics;
import org.apache.iceberg.*;
import org.apache.iceberg.io.CloseableIterable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.*;

public class MetricsCollector {

    private static final Logger log = LoggerFactory.getLogger(MetricsCollector.class);

    /**
     * Reads current table state via Iceberg's metadata API.
     * Does NOT run a Spark job — reads manifest and data file metadata only.
     *
     * @param table the live Iceberg Table object (call table.refresh() before this)
     * @param label "before" or "after", used to label the result
     */
    public SnapshotMetrics collect(Table table, String label) {
        log.info("MetricsCollector [{}]: scanning file metadata for {}", label, table.name());

        long fileCount = 0;
        long totalSize = 0;
        long minSize   = Long.MAX_VALUE;
        long maxSize   = 0;
        Map<String, Long> perPartition = new LinkedHashMap<>();

        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask task : tasks) {
                DataFile file = task.file();
                long size = file.fileSizeInBytes();
                fileCount++;
                totalSize += size;
                minSize = Math.min(minSize, size);
                maxSize = Math.max(maxSize, size);

                String partKey = task.spec().partitionToPath(file.partition());
                perPartition.merge(partKey, 1L, Long::sum);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to scan table files for metrics", e);
        }

        long avgSize = fileCount > 0 ? totalSize / fileCount : 0;
        if (fileCount == 0) minSize = 0;

        // Count all snapshots in table history
        long snapshotCount = 0;
        for (Snapshot ignored : table.snapshots()) {
            snapshotCount++;
        }

        // Count manifests referenced by the current snapshot
        long manifestCount = 0;
        Snapshot current = table.currentSnapshot();
        if (current != null) {
            manifestCount = current.allManifests(table.io()).size();
        }

        log.info("MetricsCollector [{}]: files={}, totalMB={}, avgKB={}, snapshots={}, manifests={}",
            label, fileCount,
            String.format("%.1f", totalSize / 1_048_576.0),
            String.format("%.1f", avgSize / 1_024.0),
            snapshotCount, manifestCount);

        return new SnapshotMetrics(label, fileCount, totalSize, avgSize,
            minSize, maxSize, snapshotCount, manifestCount, perPartition);
    }
}
