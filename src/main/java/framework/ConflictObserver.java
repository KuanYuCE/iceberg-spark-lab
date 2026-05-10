package framework;

import framework.model.ConcurrencyMetrics;
import framework.model.SnapshotEvent;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;

public class ConflictObserver {

    private static final Logger log = LoggerFactory.getLogger(ConflictObserver.class);

    /**
     * Reads the full snapshot lineage from the Iceberg table and builds ConcurrencyMetrics.
     * Call this after BOTH the ingestion worker and compaction worker have finished.
     */
    public ConcurrencyMetrics analyze(Table table,
                                      int ingestionCommitsAttempted,
                                      int ingestionCommitsSucceeded,
                                      int ingestionCommitsFailed,
                                      int rewriteRetries,
                                      int rewriteConflicts,
                                      int fileGroupsSkipped) {
        table.refresh();
        List<SnapshotEvent> lineage = buildLineage(table);
        long total = lineage.size();

        log.info("ConflictObserver: {} total snapshots in lineage", total);
        log.info("  ingestion: attempted={} succeeded={} failed={}",
            ingestionCommitsAttempted, ingestionCommitsSucceeded, ingestionCommitsFailed);
        log.info("  rewrite:   retries={} conflicts={} fileGroupsSkipped={}",
            rewriteRetries, rewriteConflicts, fileGroupsSkipped);

        return new ConcurrencyMetrics(
            ingestionCommitsAttempted, ingestionCommitsSucceeded, ingestionCommitsFailed,
            rewriteRetries, rewriteConflicts, fileGroupsSkipped,
            total, lineage);
    }

    private List<SnapshotEvent> buildLineage(Table table) {
        List<SnapshotEvent> events = new ArrayList<>();
        for (Snapshot snapshot : table.snapshots()) {
            Long parentId = snapshot.parentId();
            Map<String, String> summary = snapshot.summary();
            int added   = Integer.parseInt(summary.getOrDefault("added-data-files",   "0"));
            int deleted = Integer.parseInt(summary.getOrDefault("deleted-data-files", "0"));
            events.add(new SnapshotEvent(
                snapshot.snapshotId(), parentId,
                snapshot.operation(), added, deleted,
                snapshot.timestampMillis()));
        }
        events.sort(Comparator.comparingLong(e -> e.timestampMs));
        return events;
    }
}
