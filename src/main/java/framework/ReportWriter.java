package framework;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import framework.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;

public class ReportWriter {

    private static final Logger log = LoggerFactory.getLogger(ReportWriter.class);
    private static final ObjectMapper MAPPER =
        new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
    private static final DateTimeFormatter TS_FMT =
        DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss");

    public record OutputPaths(String jsonPath, String htmlPath) {}

    private final String outputDir;

    public ReportWriter(String outputDir) {
        this.outputDir = outputDir;
    }

    public OutputPaths write(ExperimentResult result) throws IOException {
        Files.createDirectories(Path.of(outputDir));
        String timestamp = LocalDateTime.now().format(TS_FMT);
        String base      = outputDir + "/" + result.experimentName + "-" + timestamp;
        String jsonPath  = base + ".json";
        String htmlPath  = base + ".html";

        MAPPER.writeValue(new File(jsonPath), result);
        log.info("ReportWriter: {}", jsonPath);

        Files.writeString(Path.of(htmlPath), buildHtml(result), StandardCharsets.UTF_8);
        log.info("ReportWriter: {}", htmlPath);

        return new OutputPaths(jsonPath, htmlPath);
    }

    // ── HTML generation ───────────────────────────────────────────────────────

    private String buildHtml(ExperimentResult r) throws IOException {
        String jsonEmbed = MAPPER.writeValueAsString(r);
        return "<!DOCTYPE html>\n"
            + "<html lang=\"en\">\n"
            + "<head>\n"
            + "  <meta charset=\"UTF-8\">\n"
            + "  <title>" + esc(r.experimentName) + " — Iceberg Lab</title>\n"
            + "  <script src=\"https://cdn.jsdelivr.net/npm/chart.js@4/dist/chart.umd.min.js\"></script>\n"
            + "  <style>\n"
            + "    body{font-family:sans-serif;max-width:1100px;margin:40px auto;padding:0 20px}\n"
            + "    h1{color:#333}h2{color:#555;margin-top:36px}\n"
            + "    .cards{display:flex;gap:16px;flex-wrap:wrap;margin-bottom:24px}\n"
            + "    .card{background:#f5f5f5;border-radius:8px;padding:14px 20px;min-width:140px}\n"
            + "    .card .val{font-size:1.9em;font-weight:bold;color:#2c7be5}\n"
            + "    .card .lbl{font-size:.8em;color:#666;margin-top:4px}\n"
            + "    canvas{max-height:300px;margin-bottom:36px}\n"
            + "    table{border-collapse:collapse;width:100%;margin-bottom:24px}\n"
            + "    th,td{border:1px solid #ddd;padding:7px 11px;text-align:left}\n"
            + "    th{background:#f0f0f0}\n"
            + "    pre{background:#272822;color:#f8f8f2;padding:16px;border-radius:6px;"
            + "overflow-x:auto;font-size:.82em;max-height:400px}\n"
            + "  </style>\n"
            + "</head>\n"
            + "<body>\n"
            + "  <h1>" + esc(r.experimentName) + "</h1>\n"
            + "  <p><strong>Mode:</strong> " + r.runMode
            + " &nbsp;|&nbsp; <strong>Timestamp:</strong> " + r.timestamp + "</p>\n"
            + "  <h2>Compaction Parameters</h2>\n"
            + "  <table>" + optionsTable(r.compactionOptions) + "</table>\n"
            + fileMetricsSection(r)
            + sparkResourceSection(r)
            + queryBenchmarkSection(r)
            + concurrencySection(r)
            + "  <h2>Raw JSON</h2>\n"
            + "  <pre id=\"rawJson\"></pre>\n"
            + "  <script>\n"
            + "  const data = " + jsonEmbed + ";\n"
            + "  document.getElementById('rawJson').textContent = JSON.stringify(data,null,2);\n"
            + fileChartScript()
            + queryChartScript()
            + "  </script>\n"
            + "</body>\n"
            + "</html>\n";
    }

    private String fileMetricsSection(ExperimentResult r) {
        SnapshotMetrics b = r.beforeFileMetrics;
        SnapshotMetrics a = r.afterFileMetrics;
        CompactionPerformance p = r.compactionPerf;
        return "  <h2>File Metrics</h2>\n"
            + "  <div class=\"cards\">\n"
            + card(b.fileCount + " → " + a.fileCount, "Data Files")
            + card(humanSize(b.avgFileSizeBytes) + " → " + humanSize(a.avgFileSizeBytes), "Avg File Size")
            + card(b.snapshotCount + " → " + a.snapshotCount, "Snapshots")
            + card(b.manifestCount + " → " + a.manifestCount, "Manifests")
            + "  </div>\n"
            + "  <canvas id=\"fileChart\"></canvas>\n"
            + "  <h2>Compaction Performance</h2>\n"
            + "  <div class=\"cards\">\n"
            + card(p.durationMs + "ms", "Duration")
            + card(String.format("%.1f MB/s", p.throughputMBps), "Throughput")
            + card(String.valueOf(p.filesRewritten), "Files Rewritten")
            + card(String.valueOf(p.fileGroupsRewritten), "File Groups")
            + "  </div>\n";
    }

    private String sparkResourceSection(ExperimentResult r) {
        SparkResourceMetrics s = r.sparkResources;
        return "  <h2>Spark Resource Metrics</h2>\n"
            + "  <div class=\"cards\">\n"
            + card(String.valueOf(s.taskCount), "Tasks")
            + card(String.format("%.1f%%", s.gcPressureRatio * 100), "GC Pressure")
            + card(String.format("%.2f", s.parallelismEfficiency), "Parallelism Eff.")
            + card(String.format("%.0f MB", s.bytesRead / 1_048_576.0), "Bytes Read")
            + "  </div>\n";
    }

    private String queryBenchmarkSection(ExperimentResult r) {
        if (r.queryBenchmark == null || r.queryBenchmark.isEmpty()) return "";
        return "  <h2>Trino Query Benchmark</h2>\n"
            + "  <canvas id=\"queryChart\"></canvas>\n";
    }

    private String concurrencySection(ExperimentResult r) {
        if (r.concurrencyMetrics == null) return "";
        ConcurrencyMetrics c = r.concurrencyMetrics;
        return "  <h2>Concurrency Metrics</h2>\n"
            + "  <div class=\"cards\">\n"
            + card(c.ingestionCommitsSucceeded + "/" + c.ingestionCommitsAttempted, "Ingestion Commits")
            + card(String.valueOf(c.rewriteCommitConflicts), "Conflicts")
            + card(String.valueOf(c.rewriteCommitRetries), "Retries")
            + card(String.valueOf(c.fileGroupsSkipped), "Groups Skipped")
            + "  </div>\n"
            + "  <p><strong>Total snapshots in lineage:</strong> " + c.totalSnapshotsCreated + "</p>\n";
    }

    private String fileChartScript() {
        return "  new Chart(document.getElementById('fileChart'),{\n"
            + "    type:'bar',\n"
            + "    data:{\n"
            + "      labels:['File Count','Avg Size (KB)','Total Size (MB)'],\n"
            + "      datasets:[\n"
            + "        {label:'Before',backgroundColor:'#e74c3c',\n"
            + "         data:[data.beforeFileMetrics.fileCount,"
            + "Math.round(data.beforeFileMetrics.avgFileSizeBytes/1024),"
            + "Math.round(data.beforeFileMetrics.totalSizeBytes/1048576)]},\n"
            + "        {label:'After',backgroundColor:'#2ecc71',\n"
            + "         data:[data.afterFileMetrics.fileCount,"
            + "Math.round(data.afterFileMetrics.avgFileSizeBytes/1024),"
            + "Math.round(data.afterFileMetrics.totalSizeBytes/1048576)]}\n"
            + "      ]\n"
            + "    },\n"
            + "    options:{responsive:true,plugins:{legend:{position:'top'}}}\n"
            + "  });\n";
    }

    private String queryChartScript() {
        return "  if(data.queryBenchmark&&data.queryBenchmark.length>0){\n"
            + "    new Chart(document.getElementById('queryChart'),{\n"
            + "      type:'bar',\n"
            + "      data:{\n"
            + "        labels:data.queryBenchmark.map(q=>q.queryName),\n"
            + "        datasets:[\n"
            + "          {label:'Before (ms)',backgroundColor:'#e74c3c',"
            + "data:data.queryBenchmark.map(q=>q.beforeMs)},\n"
            + "          {label:'After (ms)',backgroundColor:'#2ecc71',"
            + "data:data.queryBenchmark.map(q=>q.afterMs)}\n"
            + "        ]\n"
            + "      },\n"
            + "      options:{responsive:true,plugins:{legend:{position:'top'}}}\n"
            + "    });\n"
            + "  }\n";
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private String card(String value, String label) {
        return "    <div class=\"card\"><div class=\"val\">" + esc(value)
            + "</div><div class=\"lbl\">" + esc(label) + "</div></div>\n";
    }

    private String optionsTable(Map<String, String> options) {
        StringBuilder sb = new StringBuilder("<tr><th>Option</th><th>Value</th></tr>");
        if (options != null) {
            options.forEach((k, v) ->
                sb.append("<tr><td>").append(esc(k)).append("</td><td>").append(esc(v)).append("</td></tr>"));
        }
        return sb.toString();
    }

    private String humanSize(long bytes) {
        if (bytes >= 1_048_576) return String.format("%.1f MB", bytes / 1_048_576.0);
        if (bytes >= 1_024)     return String.format("%.1f KB", bytes / 1_024.0);
        return bytes + " B";
    }

    private String esc(String s) {
        if (s == null) return "";
        return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;");
    }
}
