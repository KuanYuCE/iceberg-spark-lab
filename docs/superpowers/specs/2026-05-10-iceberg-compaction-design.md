# Iceberg Compaction Lab — Design Spec

**Date:** 2026-05-10
**Status:** Approved

---

## 1. 專案目標

透過可執行的實驗，深入理解 Apache Iceberg 的 rewrite/compaction 行為，包含：

- 各壓縮參數對檔案分布、快照結構的影響
- Spark 資源設定與 Iceberg rewrite 參數的交互作用
- 壓縮前後的 Trino 查詢效能差異
- 並行寫入與壓縮作業之間的 OCC 衝突、retry、partial success 行為

---

## 2. 技術堆疊

| 項目 | 版本 / 規格 |
|------|------------|
| Java | 17 |
| Build tool | Gradle |
| Apache Spark | 3.5.x |
| Apache Iceberg | 1.6.x |
| Object storage | MinIO（S3 相容，`localhost:9000`） |
| Catalog | Hive Metastore（`localhost:9083`） |
| Query engine | Trino 467（`localhost:8080`） |
| Metastore backend | MariaDB（`localhost:3306`） |

所有服務透過現有的 `airflow_testing/integration/docker-compose.yml` 啟動，Spark 程序在本機 JVM 執行，連入 Docker network 內的服務。

---

## 3. Lakehouse 連線設定

| 項目 | 值 |
|------|----|
| MinIO endpoint | `http://localhost:9000` |
| MinIO access key | `minioadmin` |
| MinIO secret key | `minioadmin` |
| Iceberg warehouse | `s3a://my-raw/iceberg-experiments` |
| HMS thrift URI | `thrift://localhost:9083` |
| Spark catalog name | `iceberg` |
| Database | `iceberg_lab` |
| Table | `events` |
| Trino JDBC URL | `jdbc:trino://localhost:8080/iceberg/iceberg_lab` |

`my-landing` bucket 為上游原始資料（JSON/CSV）使用，與本實驗無關。所有 Iceberg data file 一律寫入 `my-raw`。

---

## 4. Schema 與分區

```sql
CREATE TABLE iceberg.iceberg_lab.events (
  event_id   STRING,
  user_id    STRING,
  event_type STRING,    -- click / view / purchase / error
  payload    STRING,    -- JSON string，模擬 payload
  region     STRING,    -- us-east / eu-west / ap-south
  ts         TIMESTAMP
)
PARTITIONED BY (region, days(ts))
```

分區組合：3 個 region × 7 天 = **21 個 partition**，足以支撐 file group 分散實驗。

---

## 5. 執行模式

框架支援兩種執行模式，透過 `LakehouseConfig.runMode` 切換：

### Local 模式（行為驗證）

| 資源 | 規格 |
|------|------|
| CPU | 2 cores |
| RAM | 4 GB（Docker 容器已佔 ~2.5 GB，Spark 可用 ~1.5 GB） |
| 磁碟 | 10 GB 可用 |
| Spark 設定 | `local[2]`，driver.memory=1g，shuffle.partitions=4 |
| 資料集規模 | ~50–80 MB，約 110 個小檔案 |
| 目標檔案大小 | 4 MB / 16 MB / 64 MB |
| max-concurrent | 1 / 2 / 4（4 為超訂閱壓力測試） |

### Cloud 模式（效能量測）

| 資源 | 建議規格 |
|------|---------|
| CPU | 8 cores |
| RAM | 16 GB |
| 磁碟 | 50 GB |
| Spark 設定 | `local[8]`，driver.memory=8g，shuffle.partitions=16 |
| 資料集規模 | 2–10 GB，500–2000 個小檔案 |
| 目標檔案大小 | 32 MB / 128 MB / 512 MB |
| max-concurrent | 1 / 5 / 10 |

---

## 6. 專案目錄結構

```
my-iceberg-experiment/
├── build.gradle
├── settings.gradle
├── gradle/wrapper/
├── scripts/
│   └── run-exp07.sh                  # Exp07 雙程序協調腳本
├── src/main/java/
│   ├── framework/
│   │   ├── LakehouseConfig.java       # 連線常數 + 執行模式設定
│   │   ├── BaseExperiment.java        # 抽象基底（template method）
│   │   ├── DataGenerator.java         # 混合小檔案生成策略
│   │   ├── MetricsCollector.java      # Iceberg metadata API 擷取
│   │   ├── SparkMetricsListener.java  # SparkListener，stage 資源指標
│   │   ├── TrinoQueryBenchmark.java   # Trino JDBC 查詢效能量測
│   │   ├── ConflictObserver.java      # Snapshot lineage + 衝突事件分析
│   │   ├── ReportWriter.java          # JSON + HTML（Chart.js）輸出
│   │   └── model/
│   │       ├── SnapshotMetrics.java
│   │       ├── CompactionPerformance.java
│   │       ├── SparkResourceMetrics.java
│   │       ├── QueryBenchmarkResult.java
│   │       ├── ConcurrencyMetrics.java
│   │       └── ExperimentResult.java
│   └── experiments/
│       ├── Exp01_TargetFileSize.java
│       ├── Exp02_MinInputFiles.java
│       ├── Exp03_MaxConcurrentRewrites.java
│       ├── Exp04_PartialProgress.java
│       ├── Exp05_RewriteByPartition.java
│       ├── Exp06_FullVsPartitionRewrite.java
│       ├── Exp07a_IngestionWorker.java
│       ├── Exp07b_CompactionWorker.java
│       └── Exp07_ResultAnalyzer.java
├── results/                           # JSON + HTML 輸出（gitignore）
└── docs/superpowers/specs/
    └── 2026-05-10-iceberg-compaction-design.md
```

---

## 7. 框架元件職責

### 7.1 `LakehouseConfig`

集中管理所有連線常數與執行模式參數，實驗類別不直接寫魔法字串。包含：
- MinIO / HMS / Trino 連線資訊
- warehouse 路徑
- `RunMode` enum（LOCAL / CLOUD）
- `pauseForUi` boolean（是否在 SparkSession 關閉前等待 Enter）
- 各模式對應的 Spark config、資料集大小、compaction 參數範圍

### 7.2 `BaseExperiment`（Template Method）

```
run()
 ├─ 1. buildSpark()              # 套用 base config + sparkOverrides()
 ├─ 2. setupTable()              # 建表（若已存在則清空重建）
 ├─ 3. generateSmallFiles()      # DataGenerator 兩階段寫入
 ├─ 4. collect(before)           # MetricsCollector + SparkMetricsListener reset
 ├─ 5. trinoQuery(before)        # TrinoQueryBenchmark（Spark 閒置時執行，不關閉 Session）
 ├─ 6. runCompaction()           # ← 各實驗覆寫此方法
 ├─ 7. collect(after)            # MetricsCollector + SparkMetricsListener flush
 ├─ 8. closeSparkSession()       # 先關閉 Session，釋放 CPU/記憶體
 ├─ 9. trinoQuery(after)         # SparkSession 已關閉後執行，無資源競爭
 ├─ 9. report()                  # ReportWriter → JSON + HTML
 └─10. report()                  # ReportWriter → JSON + HTML
       [pauseForUi=true 時：在步驟 8 關閉前暫停，等待 Enter]
```

子類別覆寫：
- `String experimentName()`
- `Map<String, String> compactionOptions()`
- `Map<String, String> sparkOverrides()`（預設空 Map）

### 7.3 `DataGenerator`

兩階段生成小檔案：

1. **Bulk 寫入**：Local 模式 10 萬筆，`repartition(60)` → ~60 個小檔案
2. **小批次迴圈**：50 次迴圈，每次 append 300 筆 → ~50 個小檔案（每個 Spark write 產生一個獨立 snapshot）

Cloud 模式放大 5–10 倍。總計 ~110 個小檔案，分散在 21 個 partition 中。

### 7.4 `MetricsCollector`

從 Iceberg metadata API 擷取快照，回傳 `SnapshotMetrics`：

```
fileCount, totalSizeBytes, avgFileSizeBytes,
minFileSizeBytes, maxFileSizeBytes,
snapshotCount, manifestCount,
perPartitionFileCounts: Map<String, Integer>
```

### 7.5 `SparkMetricsListener`

實作 `SparkListener`，在 compaction job 執行期間捕捉 stage 層指標：

```
taskCount, totalCpuTimeMs, totalGcTimeMs,
peakExecutorMemoryBytes, bytesRead, bytesWritten,
shuffleBytesWritten
```

衍生計算：
- `gcPressureRatio = totalGcTimeMs / totalCpuTimeMs`
- `parallelismEfficiency = taskCount / (cores × durationSecs)`
- `rewriteThroughputMBps = bytesRewritten / durationMs`

### 7.6 `TrinoQueryBenchmark`

在 SparkSession 關閉後，透過 Trino JDBC 執行四個標準 benchmark 查詢，各跑 3 次取中位數：

| 查詢 | 目的 |
|------|------|
| `SELECT COUNT(*)` | 全表掃描基準 |
| `SELECT region, COUNT(*) GROUP BY region` | 分區聚合 |
| `SELECT * WHERE region='us-east' AND ts BETWEEN ...` | 分區剪枝效果 |
| `SELECT event_type, AVG(LENGTH(payload)) GROUP BY event_type` | 跨分區聚合 |

### 7.7 `ConflictObserver`（Exp07 專用）

分析 `table.snapshots()` 重建 snapshot lineage，統計：

```
ingestion:  commitsAttempted, commitsSucceeded, commitsFailed
rewrite:    commitsAttempted, commitsSucceeded, commitRetries,
            commitConflicts, fileGroupsSkipped
snapshot:   totalSnapshotsCreated,
            snapshotLineage: [{ id, parentId, operation, addedFiles,
                                deletedFiles, timestamp }],
            manifestMergeEvents
```

### 7.8 `ReportWriter`

每次實驗輸出兩個檔案：

- `results/<experiment-name>-<yyyyMMdd-HHmmss>.json`
- `results/<experiment-name>-<yyyyMMdd-HHmmss>.html`

HTML 包含五組 Chart.js 圖表：

| 圖表 | 內容 |
|------|------|
| 1 | Before/after 檔案數 + 平均大小（grouped bar） |
| 2 | 壓縮耗時 + 吞吐量（數值卡片） |
| 3 | Trino 查詢 before/after latency（grouped bar） |
| 4 | 資源效率矩陣（GC 壓力、並行效率、吞吐量） |
| 5 | Snapshot 時序圖，標示衝突點與 retry（Exp07 專用） |

---

## 8. 實驗清單

### Exp01：`target-file-size-bytes`

**目標：** 觀察不同目標大小對檔案分布與查詢效能的影響。

| 模式 | 測試值 |
|------|--------|
| Local | 4 MB / 16 MB / 64 MB |
| Cloud | 32 MB / 128 MB / 512 MB |

在同一個實驗類別內跑三輪，每輪重建資料後執行壓縮，輸出三組並排結果。

---

### Exp02：`min-input-files`

**目標：** 觀察壓縮觸發門檻對壓縮範圍與 file group 選擇的影響。

測試值：2 / 5 / 10

---

### Exp03：`max-concurrent-file-group-rewrites`

**目標：** 觀察並行度對吞吐量、GC 壓力、CPU 效率的影響。

| 模式 | 測試值 | 說明 |
|------|--------|------|
| Local | 1 / 2 / 4 | 4 為超訂閱壓力測試 |
| Cloud | 1 / 5 / 10 | 真實並行 |

在同一實驗類別內跑多輪，輸出資源效率比較矩陣。

---

### Exp04：`partial-progress`

**目標：** 觀察中途 commit 對 snapshot 數量與壓縮連續性的影響。

設定：`partial-progress.enabled=true`，`max-commits`：2 / 5 / 10

---

### Exp05：`RewriteByPartition`

**目標：** 對比針對單一 partition（`region=us-east`）與全表 rewrite 的資源集中度與耗時差異。

---

### Exp06：`FullVsPartitionRewrite`

**目標：** 相同資料量下，全表 rewrite 策略 vs 逐 partition rewrite 策略的總耗時、snapshot 行為、Trino 查詢效能比較。

---

### Exp07：`ConcurrentConflicts`（雙程序）

**目標：** 觀察 Iceberg OCC 在真實並行負載下的衝突、retry、partial success 行為。

#### 架構

三個獨立類別，透過 `scripts/run-exp07.sh` 協調：

```
Exp07a_IngestionWorker  ─── 獨立 JVM 程序 ──→ Spark UI: localhost:4040
Exp07b_CompactionWorker ─── 獨立 JVM 程序 ──→ Spark UI: localhost:4041
Exp07_ResultAnalyzer    ─── 兩者結束後執行，產生合併報表
```

#### 協調機制（file-based signal）

```
results/exp07-ingestion-ready.flag   # IngestionWorker 建好初始資料後建立
results/exp07-stop.flag              # CompactionWorker 結束後建立
```

#### 三個子情境

| 子情境 | Ingestion 速率 | partial-progress | 預期現象 |
|--------|--------------|-----------------|---------|
| A | 每 2s 一次 append | disabled | 偶發 retry，rewrite 通常成功 |
| B | 每 100ms 一次 append | disabled | 高衝突率，多次 retry，可能失敗 |
| C | 每 100ms 一次 append | enabled（max-commits=5）| 部分 file group 跳過，partial success |

---

## 9. `ExperimentResult` 完整結構

```
ExperimentResult {
  experimentName: String
  timestamp: String
  runMode: LOCAL | CLOUD
  compactionOptions: Map<String, String>
  sparkConfig: Map<String, String>

  fileMetrics: {
    before: SnapshotMetrics
    after:  SnapshotMetrics
  }

  compactionPerf: {
    durationMs: Long
    filesRewritten: Int
    bytesRewritten: Long
    fileGroupsRewritten: Int
    throughputMBps: Double
  }

  sparkResources: {
    stageMetrics: {
      taskCount, totalCpuTimeMs, totalGcTimeMs,
      peakExecutorMemoryBytes, bytesRead, bytesWritten
    }
    derived: {
      gcPressureRatio, parallelismEfficiency, rewriteThroughputMBps
    }
  }

  queryBenchmark: [
    { query: String, beforeMs: Long, afterMs: Long, improvementPct: Double }
  ]

  concurrencyMetrics: ConcurrencyMetrics   // Exp07 only, null otherwise
}
```

---

## 10. Gradle 主要依賴

```gradle
implementation "org.apache.spark:spark-sql_2.12:3.5.x"
implementation "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.x"
implementation "org.apache.hadoop:hadoop-aws:3.3.x"
implementation "io.trino:trino-jdbc:467"
implementation "com.fasterxml.jackson.core:jackson-databind:2.x"
implementation "org.slf4j:slf4j-api:2.x"
implementation "org.apache.logging.log4j:log4j-slf4j2-impl:2.x"
```

---

## 11. 執行方式

```bash
# 單一實驗
./gradlew run -PmainClass=experiments.Exp01_TargetFileSize

# 開啟 Spark UI 暫停模式
./gradlew run -PmainClass=experiments.Exp01_TargetFileSize -PpauseForUi=true

# Cloud 模式
./gradlew run -PmainClass=experiments.Exp01_TargetFileSize -PrunMode=cloud

# Exp07 雙程序並行實驗
bash scripts/run-exp07.sh

# 查看報表
open results/Exp01_TargetFileSize-20260510-143022.html
```

Spark UI：
- 主程序：`http://localhost:4040`
- Exp07 壓縮程序：`http://localhost:4041`

---

## 12. 設計限制與已知取捨

| 限制 | 說明 |
|------|------|
| Local 模式並行實驗受限 | 2 cores 下 `max-concurrent > 2` 為超訂閱，非真實並行 |
| Local 模式 Trino 查詢差異小 | 資料集 ~150MB，latency 差異可能在統計噪音範圍內 |
| Trino 與 Spark 不同時運行 | 避免 CPU 競爭：SparkSession 關閉後才執行 Trino benchmark |
| `SparkContext` singleton | Exp07 使用雙獨立程序繞過此限制，行為與生產環境一致 |
| Cloud 模式需手動切換 | 透過 `LakehouseConfig.runMode` 或 `-PrunMode=cloud` 切換 |
