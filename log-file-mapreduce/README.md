# Log File Search and Error Detection Using MapReduce

A complete, production-grade, and beginner-friendly Big Data Analytics project demonstrating distributed log file processing, search indexing, and automated error detection using the **MapReduce programming model**.

---

## 1. Project Introduction

Modern enterprise web architectures, microservices, and cloud clusters generate gigabytes of log telemetry every hour. Traditional linear file parsing scripts (e.g., sequential `grep` or standard single-threaded loops) fail when handling large-scale files due to memory starvation and CPU bottlenecks.

This project implements the foundational **MapReduce programming model** to process, partition, search, and classify server log events across 4 distinct computational phases:
1. **Mapper**: Decomposes input data into discrete chunks and emits intermediate `(Key, Value)` pairs.
2. **Shuffle and Sort**: Groups records by key and distributes partitioned workloads across worker buckets.
3. **Reducer**: Computes associative and commutative aggregations (sum reduction, frequency matrices, percentage calculations).
4. **Final Output**: Produces business intelligence metrics, anomaly rankings, and structured CSV/text reports.

---

## 2. Problem Statement

System administrators and site reliability engineers (SREs) face recurring challenges:
- High volume of unstructured log messages across distributed nodes.
- Inability to quickly pinpoint critical failure root causes (e.g. database pool exhaustion, Out-of-Memory crashes, deadlock exceptions).
- Lack of transparent pedagogical tools for engineering students to inspect intermediate MapReduce data transformations.

This project solves these issues by simulating the MapReduce paradigm with full intermediate visibility, paired with an interactive dashboard and search engine.

---

## 3. Technology Stack

- **Core Language**: Python 3.10+
- **Web UI & Dashboard**: Streamlit
- **Data Engineering & Vector Operations**: Pandas, NumPy
- **Interactive Visualizations**: Plotly, Matplotlib
- **Input Formats**: CSV (Structured) and TXT/LOG (Unstructured Syslog, Apache, Spring Boot formats)
- **Processing Architecture**: Chunk-based parallel processing simulation with hash partitioning

---

## 4. System Architecture & MapReduce Workflow

```
+-----------------------------------------------------------------------+
|                         INPUT LOG DATASET                             |
|               (CSV, TXT, Server Access & Error Logs)                  |
+-----------------------------------------------------------------------+
                                   │
                                   ▼
+-----------------------------------------------------------------------+
|                             STAGE 1: MAPPER                           |
|  - Chunks input into fixed-size splits (e.g. 100 records/block)       |
|  - Parses timestamp, severity, service, IP, and message text          |
|  - Extracts normalized signatures & detects error categories          |
|  - Emits:                                                             |
|       ("LEVEL:ERROR", 1), ("SERVICE:auth-service", 1),                |
|       ("ERROR_CAT:DATABASE", 1), ("ERROR_SIG:<sig>", 1)               |
+-----------------------------------------------------------------------+
                                   │
                                   ▼
+-----------------------------------------------------------------------+
|                      STAGE 2: SHUFFLE & SORT BARRIER                  |
|  - In-memory group-by operation: collects all values for each key     |
|  - Transforms: ("LEVEL:ERROR", 1), ("LEVEL:ERROR", 1)                 |
|            -> ("LEVEL:ERROR", [1, 1, 1, 1, 1...])                     |
|  - Sorts all keys lexicographically                                   |
|  - Partitions keys across simulated Reducer tasks:                    |
|       partition_id = abs(hash(key)) % num_reducers                   |
+-----------------------------------------------------------------------+
                                   │
                                   ▼
+-----------------------------------------------------------------------+
|                            STAGE 3: REDUCER                           |
|  - Executes reduction function: Sum(List[Values])                     |
|  - Calculates error rates, warning percentages, and category counts   |
|  - Sorts root-cause signatures in descending order of frequency       |
+-----------------------------------------------------------------------+
                                   │
                                   ▼
+-----------------------------------------------------------------------+
|                       STAGE 4: FINAL ANALYTICS OUTPUT                 |
|  - Interactive Search & Multi-criteria Filtering                      |
|  - Dynamic Plotly Visualizations (Donuts, Bar Charts, Timelines)      |
|  - Automated CSV & Academic Project Report Generation                 |
+-----------------------------------------------------------------------+
```

---

## 5. Project Directory Structure

```
log-file-mapreduce/
│
├── app.py                     # Main Streamlit web application & dashboard
├── mapper.py                  # Map functions, chunk processing, (Key, 1) emissions
├── shuffle_sort.py            # Group-by keys, sorting, hash-partitioning
├── reducer.py                 # Sum aggregation, KPI calculations, metric generation
├── log_parser.py              # Parsing logic for CSV and TXT log lines (Regex fallback)
├── analytics.py               # Rule-based error classifier and multi-filter search engine
├── report_generator.py        # CSV string serializers and academic examination report
├── requirements.txt           # Python library dependencies
├── README.md                  # Comprehensive academic documentation
│
├── sample_data/
│   ├── generate_logs.py       # Deterministic Python script to generate realistic logs
│   └── sample_logs.csv        # Realistic 650+ records dataset with diverse errors
│
└── utils/
    ├── file_handler.py        # Chunk generator and file size formatting utilities
    └── validators.py          # Schema validation, error detection, integrity auditing
```

---

## 6. Installation & Execution Guide

### Prerequisites
- Python 3.8, 3.9, 3.10, 3.11, or 3.12 installed.
- `pip` package manager.

### Step 1: Clone or Navigate to the Project Directory
```bash
cd log-file-mapreduce
```

### Step 2: Install Python Dependencies
```bash
pip install -r requirements.txt
```

### Step 3: Run the Streamlit Application
```bash
streamlit run app.py
```
*The application will automatically launch in your default web browser at `http://localhost:8501`.*

---

## 7. Module Walkthrough for Academic Viva

### Module 1: Home & Overview
- Explains the MapReduce paradigm, project goals, and high-level architecture.

### Module 2: Log File Upload
- Demonstrates loading `.csv`, `.txt`, and `.log` files.
- Tests file size calculation and previews the first 15 records.
- Provides a one-click button to load the built-in 650-record realistic sample dataset.

### Module 3: MapReduce Processing Engine
- Inspect each stage's intermediate data:
  - **Mapper**: View emitted `(Key, 1)` pairs.
  - **Shuffle & Sort**: Inspect grouped `(Key, [1, 1...])` lists and partition allocations across Reducer nodes.
  - **Reducer**: View reduced count keys.
  - **Final Output**: Summarized KPI matrix.
- Interactive slider to change chunk sizes (simulating Hadoop block sizes like 64MB or 128MB).
- Side-by-side comparison between local Python simulation and Apache Hadoop cluster execution.

### Module 4: Log Search Engine
- Search logs in real time with keyword, service, date range, or regex queries.
- Toggle "Errors Only" mode to isolate anomalies instantly.

### Module 5: Error Detection & Classification
- Rule-based detection classifying errors into:
  - `Memory / OOM Crash`: OutOfMemoryError, heap space exhaustion.
  - `Database Deadlock / Pool`: PostgreSQL connection pool exhaustion, table lockups.
  - `Database Connectivity`: Connection refused, query timeouts.
  - `Authentication & Security`: Invalid JWT, unauthorized access, expired tokens.
  - `HTTP 5xx Upstream`: HTTP 500, 502 Bad Gateway, 503 Service Unavailable, 504 Timeout.
  - `Network Timeout`: SocketTimeoutException, latency SLA breaches.
  - `Storage Warning`: Disk partitions exceeding 85% capacity.

### Module 6: Analytics Dashboard
- KPI summary cards (Total Records, Errors, Warnings, Info, Debug, Failure %).
- Plotly Donut chart for severity distribution.
- Plotly Horizontal Bar chart for error category frequencies.
- Ranked leaderboard of top root-cause error signatures.

### Module 7: Report Generation
- Download processed records (CSV).
- Download error-only records (CSV).
- Download MapReduce aggregated summary (CSV).
- Download full Academic Project Examination Report (.txt / .md).

---

## 8. Viva Voce / Examination Questions & Answers

**Q1: What are the primary advantages of MapReduce over traditional loops?**  
*Answer:* MapReduce divides large datasets into independent chunks processed in parallel by Mappers, preventing memory saturation. It abstracts distributed fault-tolerance, data partitioning, and load balancing across cluster nodes.

**Q2: What is the purpose of the Shuffle and Sort stage?**  
*Answer:* The Shuffle and Sort phase acts as the synchronization barrier between Mappers and Reducers. It guarantees that all values associated with the exact same key across any Mapper node are grouped together into a single iterable list `(key, [v1, v2, ...])` before reaching the Reducer.

**Q3: How does chunk-based processing simulate Hadoop HDFS?**  
*Answer:* In Apache Hadoop, HDFS splits large files into physical blocks (typically 128MB). Each block is assigned to a separate Mapper task. In our simulation, the chunk generator slices records into discrete batches (e.g., 100 records/chunk) processed by independent worker iterations.

**Q4: How are dynamic values handled when aggregating error messages?**  
*Answer:* Dynamic tokens such as user IDs (`usr_9821`), IP addresses (`192.168.1.45`), or transaction numbers are generalized into tokens like `<ID>` or `<IP>` during the Mapper phase. This allows the Reducer to count total occurrences of the identical root cause.

---

## 9. Future Enhancements

1. **Apache Spark Migration**: Port the pipeline to PySpark for in-memory RDD and DataFrame processing.
2. **Kafka Real-Time Streaming**: Ingest live log streams via Apache Kafka topics.
3. **ELK Stack Export**: Integrate Elasticsearch indexing and Kibana visual telemetry.
4. **Machine Learning Anomaly Detection**: Implement Isolation Forests or autoencoders to detect zero-day log anomalies without static regex rules.

---

**Author:** Big Data Engineering Academic Project  
**License:** Open Source (Apache 2.0)
