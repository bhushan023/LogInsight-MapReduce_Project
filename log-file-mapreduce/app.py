"""
app.py
======
Main Streamlit Application
Project Title: Log File Search and Error Detection Using MapReduce
Designed for Big Data Analytics academic demonstration.

Modules:
1. Home Dashboard & MapReduce Architecture
2. Log Ingestion (Upload CSV/TXT & Load Sample)
3. MapReduce Processing Engine (Interactive Step-by-Step)
4. Interactive Log Search Engine
5. Error Detection & Rule-Based Classifier
6. Analytics Dashboard (KPIs & Visualizations)
7. Report Generation & Exports
"""

import os
import time
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

# Import modular project components
from log_parser import parse_csv_content, parse_txt_content
from mapper import chunk_mapper
from shuffle_sort import shuffle_and_sort
from reducer import reduce_grouped_stream
from analytics import filter_records, classify_record, CLASSIFICATION_RULES
from report_generator import generate_csv_string, generate_summary_csv, generate_academic_report
from utils.file_handler import get_file_size_formatted, read_in_chunks
from utils.validators import validate_uploaded_content, audit_parsed_records

# -----------------------------------------------------------------------------
# STREAMLIT PAGE CONFIGURATION
# -----------------------------------------------------------------------------
st.set_page_config(
    page_title="Log File Search & Error Detection Using MapReduce",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for clean academic styling
st.markdown("""
<style>
    .main-header {
        font-size: 2.1rem;
        font-weight: 700;
        color: #1E293B;
        margin-bottom: 0.25rem;
    }
    .sub-header {
        font-size: 1.05rem;
        color: #64748B;
        margin-bottom: 1.5rem;
    }
    .metric-card {
        background-color: #F8FAFC;
        border: 1px solid #E2E8F0;
        border-radius: 8px;
        padding: 1rem;
        text-align: center;
    }
    .stage-pill {
        display: inline-block;
        padding: 4px 12px;
        border-radius: 6px;
        font-weight: 600;
        font-size: 0.85rem;
        margin-right: 6px;
    }
</style>
""", unsafe_allow_html=True)

# -----------------------------------------------------------------------------
# SESSION STATE INITIALIZATION
# -----------------------------------------------------------------------------
if "records" not in st.session_state:
    st.session_state.records = []
if "filename" not in st.session_state:
    st.session_state.filename = "sample_logs.csv"
if "file_size_bytes" not in st.session_state:
    st.session_state.file_size_bytes = 0
if "mr_results" not in st.session_state:
    st.session_state.mr_results = None
if "mr_metrics" not in st.session_state:
    st.session_state.mr_metrics = {}

# -----------------------------------------------------------------------------
# HELPER: LOAD DEFAULT SAMPLE LOGS
# -----------------------------------------------------------------------------
def load_sample_dataset():
    sample_path = os.path.join(os.path.dirname(__file__), "sample_data", "sample_logs.csv")
    if os.path.exists(sample_path):
        with open(sample_path, "r", encoding="utf-8") as f:
            content = f.read()
        st.session_state.records = parse_csv_content(content)
        st.session_state.filename = "sample_logs.csv"
        st.session_state.file_size_bytes = len(content.encode("utf-8"))
        # Execute MapReduce pipeline
        run_mapreduce_pipeline(st.session_state.records)
        return True
    return False

def run_mapreduce_pipeline(records, chunk_size=100, num_reducers=4):
    start_time = time.perf_counter()
    
    # 1. Mapper Stage: Process chunks
    all_intermediate_pairs = []
    chunks = list(read_in_chunks(records, chunk_size))
    for i, chunk in enumerate(chunks):
        pairs = chunk_mapper(chunk, worker_id=i+1)
        all_intermediate_pairs.extend(pairs)
        
    map_time = time.perf_counter()
    
    # 2. Shuffle & Sort Stage
    ss_output = shuffle_and_sort(all_intermediate_pairs, num_reducers=num_reducers)
    shuffle_time = time.perf_counter()
    
    # 3. Reducer Stage
    reduced_output = reduce_grouped_stream(ss_output["grouped_data"])
    reduce_time = time.perf_counter()
    
    total_time_ms = (reduce_time - start_time) * 1000.0
    
    st.session_state.mr_results = {
        "mapper_pairs_count": len(all_intermediate_pairs),
        "mapper_sample": all_intermediate_pairs[:20],
        "shuffle_data": ss_output,
        "reduced_output": reduced_output,
        "chunks_count": len(chunks)
    }
    
    st.session_state.mr_metrics = {
        "total_time_ms": total_time_ms,
        "map_time_ms": (map_time - start_time) * 1000.0,
        "shuffle_time_ms": (shuffle_time - map_time) * 1000.0,
        "reduce_time_ms": (reduce_time - shuffle_time) * 1000.0,
        "throughput_rps": len(records) / (total_time_ms / 1000.0) if total_time_ms > 0 else 0
    }

# Auto-load on first launch if empty
if not st.session_state.records:
    load_sample_dataset()

# -----------------------------------------------------------------------------
# SIDEBAR NAVIGATION
# -----------------------------------------------------------------------------
with st.sidebar:
    st.title("⚡ MapReduce Engine")
    st.caption("Big Data Analytics Academic Project")
    st.divider()
    
    module = st.radio(
        "Navigation Modules",
        [
            "Home & Overview",
            "Log File Upload",
            "MapReduce Processing",
            "Log Search Engine",
            "Error Classification",
            "Analytics Dashboard",
            "Report Generation"
        ],
        index=0
    )
    
    st.divider()
    st.markdown("### Active Log State")
    st.write(f"📁 **File:** `{st.session_state.filename}`")
    st.write(f"📊 **Total Records:** `{len(st.session_state.records):,}`")
    if st.session_state.file_size_bytes > 0:
        st.write(f"💾 **Size:** `{get_file_size_formatted(st.session_state.file_size_bytes)}`")
        
    if st.button("🔄 Reset to Default Sample Dataset", use_container_width=True):
        load_sample_dataset()
        st.success("Sample dataset reloaded!")
        st.rerun()

# -----------------------------------------------------------------------------
# MODULE 1: HOME & OVERVIEW
# -----------------------------------------------------------------------------
if module == "Home & Overview":
    st.markdown("<h1 class='main-header'>Log File Search & Error Detection Using MapReduce</h1>", unsafe_allow_html=True)
    st.markdown("<p class='sub-header'>High-throughput distributed log search, error classification, and frequency analysis via the 4-stage MapReduce paradigm.</p>", unsafe_allow_html=True)
    
    col1, col2 = st.columns([3, 2])
    with col1:
        st.subheader("🎯 Project Objectives")
        st.markdown("""
        - **Demonstrate MapReduce Foundations**: Provide transparent visibility into the 4 classic stages:
          1. **Input Splitting & Mapper**: Parse records into intermediate `(Key, 1)` tuples.
          2. **Shuffle & Sort**: Group and partition identical keys across worker nodes.
          3. **Reducer**: Aggregate associative counts to produce statistical distributions.
          4. **Final Output**: Derive KPIs, frequency tables, and anomaly rankings.
        - **Automated Error Classification**: Categorize critical failures (OOM, Deadlocks, Timeouts, Auth, HTTP 5xx) using rule-based parsing.
        - **Academic Demonstration**: Compare chunk-based local multiprocessing simulations against real-world Apache Hadoop cluster workflows.
        """)
        
        st.subheader("🛠 Technology Stack")
        tech_col1, tech_col2 = st.columns(2)
        with tech_col1:
            st.markdown("""
            - **Programming Language**: Python 3.10+
            - **Web Framework**: Streamlit
            - **Data Manipulation**: Pandas & NumPy
            """)
        with tech_col2:
            st.markdown("""
            - **Visualization**: Plotly & Matplotlib
            - **Ingestion**: CSV and TXT log formats
            - **Processing**: Chunk-based MapReduce Simulation
            """)
            
    with col2:
        st.subheader("🏛 MapReduce Architecture")
        st.info("""
        ```
        Raw Log Records (Input)
                  │
        ┌─────────┴─────────┐ (Chunk Splits)
        ▼                   ▼
    [Mapper 1]          [Mapper 2]
    (Key, 1)            (Key, 1)
        │                   │
        └─────────┬─────────┘
                  ▼
          [Shuffle & Sort]
        (Key, [1, 1, 1...])
                  │
        ┌─────────┴─────────┐ (Partitions)
        ▼                   ▼
    [Reducer 1]         [Reducer 2]
    Sum(Key)            Sum(Key)
        │                   │
        └─────────┬─────────┘
                  ▼
         [Analytics Output]
        ```
        """)
        
    st.divider()
    st.subheader("📋 Student Examination Guide")
    st.markdown("""
    When presenting this project during viva or lab exhibitions:
    1. Go to **Log File Upload** to demonstrate loading custom log files or testing edge cases (e.g. error-free files or corrupted lines).
    2. Open **MapReduce Processing** to inspect intermediate key-value generation and show the difference between local chunk simulation and Hadoop YARN/HDFS.
    3. Demonstrate the **Log Search Engine** with real-time keyword and regex queries.
    4. View the **Analytics Dashboard** to show live charts updating dynamically from real reduced data.
    """)

# -----------------------------------------------------------------------------
# MODULE 2: LOG FILE UPLOAD
# -----------------------------------------------------------------------------
elif module == "Log File Upload":
    st.markdown("<h1 class='main-header'>Log File Ingestion & Validation</h1>", unsafe_allow_html=True)
    st.markdown("<p class='sub-header'>Upload custom server log files (.csv, .txt, .log) or use the realistic sample dataset.</p>", unsafe_allow_html=True)
    
    col1, col2 = st.columns([2, 1])
    with col1:
        uploaded_file = st.file_uploader("Choose a CSV or TXT log file", type=["csv", "txt", "log"])
        if uploaded_file is not None:
            raw_bytes = uploaded_file.read()
            raw_text = raw_bytes.decode("utf-8", errors="replace")
            
            is_valid, err_msg, stats = validate_uploaded_content(raw_text, uploaded_file.name)
            if not is_valid:
                st.error(f"Validation Error: {err_msg}")
            else:
                if uploaded_file.name.endswith(".csv"):
                    records = parse_csv_content(raw_text)
                else:
                    records = parse_txt_content(raw_text)
                    
                audit = audit_parsed_records(records)
                st.session_state.records = records
                st.session_state.filename = uploaded_file.name
                st.session_state.file_size_bytes = len(raw_bytes)
                
                # Execute pipeline automatically
                run_mapreduce_pipeline(records)
                
                st.success(f"Successfully loaded and parsed {len(records):,} records from {uploaded_file.name}!")
                st.metric("Healthy Record Rate", f"{audit.get('healthy_rate', 100)}%")
                
    with col2:
        st.subheader("Quick Actions")
        if st.button("Load Built-in 650 Record Sample", use_container_width=True):
            load_sample_dataset()
            st.success("Loaded 650 sample records!")
            st.rerun()
            
        st.markdown("""
        **Supported Formats:**
        - **CSV**: Columns like `timestamp`, `log_level`, `message`, `service`, `source_ip`
        - **TXT/LOG**: Standard server log strings:
          `2026-03-10 08:14:02 ERROR [auth-service] [10.0.4.12] Connection timed out`
        """)

    st.divider()
    st.subheader(f"Data Preview (First 15 Records of {len(st.session_state.records):,})")
    if st.session_state.records:
        df_preview = pd.DataFrame(st.session_state.records[:15])
        st.dataframe(df_preview, use_container_width=True)
    else:
        st.warning("No records currently loaded.")

# -----------------------------------------------------------------------------
# MODULE 3: MAPREDUCE PROCESSING ENGINE
# -----------------------------------------------------------------------------
elif module == "MapReduce Processing":
    st.markdown("<h1 class='main-header'>MapReduce Processing Engine</h1>", unsafe_allow_html=True)
    st.markdown("<p class='sub-header'>Interactive execution and inspection of the 4 MapReduce stages.</p>", unsafe_allow_html=True)
    
    col_ctrl1, col_ctrl2, col_ctrl3 = st.columns(3)
    with col_ctrl1:
        chunk_size = st.select_slider("Chunk Size (Simulated Block Split)", options=[25, 50, 100, 200, 500], value=100)
    with col_ctrl2:
        num_reducers = st.select_slider("Reducer Worker Nodes", options=[1, 2, 4, 8], value=4)
    with col_ctrl3:
        if st.button("⚡ Re-run MapReduce Pipeline", type="primary", use_container_width=True):
            run_mapreduce_pipeline(st.session_state.records, chunk_size=chunk_size, num_reducers=num_reducers)
            st.success("Pipeline executed successfully!")
            
    st.divider()
    
    # Execution Metrics
    metrics = st.session_state.mr_metrics
    if metrics:
        m_col1, m_col2, m_col3, m_col4 = st.columns(4)
        m_col1.metric("Total Execution Time", f"{metrics.get('total_time_ms', 0):.2f} ms")
        m_col2.metric("Map Phase Time", f"{metrics.get('map_time_ms', 0):.2f} ms")
        m_col3.metric("Shuffle/Sort Time", f"{metrics.get('shuffle_time_ms', 0):.2f} ms")
        m_col4.metric("Throughput", f"{int(metrics.get('throughput_rps', 0)):,} rec/sec")
        
    st.divider()
    
    # 4 Stage Tabs
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "1. Mapper Output",
        "2. Shuffle & Sort",
        "3. Reducer Aggregation",
        "4. Final Output",
        "Local vs Hadoop Cluster"
    ])
    
    res = st.session_state.mr_results
    if res:
        with tab1:
            st.subheader("Stage 1: Mapper Output (Key-Value Emissions)")
            st.markdown(f"Total intermediate pairs emitted across **{res['chunks_count']} chunk tasks**: `{res['mapper_pairs_count']:,}` pairs.")
            st.caption("Sample of emitted intermediate (Key, Value) pairs:")
            sample_df = pd.DataFrame(res["mapper_sample"], columns=["Emitted Key", "Emitted Value (Count)"])
            st.dataframe(sample_df, use_container_width=True)
            
        with tab2:
            st.subheader("Stage 2: Shuffle & Sort Barrier")
            ss_data = res["shuffle_data"]
            st.write(f"**Unique Keys Grouped:** `{ss_data['metrics']['unique_keys_count']}`")
            st.write(f"**Reducer Node Partitions:** `{ss_data['metrics']['reducers_count']}`")
            
            # Show partition breakdown
            part_sizes = ss_data["metrics"]["partition_sizes"]
            p_cols = st.columns(len(part_sizes))
            for i, (p_name, p_size) in enumerate(part_sizes.items()):
                p_cols[i].metric(p_name, f"{p_size} keys")
                
            st.markdown("#### Sample Grouped Keys `(Key, [1, 1, 1...])`")
            grouped_sample = {k: ss_data["grouped_data"][k][:10] for k in list(ss_data["grouped_data"].keys())[:8]}
            st.json(grouped_sample)
            
        with tab3:
            st.subheader("Stage 3: Reducer Processing")
            st.markdown("Reducers fold the grouped values using sum aggregation: `Reduce(Key, List[Values]) -> (Key, Sum)`.")
            reduced = res["reduced_output"]
            red_df = pd.DataFrame(list(reduced["raw_reduced"].items())[:20], columns=["Reduced Key", "Final Aggregated Count"])
            st.dataframe(red_df, use_container_width=True)
            
        with tab4:
            st.subheader("Stage 4: Final Output Matrix")
            kpi_dict = reduced["kpis"]
            k1, k2, k3, k4, k5 = st.columns(5)
            k1.metric("Total Records", f"{kpi_dict['total_records']:,}")
            k2.metric("Total Errors", f"{kpi_dict['total_errors']:,}", delta=f"{kpi_dict['error_percentage']}%", delta_color="inverse")
            k3.metric("Warnings", f"{kpi_dict['total_warnings']:,}")
            k4.metric("Info Logs", f"{kpi_dict['total_info']:,}")
            k5.metric("Debug Logs", f"{kpi_dict['total_debug']:,}")
            
        with tab5:
            st.subheader("Local Python Simulation vs. Apache Hadoop Distributed Execution")
            st.markdown("""
            | Architectural Feature | Local Python Simulation (This Project) | Apache Hadoop Cluster (Production) |
            | :--- | :--- | :--- |
            | **Execution Environment** | Single machine, in-memory chunking | Multi-node cluster managed by YARN Resource Manager |
            | **Storage Layer** | Local RAM / CSV files | Hadoop Distributed File System (HDFS) with 3x replication |
            | **Data Locality** | Python generator chunks | Map tasks scheduled on physical DataNode hosting the block |
            | **Shuffle Protocol** | In-memory hash dictionary | Disk spill, HTTP transfer between DataNodes, in-memory merger |
            | **Fault Tolerance** | Exception handling / re-run | Automatic speculative execution and task re-scheduling |
            | **Scaling Limit** | Limited by single system RAM | Scales horizontally to petabytes across thousands of nodes |
            """)

# -----------------------------------------------------------------------------
# MODULE 4: LOG SEARCH ENGINE
# -----------------------------------------------------------------------------
elif module == "Log Search Engine":
    st.markdown("<h1 class='main-header'>Interactive Log Search Engine</h1>", unsafe_allow_html=True)
    st.markdown("<p class='sub-header'>Search server logs by keyword, regex, severity level, or error signatures.</p>", unsafe_allow_html=True)
    
    col1, col2, col3, col4 = st.columns([3, 1, 1, 1])
    with col1:
        keyword = st.text_input("🔍 Search Query (Message, IP, Service, or Signature)", placeholder="e.g. PostgreSQL, 502, connection pool, 192.168...")
    with col2:
        level_filter = st.selectbox("Log Level", ["ALL", "ERROR", "CRITICAL", "WARNING", "INFO", "DEBUG"])
    with col3:
        errors_only = st.checkbox("Errors Only", value=False)
    with col4:
        use_regex = st.checkbox("Regex Mode", value=False)
        
    filtered = filter_records(
        st.session_state.records,
        keyword=keyword,
        log_level=level_filter,
        errors_only=errors_only,
        is_regex=use_regex
    )
    
    st.write(f"Displaying **{len(filtered):,}** matching records out of **{len(st.session_state.records):,}** total.")
    if filtered:
        df_filtered = pd.DataFrame(filtered)
        st.dataframe(df_filtered, use_container_width=True)
    else:
        st.info("No records matched your search criteria.")

# -----------------------------------------------------------------------------
# MODULE 5: ERROR CLASSIFICATION
# -----------------------------------------------------------------------------
elif module == "Error Classification":
    st.markdown("<h1 class='main-header'>Error Detection & Classification</h1>", unsafe_allow_html=True)
    st.markdown("<p class='sub-header'>Rule-based classification identifying critical failures and anomalies.</p>", unsafe_allow_html=True)
    
    st.subheader("Configured Detection Rules")
    rules_df = pd.DataFrame([
        {
            "Rule ID": r["id"],
            "Category": r["category"],
            "Severity": r["severity"],
            "Pattern Keywords": ", ".join(r["patterns"]),
            "Technical Description": r["description"]
        }
        for r in CLASSIFICATION_RULES
    ])
    st.dataframe(rules_df, use_container_width=True)
    
    st.divider()
    st.subheader("Classified Error Records Breakdown")
    
    error_records = [r for r in st.session_state.records if r.get("log_level") in ["ERROR", "CRITICAL", "FATAL", "WARNING"]]
    if error_records:
        classified_list = []
        for r in error_records:
            cl = classify_record(r)
            classified_list.append({
                "Timestamp": r.get("timestamp"),
                "Level": r.get("log_level"),
                "Category": cl["category"],
                "Rule": cl["rule_id"],
                "Message": r.get("message")
            })
        st.dataframe(pd.DataFrame(classified_list), use_container_width=True)

# -----------------------------------------------------------------------------
# MODULE 6: ANALYTICS DASHBOARD
# -----------------------------------------------------------------------------
elif module == "Analytics Dashboard":
    st.markdown("<h1 class='main-header'>Analytics Dashboard</h1>", unsafe_allow_html=True)
    st.markdown("<p class='sub-header'>Visual intelligence derived from MapReduce aggregated outputs.</p>", unsafe_allow_html=True)
    
    if st.session_state.mr_results:
        red = st.session_state.mr_results["reduced_output"]
        kpis = red["kpis"]
        
        # KPI Cards
        k1, k2, k3, k4, k5, k6 = st.columns(6)
        k1.metric("Total Records", f"{kpis['total_records']:,}")
        k2.metric("Total Errors", f"{kpis['total_errors']:,}")
        k3.metric("Warnings", f"{kpis['total_warnings']:,}")
        k4.metric("Info Logs", f"{kpis['total_info']:,}")
        k5.metric("Debug Logs", f"{kpis['total_debug']:,}")
        k6.metric("Error Rate", f"{kpis['error_percentage']}%")
        
        st.divider()
        
        # Charts Row 1
        c1, c2 = st.columns(2)
        with c1:
            st.subheader("Log Level Severity Distribution")
            lvl_data = red["level_distribution"]
            fig_pie = px.pie(
                names=list(lvl_data.keys()),
                values=list(lvl_data.values()),
                hole=0.45,
                color_discrete_sequence=px.colors.qualitative.Safe
            )
            fig_pie.update_layout(margin=dict(t=10, b=10, l=10, r=10))
            st.plotly_chart(fig_pie, use_container_width=True)
            
        with c2:
            st.subheader("Error Category Distribution")
            err_cat_data = red["error_categories"]
            fig_bar = px.bar(
                x=list(err_cat_data.values()),
                y=list(err_cat_data.keys()),
                orientation="h",
                labels={"x": "Total Count", "y": "Error Category"},
                color=list(err_cat_data.values()),
                color_continuous_scale="Reds"
            )
            fig_bar.update_layout(margin=dict(t=10, b=10, l=10, r=10), showlegend=False)
            st.plotly_chart(fig_bar, use_container_width=True)
            
        st.divider()
        
        # Top Error Signatures Leaderboard
        st.subheader("Top Root-Cause Error Signatures")
        top_sigs = red["top_error_signatures"]
        if top_sigs:
            top_df = pd.DataFrame(top_sigs, columns=["Normalized Error Signature", "Frequency Count"])
            fig_sigs = px.bar(
                top_df,
                x="Frequency Count",
                y="Normalized Error Signature",
                orientation="h",
                color="Frequency Count",
                color_continuous_scale="Purples"
            )
            fig_sigs.update_layout(yaxis=dict(autorange="reversed"), margin=dict(t=10, b=10, l=10, r=10))
            st.plotly_chart(fig_sigs, use_container_width=True)

# -----------------------------------------------------------------------------
# MODULE 7: REPORT GENERATION
# -----------------------------------------------------------------------------
elif module == "Report Generation":
    st.markdown("<h1 class='main-header'>Analytical Report Generation & Exports</h1>", unsafe_allow_html=True)
    st.markdown("<p class='sub-header'>Export processed log records, error datasets, and academic evaluation reports.</p>", unsafe_allow_html=True)
    
    col1, col2, col3 = st.columns(3)
    
    # Export 1: All Processed Records
    with col1:
        st.markdown("### 1. Processed Records CSV")
        st.write("Complete structured dataset parsed and normalized by the pipeline.")
        all_csv = generate_csv_string(st.session_state.records)
        st.download_button(
            label="📥 Download All Records (CSV)",
            data=all_csv,
            file_name=f"processed_{st.session_state.filename}",
            mime="text/csv",
            use_container_width=True
        )
        
    # Export 2: Error Records Only
    with col2:
        st.markdown("### 2. Error Records Only CSV")
        st.write("Filtered dataset containing strictly ERROR and CRITICAL severity lines.")
        error_records = [r for r in st.session_state.records if r.get("log_level") in ["ERROR", "CRITICAL", "FATAL"]]
        err_csv = generate_csv_string(error_records)
        st.download_button(
            label="📥 Download Errors Only (CSV)",
            data=err_csv,
            file_name=f"errors_only_{st.session_state.filename}",
            mime="text/csv",
            use_container_width=True
        )
        
    # Export 3: MapReduce Summary CSV
    with col3:
        st.markdown("### 3. MapReduce Summary CSV")
        st.write("Aggregated key-value statistics and root-cause frequency matrices.")
        if st.session_state.mr_results:
            summary_csv = generate_summary_csv(st.session_state.mr_results["reduced_output"])
            st.download_button(
                label="📥 Download Summary (CSV)",
                data=summary_csv,
                file_name="mapreduce_summary.csv",
                mime="text/csv",
                use_container_width=True
            )
            
    st.divider()
    
    # Academic Project Analysis Report
    st.subheader("Academic Project Analysis Report")
    if st.session_state.mr_results:
        report_text = generate_academic_report(
            filename=st.session_state.filename,
            file_size_bytes=st.session_state.file_size_bytes,
            records_count=len(st.session_state.records),
            reduced_results=st.session_state.mr_results["reduced_output"],
            mr_execution_time_ms=st.session_state.mr_metrics.get("total_time_ms", 0)
        )
        
        st.download_button(
            label="📄 Download Comprehensive Academic Report (.txt)",
            data=report_text,
            file_name="Big_Data_MapReduce_Log_Analysis_Report.txt",
            mime="text/plain"
        )
        
        st.text_area("Report Preview", value=report_text, height=380)
