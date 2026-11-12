"""
log_parser.py
=============
Module responsible for parsing server logs in varied formats:
1. Structured CSV (record_id, timestamp, log_level, service, source_ip, message)
2. Raw TXT files (Standard server logs: [TIMESTAMP] [LEVEL] [SERVICE] MESSAGE)
3. Apache/Nginx combined format, Spring Boot, and Syslog format.

Provides robust fallback regex parsing and data cleaning for Big Data pipelines.
"""

import re
import csv
import io
from typing import List, Dict, Any, Optional

# Regex for standard server log lines:
# Examples:
# 2026-03-10 08:01:15 [ERROR] [auth-service] [192.168.1.45] Connection timed out
# 2026-03-10 08:01:15 ERROR auth-service: Failed to connect to database
LOG_LINE_REGEX = re.compile(
    r'^(?P<timestamp>\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:\.\d+)?)\s+'
    r'(?:\[?(?P<level>INFO|WARNING|WARN|ERROR|CRITICAL|FATAL|DEBUG)\]?)\s+'
    r'(?:\[?(?P<service>[\w\-\.]+)\]?\s+)?'
    r'(?:\[?(?P<ip>\d{1,3}(?:\.\d{1,3}){3})\]?\s+)?'
    r'[:\-]?\s*(?P<message>.*)$',
    re.IGNORECASE
)

# Fallback regex to capture at least timestamp, level, and message anywhere in the line
FALLBACK_REGEX = re.compile(
    r'(?P<timestamp>\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2})?'
    r'.*?\b(?P<level>CRITICAL|FATAL|ERROR|WARNING|WARN|INFO|DEBUG)\b'
    r'[:\s\-\]]*(?P<message>.*)',
    re.IGNORECASE
)

def normalize_log_level(level_raw: Optional[str]) -> str:
    """Normalizes various log level string representations to standard levels."""
    if not level_raw:
        return "UNKNOWN"
    lvl = level_raw.strip().upper()
    if lvl == "WARN":
        return "WARNING"
    if lvl == "FATAL":
        return "CRITICAL"
    if lvl in ["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"]:
        return lvl
    return "INFO"

def parse_csv_content(content_str: str) -> List[Dict[str, Any]]:
    """
    Parses a CSV formatted string into a structured list of log records.
    Handles dynamic column mapping (case-insensitive headers).
    """
    records: List[Dict[str, Any]] = []
    reader = csv.reader(io.StringIO(content_str.strip()))
    
    header = next(reader, None)
    if not header:
        return records
    
    # Map column headers to standard names
    header_lower = [h.strip().lower() for h in header]
    
    idx_map = {
        "record_id": -1,
        "timestamp": -1,
        "log_level": -1,
        "service": -1,
        "source_ip": -1,
        "message": -1
    }
    
    for i, col in enumerate(header_lower):
        if "id" in col and idx_map["record_id"] == -1:
            idx_map["record_id"] = i
        elif "time" in col or "date" in col:
            idx_map["timestamp"] = i
        elif "level" in col or "severity" in col:
            idx_map["log_level"] = i
        elif "service" in col or "app" in col or "module" in col:
            idx_map["service"] = i
        elif "ip" in col or "host" in col or "client" in col:
            idx_map["source_ip"] = i
        elif "msg" in col or "message" in col or "text" in col or "log" in col:
            idx_map["message"] = i
            
    # Default message to last column if not found
    if idx_map["message"] == -1 and len(header) > 0:
        idx_map["message"] = len(header) - 1

    row_count = 0
    for row in reader:
        if not row or all(c.strip() == "" for c in row):
            continue
        row_count += 1
        
        def get_val(key: str, default: str = "") -> str:
            idx = idx_map.get(key, -1)
            if 0 <= idx < len(row):
                return row[idx].strip()
            return default
            
        timestamp = get_val("timestamp", "2026-03-10 00:00:00")
        raw_level = get_val("log_level", "INFO")
        service = get_val("service", "core-system")
        source_ip = get_val("source_ip", "127.0.0.1")
        message = get_val("message", f"Log event #{row_count}")
        record_id = get_val("record_id", str(row_count))
        
        try:
            r_id = int(record_id)
        except ValueError:
            r_id = row_count
            
        records.append({
            "record_id": r_id,
            "timestamp": timestamp,
            "log_level": normalize_log_level(raw_level),
            "service": service,
            "source_ip": source_ip,
            "message": message
        })
        
    return records

def parse_txt_content(content_str: str) -> List[Dict[str, Any]]:
    """
    Parses unstructured text log lines into structured dictionaries.
    Uses regex matching with graceful fallback for malformed lines.
    """
    records: List[Dict[str, Any]] = []
    lines = content_str.strip().splitlines()
    
    for i, line in enumerate(lines, start=1):
        line_clean = line.strip()
        if not line_clean:
            continue
            
        match = LOG_LINE_REGEX.match(line_clean)
        if match:
            group = match.groupdict()
            records.append({
                "record_id": i,
                "timestamp": group.get("timestamp") or "2026-03-10 00:00:00",
                "log_level": normalize_log_level(group.get("level")),
                "service": group.get("service") or "server-node",
                "source_ip": group.get("ip") or "127.0.0.1",
                "message": (group.get("message") or "").strip()
            })
        else:
            # Try fallback
            fb_match = FALLBACK_REGEX.search(line_clean)
            if fb_match:
                fb_group = fb_match.groupdict()
                records.append({
                    "record_id": i,
                    "timestamp": fb_group.get("timestamp") or "2026-03-10 00:00:00",
                    "log_level": normalize_log_level(fb_group.get("level")),
                    "service": "app-service",
                    "source_ip": "127.0.0.1",
                    "message": (fb_group.get("message") or line_clean).strip()
                })
            else:
                # Store as unclassified INFO line
                records.append({
                    "record_id": i,
                    "timestamp": "2026-03-10 00:00:00",
                    "log_level": "INFO",
                    "service": "syslog",
                    "source_ip": "127.0.0.1",
                    "message": line_clean
                })
                
    return records
