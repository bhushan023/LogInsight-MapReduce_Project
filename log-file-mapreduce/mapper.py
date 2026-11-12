"""
mapper.py
=========
Module implementing the Mapper phase of the MapReduce programming model.

The Mapper function processes individual log records or partitioned chunks,
extracts meaningful dimensions (severity, error categories, keywords, temporal slots),
and emits intermediate Key-Value pairs: (Key, Value).
"""

import re
from typing import List, Tuple, Dict, Any

# Predefined error keyword dictionary for categorization
ERROR_SIGNATURES = {
    "DATABASE": [
        "database", "postgresql", "mysql", "deadlock", "connection pool",
        "write lock", "transaction", "sql", "shard"
    ],
    "AUTHENTICATION": [
        "authentication", "jwt", "unauthorized", "oauth", "permission denied",
        "login attempt", "forbidden", "invalid signature"
    ],
    "HTTP_5XX": [
        "500 internal server error", "502 bad gateway", "503 service unavailable",
        "504 gateway timeout", "500", "502", "503", "504"
    ],
    "TIMEOUT": [
        "timed out", "timeout", "sockettimeout", "read timeout", "deadline"
    ],
    "MEMORY_OOM": [
        "outofmemory", "oom-killer", "heap space", "memory leak", "ram"
    ],
    "DISK_STORAGE": [
        "disk usage", "volume", "storage threshold", "quota exceeded"
    ]
}

def extract_error_category(message: str) -> str:
    """Classifies an error message into standard Big Data error categories."""
    msg_lower = message.lower()
    for category, keywords in ERROR_SIGNATURES.items():
        if any(kw in msg_lower for kw in keywords):
            return category
    return "GENERAL_ERROR"

def extract_error_signature(message: str) -> str:
    """
    Extracts a generalized signature by stripping dynamic numbers/IDs,
    allowing the reducer to aggregate identical root-cause errors.
    """
    clean = message.strip()
    # Remove ERROR: / CRITICAL: prefixes
    clean = re.sub(r'^(?:CRITICAL|FATAL|ERROR|WARNING):\s*', '', clean, flags=re.IGNORECASE)
    # Replace variable numbers, IDs, IPs with placeholders
    clean = re.sub(r'\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b', '<IP>', clean)
    clean = re.sub(r'\b(?:usr_|ORD-|ak_live_)?\d+\b', '<ID>', clean)
    clean = re.sub(r'duration=\d+ms|latency=\d+ms', 'latency=<MS>', clean)
    return clean[:90]

def map_single_record(record: Dict[str, Any]) -> List[Tuple[str, int]]:
    """
    Mapper function applied to a single record.
    Generates a list of (Key, Value) tuples.
    """
    emitted_pairs: List[Tuple[str, int]] = []
    
    level = record.get("log_level", "INFO").upper()
    timestamp = record.get("timestamp", "2026-03-10 00:00:00")
    service = record.get("service", "unknown-service")
    message = record.get("message", "")
    
    # Key 1: Total records count
    emitted_pairs.append(("METRIC:TOTAL_RECORDS", 1))
    
    # Key 2: Log Level frequency
    emitted_pairs.append((f"LEVEL:{level}", 1))
    
    # Key 3: Service-level distribution
    emitted_pairs.append((f"SERVICE:{service}", 1))
    
    # Key 4: Temporal hour slot (e.g., '2026-03-10 08:00')
    try:
        hour_slot = timestamp[:13] + ":00"
    except Exception:
        hour_slot = "2026-03-10 00:00"
    emitted_pairs.append((f"TIMELINE:{hour_slot}:{level}", 1))
    
    # If the record is ERROR, CRITICAL or WARNING, generate diagnostic keys
    if level in ["ERROR", "CRITICAL", "FATAL"]:
        emitted_pairs.append(("METRIC:TOTAL_ERRORS", 1))
        
        category = extract_error_category(message)
        emitted_pairs.append((f"ERROR_CAT:{category}", 1))
        
        signature = extract_error_signature(message)
        emitted_pairs.append((f"ERROR_SIG:{signature}", 1))
        
    elif level == "WARNING":
        emitted_pairs.append(("METRIC:TOTAL_WARNINGS", 1))
        
    return emitted_pairs

def chunk_mapper(records_chunk: List[Dict[str, Any]], worker_id: int = 1) -> List[Tuple[str, int]]:
    """
    Executes mapping on a chunk of records, simulating a distributed Map worker node.
    Returns aggregated intermediate key-value list for this chunk.
    """
    intermediate_pairs: List[Tuple[str, int]] = []
    for record in records_chunk:
        pairs = map_single_record(record)
        intermediate_pairs.extend(pairs)
    return intermediate_pairs
