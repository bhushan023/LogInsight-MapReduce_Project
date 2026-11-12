"""
analytics.py
============
Module providing analytical query execution, rule-based classification definitions,
and log filtering operations on parsed records.
"""

import re
from typing import List, Dict, Any, Optional

CLASSIFICATION_RULES = [
    {
        "id": "RULE_CRITICAL_OOM",
        "category": "Memory / OOM Crash",
        "severity": "CRITICAL",
        "patterns": ["outofmemory", "oom-killer", "heap space", "failed to allocate"],
        "description": "JVM heap exhaustion or Linux kernel Out-Of-Memory killer terminating worker processes."
    },
    {
        "id": "RULE_DB_DEADLOCK",
        "category": "Database Deadlock / Pool",
        "severity": "CRITICAL",
        "patterns": ["deadlock detected", "connection pool exhausted", "failed to acquire write lock"],
        "description": "RDBMS resource contention, table locking deadlocks, or exhausted connection pool limits."
    },
    {
        "id": "RULE_DB_CONN",
        "category": "Database Connectivity",
        "severity": "ERROR",
        "patterns": ["connection refused", "could not connect to postgresql", "query execution timeout"],
        "description": "Database server unreachable, network drop, or long-running query timeout."
    },
    {
        "id": "RULE_AUTH_BREACH",
        "category": "Authentication & Security",
        "severity": "ERROR",
        "patterns": ["invalid jwt", "unauthorized access", "repeated failed login", "permission denied", "invalid_grant"],
        "description": "Expired or forged tokens, privilege escalation attempts, or brute-force logins."
    },
    {
        "id": "RULE_HTTP_UPSTREAM",
        "category": "HTTP 5xx Upstream Outage",
        "severity": "ERROR",
        "patterns": ["500 internal server", "502 bad gateway", "503 service unavailable", "504 gateway timeout"],
        "description": "Microservice communication breakdown or downstream API gateway failures."
    },
    {
        "id": "RULE_TIMEOUT",
        "category": "Network & Socket Timeout",
        "severity": "ERROR",
        "patterns": ["connection timed out", "sockettimeoutexception", "exceeded deadline", "read timeout"],
        "description": "Slow network hops, Redis cache unresponsive, or latency SLA breaches."
    },
    {
        "id": "RULE_STORAGE_CAPACITY",
        "category": "Storage & Disk Warning",
        "severity": "WARNING",
        "patterns": ["disk usage", "volume /var/data", "quota exceeded"],
        "description": "Storage partitions approaching physical capacity limits (>80%)."
    },
    {
        "id": "RULE_PERF_DEGRADATION",
        "category": "Performance Degradation",
        "severity": "WARNING",
        "patterns": ["slow sql query", "cpu utilization", "latency spiked", "deprecated api"],
        "description": "Sub-optimal queries, high CPU load, or legacy endpoint deprecation warnings."
    }
]

def classify_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Applies rule-based engine to classify an individual log record.
    Returns matched rule information or default classification.
    """
    msg_lower = (record.get("message") or "").lower()
    level = (record.get("log_level") or "INFO").upper()
    
    for rule in CLASSIFICATION_RULES:
        if any(pat in msg_lower for pat in rule["patterns"]):
            return {
                "rule_id": rule["id"],
                "category": rule["category"],
                "assigned_severity": rule["severity"],
                "explanation": rule["description"],
                "is_error": rule["severity"] in ["CRITICAL", "ERROR"]
            }
            
    is_err = level in ["ERROR", "CRITICAL", "FATAL"]
    return {
        "rule_id": "RULE_DEFAULT_LEVEL",
        "category": "General Event" if not is_err else "General Error",
        "assigned_severity": level,
        "explanation": f"Classified by explicit severity level header: {level}",
        "is_error": is_err
    }

def filter_records(
    records: List[Dict[str, Any]],
    keyword: str = "",
    log_level: str = "ALL",
    service: str = "ALL",
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    errors_only: bool = False,
    is_regex: bool = False
) -> List[Dict[str, Any]]:
    """
    Filters records based on multiple combined search parameters.
    """
    results: List[Dict[str, Any]] = []
    
    # Pre-compile regex if requested
    compiled_regex = None
    if is_regex and keyword.strip():
        try:
            compiled_regex = re.compile(keyword.strip(), re.IGNORECASE)
        except re.error:
            compiled_regex = None

    kw_lower = keyword.strip().lower()

    for r in records:
        # 1. Level filter
        lvl = r.get("log_level", "").upper()
        if log_level != "ALL" and lvl != log_level.upper():
            continue
            
        # 2. Errors only filter
        if errors_only and lvl not in ["ERROR", "CRITICAL", "FATAL"]:
            continue
            
        # 3. Service filter
        srv = r.get("service", "")
        if service != "ALL" and srv.lower() != service.lower():
            continue
            
        # 4. Timestamp filtering
        ts = r.get("timestamp", "")
        if start_time and ts < start_time:
            continue
        if end_time and ts > end_time:
            continue
            
        # 5. Keyword search (message, service, or source_ip)
        if kw_lower:
            if compiled_regex:
                if not (
                    compiled_regex.search(r.get("message", "")) or
                    compiled_regex.search(srv) or
                    compiled_regex.search(r.get("source_ip", ""))
                ):
                    continue
            else:
                msg_lower = r.get("message", "").lower()
                ip_lower = r.get("source_ip", "").lower()
                if kw_lower not in msg_lower and kw_lower not in srv.lower() and kw_lower not in ip_lower:
                    continue
                    
        results.append(r)
        
    return results
