"""
utils/validators.py
===================
Validation functions to check input log files, detect schema anomalies,
empty streams, and corrupted records.
"""

from typing import Tuple, Dict, Any, List

def validate_uploaded_content(content_str: str, filename: str) -> Tuple[bool, str, Dict[str, Any]]:
    """
    Validates uploaded raw string content before running the pipeline.
    
    Returns:
        (is_valid, error_message_if_any, validation_stats)
    """
    if not content_str or not content_str.strip():
        return False, "File is completely empty. Please upload a valid log file.", {}
        
    lines = content_str.strip().splitlines()
    total_lines = len(lines)
    
    if total_lines == 0:
        return False, "No data lines found in file.", {}
        
    ext = filename.split(".")[-1].lower() if "." in filename else ""
    if ext not in ["csv", "txt", "log"]:
        return False, f"Unsupported file extension '.{ext}'. Supported extensions are .csv, .txt, and .log.", {}
        
    stats = {
        "total_lines": total_lines,
        "sample_preview": lines[:5],
        "format_detected": "CSV" if ext == "csv" or ("," in lines[0] and "timestamp" in lines[0].lower()) else "TXT/LOG"
    }
    
    return True, "", stats

def audit_parsed_records(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Audits parsed records to identify corrupted lines or missing attributes."""
    if not records:
        return {"total": 0, "healthy": 0, "corrupted": 0, "status": "EMPTY"}
        
    healthy = 0
    corrupted = 0
    
    for r in records:
        if r.get("timestamp") and r.get("log_level") and r.get("message"):
            healthy += 1
        else:
            corrupted += 1
            
    return {
        "total": len(records),
        "healthy": healthy,
        "corrupted": corrupted,
        "healthy_rate": round((healthy / len(records)) * 100, 2)
    }
