"""
reducer.py
==========
Module implementing the Reducer phase of the MapReduce programming model.

Responsibilities:
1. Iterates over grouped (Key, [Values]) streams.
2. Applies reduction operations (sum, aggregation, percentiles).
3. Produces final aggregated analytical matrices and statistical KPIs.
"""

from typing import Dict, List, Any

def reduce_grouped_stream(grouped_data: Dict[str, List[int]]) -> Dict[str, Any]:
    """
    Executes reduction across all keys.
    Computes sums, ratios, and organizes outputs into structured business intelligence schemas.
    """
    reduced_raw: Dict[str, int] = {}
    
    # 1. Primary reduction: sum(values)
    for key, values_list in grouped_data.items():
        reduced_raw[key] = sum(values_list)
        
    # 2. Decompose reduced keys into domain dimensions
    total_records = reduced_raw.get("METRIC:TOTAL_RECORDS", 0)
    total_errors = reduced_raw.get("METRIC:TOTAL_ERRORS", 0)
    total_warnings = reduced_raw.get("METRIC:TOTAL_WARNINGS", 0)
    
    level_counts: Dict[str, int] = {}
    service_counts: Dict[str, int] = {}
    error_categories: Dict[str, int] = {}
    error_signatures: Dict[str, int] = {}
    timeline: Dict[str, Dict[str, int]] = {}
    
    for key, count in reduced_raw.items():
        if key.startswith("LEVEL:"):
            lvl = key.split(":", 1)[1]
            level_counts[lvl] = count
        elif key.startswith("SERVICE:"):
            srv = key.split(":", 1)[1]
            service_counts[srv] = count
        elif key.startswith("ERROR_CAT:"):
            cat = key.split(":", 1)[1]
            error_categories[cat] = count
        elif key.startswith("ERROR_SIG:"):
            sig = key.split(":", 1)[1]
            error_signatures[sig] = count
        elif key.startswith("TIMELINE:"):
            parts = key.split(":")
            if len(parts) >= 3:
                hour = f"{parts[1]}:{parts[2]}"
                lvl = parts[3] if len(parts) > 3 else "INFO"
                if hour not in timeline:
                    timeline[hour] = {"ERROR": 0, "WARNING": 0, "INFO": 0, "DEBUG": 0}
                if lvl in timeline[hour]:
                    timeline[hour][lvl] += count
                else:
                    timeline[hour][lvl] = count

    # Calculate derived KPIs
    info_count = level_counts.get("INFO", 0)
    debug_count = level_counts.get("DEBUG", 0)
    
    # Accurate error count including CRITICAL
    actual_errors = (
        level_counts.get("ERROR", 0) +
        level_counts.get("CRITICAL", 0) +
        level_counts.get("FATAL", 0)
    )
    if actual_errors == 0 and total_errors > 0:
        actual_errors = total_errors
        
    error_rate = (actual_errors / total_records * 100.0) if total_records > 0 else 0.0
    warning_rate = (total_warnings / total_records * 100.0) if total_records > 0 else 0.0
    
    # Sort top error signatures by frequency descending
    sorted_signatures = sorted(error_signatures.items(), key=lambda x: x[1], reverse=True)
    sorted_categories = sorted(error_categories.items(), key=lambda x: x[1], reverse=True)
    sorted_services = sorted(service_counts.items(), key=lambda x: x[1], reverse=True)
    
    return {
        "kpis": {
            "total_records": total_records,
            "total_errors": actual_errors,
            "total_warnings": total_warnings,
            "total_info": info_count,
            "total_debug": debug_count,
            "error_percentage": round(error_rate, 2),
            "warning_percentage": round(warning_rate, 2)
        },
        "level_distribution": level_counts,
        "service_distribution": dict(sorted_services),
        "error_categories": dict(sorted_categories),
        "top_error_signatures": sorted_signatures[:15],
        "timeline": timeline,
        "raw_reduced": reduced_raw
    }
