"""
shuffle_sort.py
===============
Module implementing the Shuffle and Sort phase of the MapReduce programming model.

Responsibilities:
1. Shuffle: Groups intermediate (Key, Value) pairs from all Map tasks by key.
2. Sort: Orders the partitioned keys lexicographically or by frequency.
3. Partitioning: Simulates hash-partitioning to assign keys to specific Reducer tasks.
"""

from collections import defaultdict
from typing import List, Tuple, Dict, Any

def shuffle_and_sort(intermediate_pairs: List[Tuple[str, int]], num_reducers: int = 4) -> Dict[str, Any]:
    """
    Simulates the MapReduce Shuffle and Sort barrier.
    
    Returns:
        A dictionary containing:
        - 'grouped_data': Dict mapping each unique Key to its list of values [1, 1, 1...]
        - 'sorted_keys': Alphabetically sorted list of unique keys
        - 'partitions': Data partitioned into simulated reducer buckets based on hash(key) % num_reducers
        - 'metrics': Statistics about intermediate pairs, unique keys, and partition sizes.
    """
    grouped_data: Dict[str, List[int]] = defaultdict(list)
    
    # 1. Shuffle phase: Collect all values for each key
    for key, value in intermediate_pairs:
        grouped_data[key].append(value)
        
    # 2. Sort phase: Sort the keys alphabetically
    sorted_keys = sorted(grouped_data.keys())
    
    # 3. Partitioning phase: Assign keys to simulated Reducer nodes using hash-partitioning
    partitions: Dict[int, Dict[str, List[int]]] = {r_id: {} for r_id in range(num_reducers)}
    for key in sorted_keys:
        partition_id = abs(hash(key)) % num_reducers
        partitions[partition_id][key] = grouped_data[key]
        
    metrics = {
        "total_intermediate_pairs": len(intermediate_pairs),
        "unique_keys_count": len(grouped_data),
        "reducers_count": num_reducers,
        "partition_sizes": {f"Reducer_{r_id}": len(part) for r_id, part in partitions.items()}
    }
    
    return {
        "grouped_data": dict(grouped_data),
        "sorted_keys": sorted_keys,
        "partitions": partitions,
        "metrics": metrics
    }
