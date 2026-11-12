"""
utils/file_handler.py
=====================
File utility helpers for reading files in chunks, validating sizes,
and safely handling stream buffers.
"""

import os
from typing import Generator, List

def get_file_size_formatted(num_bytes: int) -> str:
    """Formats bytes into human readable format (B, KB, MB)."""
    for unit in ['bytes', 'KB', 'MB', 'GB']:
        if num_bytes < 1024.0:
            return f"{num_bytes:.2f} {unit}"
        num_bytes /= 1024.0
    return f"{num_bytes:.2f} TB"

def read_in_chunks(records: list, chunk_size: int = 100) -> Generator[list, None, None]:
    """
    Yields successive n-sized chunks from a list of records.
    Simulates Hadoop block splitting (e.g. 64MB / 128MB HDFS block splits).
    """
    for i in range(0, len(records), chunk_size):
        yield records[i:i + chunk_size]
