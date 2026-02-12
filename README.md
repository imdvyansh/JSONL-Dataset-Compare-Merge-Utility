# JSONL-Dataset-Compare-Merge-Utility
JSONL Merge Utility is a deterministic merge system that consolidates two JSONL datasets using uid as a primary key. It applies set-based logic where Dataset B overrides Dataset A on conflicts, ensuring uniqueness and reproducible results.Built on SQLite as a disk-backed indexed store, it enables efficient, memory-safe processing of large datasets.
