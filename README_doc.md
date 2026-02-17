# Structural Analysis and Mathematical Formalization of Relational-Buffered JSONL Consolidation Pipelines

## Overview

Modern data engineering pipelines frequently process high-velocity, semi-structured datasets. Ensuring record-level integrity, deduplication, and scalability under memory constraints is critical.

This project implements a **Python-based JSONL consolidation pipeline using SQLite as an intermediate relational buffer** to merge multiple JSONL datasets into a unified, deduplicated **Single Source of Truth**.

The system ensures:

- Deterministic deduplication using relational primary keys  
- Memory-efficient streaming using Python generators  
- ACID-compliant persistence using SQLite  
- Last-Write-Wins (LWW) merge logic  
- Idempotent and crash-safe execution  
- Scalable performance for millions of records  

---

# Table of Contents

- Architecture Overview  
- JSONL Specification  
- Generator-Based Stream Processing  
- SQLite Relational Buffer Design  
- Deduplication Mechanism  
- Mathematical Formalization  
- Computational Complexity  
- ETL Architecture  
- Pipeline Workflow  
- Idempotency and Data Integrity  
- Performance Considerations  
- Data Dictionary  
- Execution Metrics  
- Example Implementation  
- Conclusion  

---

# Architecture Overview

The system uses a hybrid architecture combining streaming extraction with relational deduplication:

```
JSONL File A ─┐
              ├──> Python Generator ───> SQLite Buffer ───> Consolidated JSONL Output
JSONL File B ─┘
```

Components:

| Component | Role |
|---------|------|
| Python Generator | Streams records line-by-line |
| SQLite Database | Deduplication and persistent state |
| Primary Key Index | Ensures uniqueness |
| JSONL Writer | Outputs consolidated dataset |

---

# JSONL Specification

JSON Lines (JSONL), also called NDJSON, stores one valid JSON object per line:

```
{"uid": "1", "name": "Alice"}
{"uid": "2", "name": "Bob"}
{"uid": "3", "name": "Charlie"}
```

Advantages:

- Streaming friendly  
- Memory efficient  
- Append friendly  
- No full-file memory loading required  

Comparison:

| Format | Memory Efficiency | Streaming | Append Friendly |
|------|------------------|----------|----------------|
| JSON | Low | No | No |
| JSONL | High | Yes | Yes |
| CSV | High | Yes | Yes |
| XML | Medium | Limited | No |
| Parquet | High | Block | No |

---

# Generator-Based Stream Processing

Extraction uses a Python generator:

```python
def rows(path):
    with open(path, "r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, 1):
            line = line.strip()

            if not line:
                continue

            obj = json.loads(line)

            if "uid" not in obj:
                raise ValueError(f"Missing uid at line {line_no}")

            yield str(obj["uid"]), line
```

Properties:

Memory Complexity:

```
O(1)
```

Benefits:

- Constant memory usage  
- Streaming processing  
- Immediate validation  
- No memory overflow risk  

---

# SQLite Relational Buffer Design

SQLite staging table schema:

```sql
CREATE TABLE kv(
    uid TEXT PRIMARY KEY,
    j TEXT NOT NULL
);
```

Properties:

- Primary key automatically creates B-Tree index  
- Ensures uniqueness  
- Provides ACID compliance  
- Persistent and crash safe  

---

# Deduplication Mechanism

Deduplication is handled using SQLite UPSERT:

```sql
INSERT OR REPLACE INTO kv(uid, j) VALUES (?, ?)
```

Mechanism:

1. Check if UID exists  
2. If exists → delete old record  
3. Insert new record  
4. Update index  

Implements:

```
Last-Write-Wins (LWW)
```

---

# Mathematical Formalization

Define datasets:

```
A = {(k, vA)}
B = {(k, vB)}
```

Merged result:

```
M = {(k, vB) if k ∈ B
     (k, vA) if k ∈ A and k ∉ B}
```

Union with overwrite:

```
M = B ∪ (A − keys(B))
```

Counting metrics:

```
Total Unique = |A| + |B| − |A ∩ B|
```

Definitions:

| Metric | Meaning |
|------|--------|
| Intersection | Duplicate UIDs |
| Union | Total unique records |
| Difference | New records |

---

# Computational Complexity

Let:

```
n = records in File A
m = records in File B
```

Time complexity:

| Stage | Complexity |
|------|-----------|
| Load A | O(n log n) |
| Process B | O(m log n) |
| Export | O(n + m) |

Memory complexity:

```
O(1)
```

Efficient due to B-Tree indexing.

Naive approach complexity:

```
O(n × m)
```

---

# ETL Architecture

The system follows Extract-Transform-Load architecture.

## Extraction

Python generator reads JSONL stream.

## Transformation

SQLite primary key enforces uniqueness.

## Storage

SQLite provides durable staging.

## Loading

Exports final merged JSONL.

---

# Pipeline Workflow

Execution flow:

```
Initialize Database
        ↓
Create Table
        ↓
Load File A
        ↓
Commit
        ↓
Process File B
        ↓
Replace duplicates
        ↓
Commit
        ↓
Export merged JSONL
        ↓
Print metrics
        ↓
Finish
```

---

# Idempotency and Data Integrity

Database reset ensures deterministic execution:

```python
if DB.exists():
    DB.unlink()
```

Guarantees:

- Clean execution  
- Consistent output  
- Safe retries  

---

# Performance Considerations

Primary bottleneck:

```
Disk I/O
```

Optimizations:

Enable WAL mode:

```sql
PRAGMA journal_mode=WAL;
```

Batch commits improve speed.

---

# Data Dictionary

| Column | Type | Description |
|------|------|-------------|
| uid | TEXT | Unique identifier |
| j | TEXT | Full JSON record |

---

# Execution Metrics

Example output:

```
same_uid_count: 15000
inserted_from_B: 85000
merged_total_unique: 100000
output_file: merged.jsonl
```

Ensures full transparency.

---

# Example Implementation

```python
cur.execute("CREATE TABLE kv(uid TEXT PRIMARY KEY, j TEXT NOT NULL)")

for uid, line in rows(fileA):
    cur.execute("INSERT OR REPLACE INTO kv VALUES (?, ?)", (uid, line))

for uid, line in rows(fileB):
    cur.execute("INSERT OR REPLACE INTO kv VALUES (?, ?)", (uid, line))

for row in cur.execute("SELECT j FROM kv"):
    output.write(row[0] + "\n")
```

---

# System Properties

Memory Efficient  
Scalable  
Crash Safe  
Deterministic  
Idempotent  
Schema Flexible  

---

# Conclusion

This project provides a scalable, reliable, and mathematically sound solution for merging JSONL datasets using relational buffering.

It combines:

- Streaming extraction  
- Relational deduplication  
- Transactional safety  
- Set-theoretic correctness  
- Logarithmic time complexity  

This architecture is suitable for:

- ETL pipelines  
- Data lake consolidation  
- Log merging systems  
- Incremental ingestion pipelines  

---

# License

MIT License
