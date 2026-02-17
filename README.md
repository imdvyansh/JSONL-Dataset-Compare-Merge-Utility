# JSONL-Dataset-Compare-Merge-Utility
JSONL Merge Utility is a deterministic merge system that consolidates two JSONL datasets using uid as a primary key. It applies set-based logic where Dataset B overrides Dataset A on conflicts, ensuring uniqueness and reproducible results.Built on SQLite as a disk-backed indexed store, it enables efficient, memory-safe processing of large datasets.


A deterministic and memory-efficient utility to compare and merge two JSONL datasets using `uid` as a primary key. The tool detects overlapping records, inserts new records, replaces outdated records, and produces a clean merged dataset with guaranteed uniqueness.

---

## Features

- UID-based deterministic merge
- Detects overlapping records between datasets
- Inserts only new records
- Replaces outdated records with latest version
- Memory-efficient streaming processing
- Indexed lookup using SQLite (disk-backed)
- Scalable for large datasets
- Deterministic and reproducible output

---

## How It Works

The utility uses SQLite as a temporary indexed storage engine to enforce uniqueness and enable fast lookup.

### Processing Flow

```
Dataset A → Read → Insert into SQLite
Dataset B → Read → Check overlap → Insert / Replace
SQLite → Export → merged.jsonl
```

SQLite acts as a key-value store:

```
uid → JSON record
```

Dataset B overrides Dataset A if the same UID exists.

---

## Project Structure

```
JSONL-Dataset-Compare-Merge-Utility/
│
├── jsonl_compare.py
├── data/
│   └── .gitkeep
├── .gitignore
└── README.md
└── README_doc.md
```

Place your input files inside the `data/` folder:

```
data/
├── input_1.jsonl
├── input_2.jsonl
```

---

## Input Format

Each file must be in JSON Lines format:

```
{"uid": "123", "name": "John"}
{"uid": "124", "name": "Alice"}
```

Each record must contain a unique `uid` field.

---

## Output

The script generates:

```
data/merged.jsonl
```

and prints statistics:

```
same_uid_count=...
inserted_from_B=...
merged_total_unique=...
output=data/merged.jsonl
```

---

## Statistics Explanation

| Metric | Description |
|------|-------------|
| same_uid_count | Number of records present in both datasets |
| inserted_from_B | New records added from Dataset B |
| merged_total_unique | Total unique records after merge |
| output | Path of merged output file |

---

## Mathematical Model

Let:

```
U_A = Set of UIDs in Dataset A
U_B = Set of UIDs in Dataset B
```

Overlap:

```
|U_A ∩ U_B|
```

New records:

```
|U_B − U_A|
```

Final merged dataset:

```
|U_A ∪ U_B|
```

---

## Conflict Resolution Policy

If the same UID exists in both datasets:

```
Dataset B overrides Dataset A
```

This ensures deterministic and predictable merging.

---

## Complexity

Time Complexity:

```
O((n + m) log n)
```

Space Complexity:

```
O(n + m) disk usage
O(1) memory usage
```

---

## Requirements

Python 3.8+

No external dependencies required.

Uses built-in modules:

```
json
sqlite3
pathlib
```

---

## Usage

Run the script:

```
python jsonl_compare.py
```

Example output:

```
same_uid_count=100
inserted_from_B=200
merged_total_unique=1200
output=data/merged.jsonl
```

---

## Error Handling

The script validates each record and raises an error if UID is missing:

```
ValueError: missing uid at line X
```

This ensures data integrity.

---

## Why SQLite is Used

SQLite provides:

- Fast indexed lookup
- Guaranteed uniqueness
- Disk-based storage
- Low memory usage
- High scalability

SQLite uses B-tree indexing internally for efficient operations.

---

## Use Cases

- Incremental dataset ingestion
- ETL pipelines
- Data deduplication
- Watchlist / sanctions merging
- Database synchronization
- Elasticsearch indexing preparation

---

## License

MIT License
