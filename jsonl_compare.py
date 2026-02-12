import json, sqlite3
from pathlib import Path

BASE = Path(__file__).resolve().parent

A   = BASE / "data" / "input_20260120.jsonl"
B   = BASE / "data" / "input_20260209.jsonl"
OUT = BASE / "data" / "merged.jsonl"
DB  = BASE / "tmp.sqlite"

def rows(p: Path):
    with p.open("r", encoding="utf-8") as f:
        for i, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            if "uid" not in obj:
                raise ValueError(f"{p.name}: missing uid at line {i}")
            yield str(obj["uid"]), line

def main():
    OUT.parent.mkdir(parents=True, exist_ok=True)

    if DB.exists():
        DB.unlink()

    con = sqlite3.connect(str(DB))
    cur = con.cursor()
    cur.execute("CREATE TABLE kv(uid TEXT PRIMARY KEY, j TEXT NOT NULL)")

    for uid, line in rows(A):
        cur.execute("INSERT OR REPLACE INTO kv VALUES(?,?)", (uid, line))
    con.commit()

    same = b_cnt = 0
    for uid, line in rows(B):
        cur.execute("SELECT 1 FROM kv WHERE uid=? LIMIT 1", (uid,))
        same += cur.fetchone() is not None
        cur.execute("INSERT OR REPLACE INTO kv VALUES(?,?)", (uid, line))
        b_cnt += 1
    con.commit()

    with OUT.open("w", encoding="utf-8") as out:
        for (line,) in cur.execute("SELECT j FROM kv"):
            out.write(line + "\n")

    merged = cur.execute("SELECT COUNT(*) FROM kv").fetchone()[0]
    con.close()

    print(f"same_uid_count={same}")
    print(f"inserted_from_B={b_cnt - same}")
    print(f"merged_total_unique={merged}")
    print(f"output={OUT}")

if __name__ == "__main__":
    main()
