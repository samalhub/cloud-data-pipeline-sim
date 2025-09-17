import sqlite3, json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "data" / "pipeline.db"

def main():
    if not DB.exists():
        print("No DB yet. Run processor first.")
        return
    conn = sqlite3.connect(DB)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*), status FROM events GROUP BY status")
    rows = cur.fetchall()
    counts = {status: n for (n, status) in rows}
    total = sum(counts.values())
    print(f"Total events: {total}")
    for status, n in counts.items():
        print(f"  {status}: {n}")
    cur.execute("SELECT payload FROM events WHERE status='OK' LIMIT 1")
    row = cur.fetchone()
    if row:
        print("\nSample processed payload:")
        print(json.dumps(json.loads(row[0]), indent=2))
    conn.close()

if __name__ == "__main__":
    main()
