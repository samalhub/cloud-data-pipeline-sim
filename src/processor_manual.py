import argparse, json, hashlib
from pathlib import Path
from datetime import datetime, UTC
from models import init_db, upsert_event

ROOT = Path(__file__).resolve().parents[1]
INCOMING = ROOT / "data" / "incoming"
PROCESSED = ROOT / "data" / "processed"
DEADLETTER = ROOT / "data" / "deadletter"

def content_hash(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()[:16]

def transform(record: dict) -> dict:
    # Simple transform: add a score field
    message = record.get("message", "")
    priority = int(record.get("priority", 0))
    record["score"] = len(message) + priority * 10
    return record

def process_file(path: Path):
    raw = path.read_bytes()
    eid = content_hash(raw)
    created_at = datetime.now(UTC).isoformat()
    source = "s3://local/incoming/" + path.name
    try:
        record = json.loads(raw.decode("utf-8"))
        record = transform(record)
        payload = json.dumps(record, separators=(",", ":"))
        upsert_event(eid, source, payload, created_at, datetime.now(UTC).isoformat(), "OK")
        target = PROCESSED / f"{eid}.json"
        path.replace(target)
        print(f"OK {path.name} -> {target.name}")
    except Exception as e:
        upsert_event(eid, source, raw.decode('utf-8', errors='ignore'), created_at, datetime.now(UTC).isoformat(), "ERROR")
        target = DEADLETTER / f"{eid}.json"
        path.replace(target)
        print(f"ERROR {path.name} -> DLQ ({e})")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--all", action="store_true", help="process all .json files in incoming/")
    ap.add_argument("--file", type=str, help="process a specific file path")
    args = ap.parse_args()

    INCOMING.mkdir(parents=True, exist_ok=True)
    PROCESSED.mkdir(parents=True, exist_ok=True)
    DEADLETTER.mkdir(parents=True, exist_ok=True)
    init_db()

    paths = []
    if args.file:
        p = Path(args.file)
        if p.exists():
            paths.append(p)
    if args.all:
        paths.extend(sorted([p for p in INCOMING.glob("*.json")]))

    if not paths:
        print("Nothing to process. Use --all or --file <path>.")
        return

    for p in paths:
        process_file(p)

if __name__ == "__main__":
    main()
