import hashlib, json, time, traceback
from pathlib import Path
from datetime import datetime
from loguru import logger
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

from models import init_db, upsert_event

ROOT = Path(__file__).resolve().parents[1]
INCOMING = ROOT / "data" / "incoming"
PROCESSED = ROOT / "data" / "processed"
DEADLETTER = ROOT / "data" / "deadletter"
LOGS = ROOT / "logs"
LOGS.mkdir(parents=True, exist_ok=True)
logger.add(LOGS / "pipeline.log", rotation="500 KB", serialize=False)

def content_hash(b):
    import hashlib
    return hashlib.sha256(b).hexdigest()[:16]

def transform(record):
    # Toy transform: score = len(message) + priority*10
    message = record.get("message", "")
    priority = int(record.get("priority", 0))
    record["score"] = len(message) + priority * 10
    return record

def process_file(path: Path):
    with open(path, "rb") as f:
        raw = f.read()

    event_id = content_hash(raw)
    created_at = datetime.utcnow().isoformat()
    source = "s3://local/incoming/" + path.name

    try:
        record = json.loads(raw.decode("utf-8"))
        record = transform(record)
        payload = json.dumps(record, separators=(",", ":"))
        upsert_event(event_id, source, payload, created_at, datetime.utcnow().isoformat(), "OK")
        target = PROCESSED / f"{event_id}.json"
        path.replace(target)
        logger.info(f"OK id={event_id} file={path.name} -> {target.name}")
    except Exception as e:
        upsert_event(event_id, source, raw.decode('utf-8', errors='ignore'), created_at, datetime.utcnow().isoformat(), "ERROR")
        target = DEADLETTER / f"{event_id}.json"
        path.replace(target)
        logger.error(f"ERROR id={event_id} file={path.name} -> DLQ ({e})\n{traceback.format_exc()}")

class Handler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory:
            return
        p = Path(event.src_path)
        time.sleep(0.1)  # ensure file write finished
        if p.suffix.lower() == ".json":
            process_file(p)

def main():
    INCOMING.mkdir(parents=True, exist_ok=True)
    PROCESSED.mkdir(parents=True, exist_ok=True)
    DEADLETTER.mkdir(parents=True, exist_ok=True)
    init_db()
    logger.info("Processor starting; watching for new files...")
    event_handler = Handler()
    observer = Observer()
    observer.schedule(event_handler, str(INCOMING), recursive=False)
    observer.start()
    try:
        while True:
            time.sleep(0.5)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

if __name__ == "__main__":
    main()
