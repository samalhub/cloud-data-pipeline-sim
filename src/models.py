import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).resolve().parents[1] / "data" / "pipeline.db"

DDL = '''
CREATE TABLE IF NOT EXISTS events (
    id TEXT PRIMARY KEY,
    source TEXT NOT NULL,
    payload TEXT NOT NULL,
    created_at TEXT NOT NULL,
    processed_at TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('OK', 'ERROR'))
);
'''

def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn

def init_db():
    conn = get_conn()
    with conn:
        conn.execute(DDL)
    conn.close()

def upsert_event(event_id, source, payload, created_at, processed_at, status):
    conn = get_conn()
    with conn:
        conn.execute(
            """INSERT INTO events (id, source, payload, created_at, processed_at, status)
               VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT(id) DO UPDATE SET
                 source=excluded.source,
                 payload=excluded.payload,
                 processed_at=excluded.processed_at,
                 status=excluded.status
            """,
            (event_id, source, payload, created_at, processed_at, status)
        )
    conn.close()
