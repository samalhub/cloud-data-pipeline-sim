# Cloud Data Pipeline (AWS-Style, Local Simulation)

> A local simulation of an AWS-style event-driven pipeline (S3 → Lambda → DynamoDB → CloudWatch)

- Drop JSON into `data/incoming/` → processor transforms + idempotent upsert to SQLite  
- Routes to `processed/` or **deadletter** on failure  
- Provides basic metrics and logs  
- Zero-dependency mode (no installs):
  ```bash
  python src/processor_manual.py --all
  python src/report.py

# Cloud Data Pipeline (Local AWS-Style Simulation)

**Goal:** Demonstrate a scalable, event-driven data pipeline inspired by AWS (S3 → Lambda → DynamoDB → CloudWatch) **without** a cloud account.

- **S3** → local `data/incoming/`
- **Lambda** → `src/processor.py` (watchdog handler)
- **DynamoDB** → SQLite DB at `data/pipeline.db`
- **CloudWatch** → logs in `logs/pipeline.log` + simple metrics

## Architecture (Mermaid)
```mermaid
flowchart LR
  Producer[Producer] -->|PUT file| S3[(data/incoming)]
  S3 -->|file event| Lambda[Processor (watchdog)]
  Lambda -->|UPSERT| DB[(SQLite: data/pipeline.db)]
  Lambda -->|move ok| Processed[data/processed]
  Lambda -->|on error| DLQ[data/deadletter]
  DB --> Report[report.py metrics]
```

## Quickstart
```bash
python -m venv .venv && source .venv/bin/activate     # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# Terminal A
python src/processor.py

# Terminal B
python src/producer.py --n 5

# Metrics
python src/report.py

# Reset state
python src/reset.py
```

## Resume bullets
- Designed and implemented an event-driven cloud data pipeline simulation (S3→Lambda→DynamoDB) using Python watchdog and SQLite; built DLQ & metrics for operational reliability.
- Achieved exactly-once behavior for file ingests via content-hash IDs and upserts; added dead-letter handling and structured logging to mimic CloudWatch.
- Produced a clean, cloud-migration-ready architecture with a clear path to AWS services using the same interfaces and patterns.
