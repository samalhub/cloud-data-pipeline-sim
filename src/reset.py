from pathlib import Path
import shutil

ROOT = Path(__file__).resolve().parents[1]

for d in ["data/processed", "data/deadletter", "data/incoming", "logs"]:
    p = ROOT / d
    if p.exists():
        for item in p.iterdir():
            if item.is_file():
                item.unlink()
            elif item.is_dir():
                shutil.rmtree(item)

db = ROOT / "data" / "pipeline.db"
if db.exists():
    db.unlink()

print("Reset complete.")
