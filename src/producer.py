import argparse, json, random, string, time
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).resolve().parents[1]
INCOMING = ROOT / "data" / "incoming"

MESSAGES = [
    "user signed up",
    "item added to cart",
    "checkout started",
    "payment success",
    "payment failed",
    "profile updated",
    "password reset",
]

def rand_message():
    base = random.choice(MESSAGES)
    noise = ''.join(random.choice(string.ascii_lowercase) for _ in range(random.randint(3, 8)))
    return f"{base}::{noise}"

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=5, help="number of events to produce")
    ap.add_argument("--delay", type=float, default=0.1, help="seconds between events")
    args = ap.parse_args()

    INCOMING.mkdir(parents=True, exist_ok=True)
    for i in range(args.n):
        doc = {
            "message": rand_message(),
            "priority": random.choice([0, 1, 2, 3]),
            "timestamp": datetime.utcnow().isoformat()
        }
        p = INCOMING / f"event_{int(time.time()*1000)}_{i}.json"
        with open(p, "w", encoding="utf-8") as f:
            json.dump(doc, f, ensure_ascii=False)
        print("PUT", p.name)
        time.sleep(args.delay)

if __name__ == "__main__":
    main()
