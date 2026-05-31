import csv
import json
import time
from confluent_kafka import Producer

# ── Config ────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC           = "flight-stream"
DELAY_SECONDS   = 0.05
LOCAL_PATH = r"D:\Documents\BigData\src\dataset\flights.csv"  # đổi path thật của bạn

# ── Producer setup ────────────────────────────────────────────────
producer = Producer({
    'bootstrap.servers': 'localhost:9092',
    'broker.address.family': 'v4',
    'message.timeout.ms': 10000,      # tăng timeout lên 10 giây
    'request.timeout.ms': 10000,
})

def delivery_report(err, msg):
    if err is not None:
        print(f'Delivery failed: {err}')

print(f"Connected to Kafka at {KAFKA_BOOTSTRAP}")
print(f"Streaming to topic: {TOPIC}")
print("-" * 50)

# ── Stream rows ───────────────────────────────────────────────────
sent = 0
skipped = 0

with open(LOCAL_PATH, encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        try:
            if not row.get("AIRLINE") or not row.get("DEPARTURE_DELAY"):
                skipped += 1
                continue

            record = {
                "AIRLINE":         row.get("AIRLINE", "").strip(),
                "ORIGIN_AIRPORT":  row.get("ORIGIN_AIRPORT", "").strip(),
                "DEST_AIRPORT":    row.get("DESTINATION_AIRPORT", "").strip(),
                "DEPARTURE_DELAY": float(row["DEPARTURE_DELAY"]) if row["DEPARTURE_DELAY"] else 0.0,
                "ARRIVAL_DELAY":   float(row["ARRIVAL_DELAY"])   if row.get("ARRIVAL_DELAY") else 0.0,
                "DISTANCE":        float(row["DISTANCE"])        if row.get("DISTANCE") else 0.0,
                "CANCELLED":       int(row.get("CANCELLED", 0)),
            }

            producer.produce(
                TOPIC,
                key=record["AIRLINE"],
                value=json.dumps(record).encode("utf-8"),
                callback=delivery_report
            )
            producer.poll(0)
            sent += 1

            if sent % 1000 == 0:
                print(f"  Sent {sent:,} records | Skipped {skipped:,}")

            time.sleep(DELAY_SECONDS)

        except (ValueError, KeyError):
            skipped += 1
            continue

producer.flush()
print(f"\nDone! Total sent: {sent:,} | Skipped: {skipped:,}")