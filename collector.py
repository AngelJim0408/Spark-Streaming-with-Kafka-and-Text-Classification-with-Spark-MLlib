"""
Part 1 Collector: reads live Bluesky posts from Jetstream and writes them to a Kafka topic1.
"""

import argparse   # parses command-line options (--bootstrap, --topic, ...)
import json       # converts between JSON text and Python dictionaries
import random     # used for random sampling of posts
import time       # used to wait between reconnect attempts
 
import websocket                 # WebSocket connection to Jetstream
from kafka import KafkaProducer  # sends messages to Kafka

"""""
Public Bluesky Jetstream endpoint. wantedCollections=app.bsky.feed.post 
asks the server to send only post events (no likes, follows, reposts, ...), 
which greatly reduces the amount of data we receive.
"""""
JETSTREAM_URL = (
    "wss://jetstream2.us-east.bsky.network/subscribe"
    "?wantedCollections=app.bsky.feed.post"
)


def parse_args():
    p = argparse.ArgumentParser(description="Bluesky -> Kafka producer")
    p.add_argument("--bootstrap", required=True, help="Kafka bootstrap servers, e.g. localhost:9092")
    p.add_argument("--topic", default="topic1", help="Kafka topic to write to")
    p.add_argument("--out", default=None, help="Optional JSONL file to also save each message")
    p.add_argument("--lang", default="en", help="Keep only posts in this language ('' = all)")
    p.add_argument("--sample", type=float, default=0.2,
                   help="Fraction of posts to keep (0-1); Bluesky volume is high")
    p.add_argument("--min-chars", type=int, default=20, help="Skip posts shorter than this")
    return p.parse_args()


def main():
    args = parse_args()
    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    out_file = open(args.out, "a", encoding="utf-8") if args.out else None
    sent = 0

    def on_message(ws, raw):
        nonlocal sent
        try:
            event = json.loads(raw)
        except json.JSONDecodeError:
            return
        if event.get("kind") != "commit":
            return
        commit = event.get("commit", {})
        if commit.get("operation") != "create":
            return
        record = commit.get("record", {})
        text = (record.get("text") or "").strip()
        if len(text) < args.min_chars:
            return
        if args.lang and args.lang not in (record.get("langs") or []):
            return
        if random.random() > args.sample:
            return

        msg = {
            "id": f"{event.get('did')}/{commit.get('rkey')}",
            "text": text,
            "created_at": record.get("createdAt"),
        }
        producer.send(args.topic, msg)
        if out_file:
            out_file.write(json.dumps(msg, ensure_ascii=False) + "\n")
            out_file.flush()
        sent += 1
        if sent % 50 == 0:
            print(f"[producer] sent {sent} messages to {args.topic}")

    def on_error(ws, err):
        print(f"[producer] websocket error: {err}")

    # Reconnect loop so the producer keeps running if the connection drops
    while True:
        ws = websocket.WebSocketApp(JETSTREAM_URL, on_message=on_message, on_error=on_error)
        ws.run_forever(ping_interval=30, ping_timeout=10)
        print("[producer] connection closed, reconnecting in 5s...")
        producer.flush()
        time.sleep(5)


if __name__ == "__main__":
    main()