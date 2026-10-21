from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path

from kafka import KafkaConsumer, KafkaProducer

from fraud_detector.scorer import FraudScorer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOG = logging.getLogger(__name__)


def run() -> None:
    broker = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")
    input_topic = os.getenv("TRANSACTIONS_TOPIC", "transactions")
    output_topic = os.getenv("SCORES_TOPIC", "scores")
    model_dir = Path(os.getenv("MODEL_DIR", "/app/models"))
    model = None
    active_version = None

    while True:
        try:
            consumer = KafkaConsumer(
                input_topic,
                bootstrap_servers=broker,
                group_id="fraud-detector-v1",
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                value_deserializer=lambda raw: json.loads(raw.decode("utf-8")),
            )
            producer = KafkaProducer(
                bootstrap_servers=broker,
                acks="all",
                retries=5,
                value_serializer=lambda value: json.dumps(value).encode("utf-8"),
                key_serializer=lambda value: value.encode("utf-8"),
            )
            LOG.info("Scoring from %s to %s", input_topic, output_topic)
            for message in consumer:
                try:
                    transaction = message.value
                    transaction_id = str(transaction["transaction_id"])
                    manifest = json.loads((model_dir / "active.json").read_text(encoding="utf-8"))
                    if manifest["version"] != active_version:
                        model = FraudScorer(model_dir / manifest["file"])
                        active_version = manifest["version"]
                    score, fraud_flag = model.predict(transaction)
                    result = {
                        "transaction_id": transaction_id,
                        "score": score,
                        "fraud_flag": fraud_flag,
                        "model_version": model.version,
                    }
                    producer.send(output_topic, key=transaction_id, value=result).get(timeout=30)
                    consumer.commit()
                    LOG.info("Scored %s: %.6f, fraud=%s", transaction_id, score, fraud_flag)
                except (KeyError, TypeError, ValueError) as exc:
                    LOG.error("Skipping invalid transaction at offset %s: %s", message.offset, exc)
                    consumer.commit()
        except Exception:
            LOG.exception("Kafka connection failed; reconnecting in 5 seconds")
            time.sleep(5)


if __name__ == "__main__":
    run()
