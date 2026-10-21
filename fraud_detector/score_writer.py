from __future__ import annotations

import json
import logging
import os
import time

import psycopg
from kafka import KafkaConsumer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOG = logging.getLogger(__name__)


def run() -> None:
    broker = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")
    topic = os.getenv("SCORES_TOPIC", "scores")
    database_url = os.environ["DATABASE_URL"]
    while True:
        try:
            with psycopg.connect(database_url) as db:
                consumer = KafkaConsumer(
                    topic,
                    bootstrap_servers=broker,
                    group_id="score-writer-v1",
                    auto_offset_reset="earliest",
                    enable_auto_commit=False,
                    value_deserializer=lambda raw: json.loads(raw.decode("utf-8")),
                )
                LOG.info("Writing %s events to PostgreSQL", topic)
                try:
                    for message in consumer:
                        result = message.value
                        try:
                            transaction_id = str(result["transaction_id"])
                            score = float(result["score"])
                            fraud_flag = int(result["fraud_flag"])
                            model_version = str(result["model_version"])
                            if not transaction_id or not model_version or not (0 <= score <= 1) or fraud_flag not in (0, 1):
                                raise ValueError("Invalid scoring result")
                        except (KeyError, TypeError, ValueError) as exc:
                            LOG.error("Skipping invalid score at offset %s: %s", message.offset, exc)
                            consumer.commit()
                            continue
                        with db.transaction():
                            db.execute(
                                """INSERT INTO transaction_scores (transaction_id, score, fraud_flag, model_version)
                                VALUES (%s, %s, %s, %s)
                                ON CONFLICT (transaction_id) DO UPDATE
                                SET score = EXCLUDED.score, fraud_flag = EXCLUDED.fraud_flag,
                                    model_version = EXCLUDED.model_version, scored_at = NOW()""",
                                (transaction_id, score, fraud_flag, model_version),
                            )
                        consumer.commit()
                        LOG.info("Saved %s", transaction_id)
                finally:
                    consumer.close()
        except Exception:
            LOG.exception("Kafka or PostgreSQL connection failed; reconnecting in 5 seconds")
            time.sleep(5)


if __name__ == "__main__":
    run()
