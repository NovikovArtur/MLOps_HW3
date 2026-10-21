from __future__ import annotations

import csv
import io
import json
import os
from itertools import islice
from uuid import uuid4

import numpy as np
import pandas as pd
import psycopg
import streamlit as st
from kafka import KafkaProducer

from fraud_detector.preprocessing import REQUIRED_FIELDS

st.set_page_config(page_title="Скоринг транзакций", layout="wide")
st.title("Скоринг транзакций")
st.caption("Kafka → модель → PostgreSQL")

broker = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")
topic = os.getenv("TRANSACTIONS_TOPIC", "transactions")
database_url = os.environ["DATABASE_URL"]

upload_tab, results_tab = st.tabs(["Отправить транзакции", "Результаты"])

with upload_tab:
    st.write("Загрузите CSV в формате `test.csv`.")
    uploaded = st.file_uploader("Файл с транзакциями", type="csv")
    limit = st.number_input("Сколько строк отправить", min_value=1, max_value=10000, value=100)
    if st.button("Отправить в Kafka", disabled=uploaded is None):
        try:
            uploaded.seek(0)
            reader = csv.DictReader(io.TextIOWrapper(uploaded, encoding="utf-8-sig"))
            missing = set(REQUIRED_FIELDS) - set(reader.fieldnames or [])
            if missing:
                raise ValueError("В CSV нет полей: " + ", ".join(sorted(missing)))
            producer = KafkaProducer(
                bootstrap_servers=broker,
                acks="all",
                retries=5,
                value_serializer=lambda value: json.dumps(value).encode("utf-8"),
                key_serializer=lambda value: value.encode("utf-8"),
            )
            futures = []
            try:
                for row in islice(reader, int(limit)):
                    transaction_id = str(uuid4())
                    row["transaction_id"] = transaction_id
                    futures.append(producer.send(topic, key=transaction_id, value=row))
                for future in futures:
                    future.get(timeout=30)
            finally:
                producer.close(timeout=30)
            st.success(f"Отправлено транзакций: {len(futures)}. Откройте раздел «Результаты».")
        except Exception as exc:
            st.error(f"Не удалось отправить транзакции: {exc}")

with results_tab:
    st.write("Результаты сохраняются после обработки сообщений из Kafka.")
    if st.button("Посмотреть результаты"):
        try:
            with psycopg.connect(database_url) as db:
                frauds = db.execute(
                    """SELECT transaction_id, score, fraud_flag, model_version, scored_at
                    FROM transaction_scores WHERE fraud_flag = 1
                    ORDER BY scored_at DESC, transaction_id DESC LIMIT 10"""
                ).fetchall()
                recent = db.execute(
                    """SELECT score FROM transaction_scores
                    ORDER BY scored_at DESC, transaction_id DESC LIMIT 100"""
                ).fetchall()
            st.subheader("10 последних транзакций с флагом фрода")
            if frauds:
                st.dataframe(
                    pd.DataFrame(frauds, columns=["transaction_id", "score", "fraud_flag", "model_version", "scored_at"]),
                    hide_index=True,
                    use_container_width=True,
                )
            else:
                st.info("Транзакций с флагом фрода пока нет.")

            st.subheader(f"Распределение скоров последних {len(recent)} транзакций")
            if recent:
                counts, edges = np.histogram([row[0] for row in recent], bins=np.linspace(0, 1, 11))
                labels = [f"{edges[i]:.1f}–{edges[i + 1]:.1f}" for i in range(10)]
                st.bar_chart(pd.DataFrame({"Диапазон скора": labels, "Количество": counts}).set_index("Диапазон скора"))
            else:
                st.info("Результатов пока нет. Отправьте транзакции и попробуйте ещё раз.")
        except Exception as exc:
            st.error(f"Не удалось прочитать результаты: {exc}")
