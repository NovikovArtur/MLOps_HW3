from __future__ import annotations

import hashlib
import html
import json
import math
import os
from collections import Counter
from datetime import datetime
from pathlib import Path

from fraud_detector.preprocessing import CATEGORIES, REQUIRED_FIELDS
from tasks.warehouse import connect, ensure_warehouse

REPORT_DIR = Path("/opt/airflow/reports")


def check_quality(run_id: str) -> str:
    """Reads the labeled warehouse and writes an HTML rule report; retrying the run replaces its report without changing data."""
    ensure_warehouse()
    counts = Counter()
    total = 0
    with connect() as db:
        for month, timestamp, label, payload in db.execute("SELECT month, transaction_time, target, payload FROM records"):
            total += 1
            row = json.loads(payload)
            if any(row.get(field) in (None, "") for field in REQUIRED_FIELDS):
                counts["required_fields"] += 1
            if label not in (0, 1):
                counts["binary_target"] += 1
            try:
                amount = float(row["amount"])
                if not math.isfinite(amount) or amount < 0:
                    counts["amount"] += 1
            except (KeyError, TypeError, ValueError):
                counts["amount"] += 1
            try:
                lat, lon = float(row["lat"]), float(row["lon"])
                merchant_lat, merchant_lon = float(row["merchant_lat"]), float(row["merchant_lon"])
                if not all(map(math.isfinite, (lat, lon, merchant_lat, merchant_lon))) or not (-90 <= lat <= 90 and -90 <= merchant_lat <= 90 and -180 <= lon <= 180 and -180 <= merchant_lon <= 180):
                    counts["coordinates"] += 1
            except (KeyError, TypeError, ValueError):
                counts["coordinates"] += 1
            try:
                parsed = datetime.fromisoformat(timestamp)
                if parsed.strftime("%Y-%m") != month or row["transaction_time"] != timestamp:
                    counts["month"] += 1
            except (KeyError, TypeError, ValueError):
                counts["month"] += 1
            if row.get("cat_id") not in CATEGORIES:
                counts["category"] += 1
    rules = {
        "required_fields": "Обязательные поля заполнены",
        "binary_target": "Метка равна 0 или 1",
        "amount": "Сумма неотрицательна и конечна",
        "coordinates": "Координаты входят в допустимые диапазоны",
        "month": "Дата корректна и соответствует месяцу витрины",
        "category": "Категория известна препроцессингу",
    }
    rows = "".join(f"<tr><td>{html.escape(title)}</td><td>{counts[key]}</td><td>{'OK' if counts[key] == 0 else 'FAIL'}</td></tr>" for key, title in rules.items())
    status = "OK" if total > 0 and not any(counts.values()) else "FAIL"
    report = f"<!doctype html><html lang='ru'><meta charset='utf-8'><title>Data Quality</title><h1>Data Quality</h1><p>Строк: {total}. Результат: {status}.</p><table border='1'><tr><th>Правило</th><th>Нарушений</th><th>Статус</th></tr>{rows}</table></html>"
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    name = f"dq_{hashlib.sha256(run_id.encode()).hexdigest()[:16]}.html"
    temporary = REPORT_DIR / f".{name}.tmp"
    temporary.write_text(report, encoding="utf-8")
    os.replace(temporary, REPORT_DIR / name)
    latest = REPORT_DIR / ".latest.tmp"
    latest.write_text(report, encoding="utf-8")
    os.replace(latest, REPORT_DIR / "latest.html")
    if status == "FAIL":
        raise ValueError(f"Data Quality failed; report: {REPORT_DIR / name}")
    return str(REPORT_DIR / name)
