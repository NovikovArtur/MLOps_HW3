from __future__ import annotations

import calendar
import csv
import json
import sqlite3
from datetime import datetime
from pathlib import Path

DATA_FILE = Path("/opt/airflow/data/train.csv")
DB_FILE = Path("/opt/airflow/state/warehouse.sqlite")
BATCH_SIZE = 100000
HISTORY_MONTHS = 36
WINDOW_MONTHS = 3


def next_month(month: str) -> str:
    year, number = map(int, month.split("-"))
    year += number // 12
    number = number % 12 + 1
    return f"{year:04d}-{number:02d}"


def previous_month(month: str) -> str:
    year, number = map(int, month.split("-"))
    if number == 1:
        return f"{year - 1:04d}-12"
    return f"{year:04d}-{number - 1:02d}"


def shift_time(value: str, month: str) -> str:
    original = datetime.fromisoformat(value)
    year, number = map(int, month.split("-"))
    day = min(original.day, calendar.monthrange(year, number)[1])
    return original.replace(year=year, month=number, day=day).strftime("%Y-%m-%d %H:%M:%S")


def connect() -> sqlite3.Connection:
    DB_FILE.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(DB_FILE, timeout=600)
    db.execute("PRAGMA busy_timeout=600000")
    db.execute("PRAGMA journal_mode=WAL")
    return db


def ensure_warehouse() -> None:
    """Reads the source CSV and initializes the warehouse once; a single transaction prevents partial repeated imports."""
    with connect() as db:
        db.execute("BEGIN IMMEDIATE")
        db.execute("CREATE TABLE IF NOT EXISTS records (record_id TEXT PRIMARY KEY, month TEXT NOT NULL, transaction_time TEXT NOT NULL, target INTEGER NOT NULL, payload TEXT NOT NULL)")
        db.execute("CREATE INDEX IF NOT EXISTS records_month_idx ON records (month)")
        db.execute("CREATE TABLE IF NOT EXISTS state (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
        if db.execute("SELECT 1 FROM state WHERE key = 'committed_month'").fetchone():
            return
        if not DATA_FILE.is_file():
            raise FileNotFoundError(DATA_FILE)
        latest = None
        batch = []
        with DATA_FILE.open(encoding="utf-8", newline="") as source:
            for index, row in enumerate(csv.DictReader(source)):
                month = row["transaction_time"][:7]
                latest = max(latest or month, month)
                batch.append((f"seed:{index}", month, row["transaction_time"], int(row["target"]), json.dumps(row, ensure_ascii=False)))
                if len(batch) == 5000:
                    db.executemany("INSERT INTO records VALUES (?, ?, ?, ?, ?)", batch)
                    batch.clear()
        if batch:
            db.executemany("INSERT INTO records VALUES (?, ?, ?, ?, ?)", batch)
        if latest is None:
            raise ValueError("The training CSV is empty")
        db.execute("INSERT INTO state VALUES ('committed_month', ?)", (latest,))


def append_month() -> str:
    """Reads existing labeled rows and adds one dated monthly batch; the transaction makes retries idempotent."""
    ensure_warehouse()
    with connect() as db:
        db.execute("BEGIN IMMEDIATE")
        committed = db.execute("SELECT value FROM state WHERE key = 'committed_month'").fetchone()[0]
        month = next_month(committed)
        if db.execute("SELECT 1 FROM records WHERE month = ? LIMIT 1", (month,)).fetchone():
            return month
        sample = db.execute("SELECT payload FROM records WHERE month < ? ORDER BY RANDOM() LIMIT ?", (month, BATCH_SIZE)).fetchall()
        if len(sample) < BATCH_SIZE:
            raise ValueError(f"Only {len(sample)} rows available for a {BATCH_SIZE} row batch")
        rows = []
        for index, (payload,) in enumerate(sample):
            record = json.loads(payload)
            record["transaction_time"] = shift_time(record["transaction_time"], month)
            rows.append((f"{month}:{index}", month, record["transaction_time"], int(record["target"]), json.dumps(record, ensure_ascii=False)))
        db.executemany("INSERT INTO records VALUES (?, ?, ?, ?, ?)", rows)
        oldest = month
        for _ in range(HISTORY_MONTHS - 1):
            oldest = previous_month(oldest)
        db.execute("DELETE FROM records WHERE month < ?", (oldest,))
        return month
