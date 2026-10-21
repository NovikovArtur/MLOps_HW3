from __future__ import annotations

import csv
import hashlib
import json
import os
import shutil
from pathlib import Path

import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score

from fraud_detector.preprocessing import FEATURE_NAMES, preprocess
from tasks.warehouse import DATA_FILE, WINDOW_MONTHS, connect, ensure_warehouse, previous_month, next_month

STATE_DIR = Path("/opt/airflow/state")
MODEL_DIR = Path("/opt/airflow/models")


def prepare_window(run_id: str) -> str:
    """Reads the current three calendar months and writes train and validation CSVs; the run ID isolates retries."""
    ensure_warehouse()
    stage = STATE_DIR / hashlib.sha256(run_id.encode()).hexdigest()[:16]
    stage.mkdir(parents=True, exist_ok=True)
    with connect() as db:
        committed = db.execute("SELECT value FROM state WHERE key = 'committed_month'").fetchone()[0]
        pending = next_month(committed)
        latest = pending if db.execute("SELECT 1 FROM records WHERE month = ? LIMIT 1", (pending,)).fetchone() else committed
        months = [latest]
        for _ in range(WINDOW_MONTHS - 1):
            months.insert(0, previous_month(months[0]))
        with DATA_FILE.open(encoding="utf-8", newline="") as source:
            fields = csv.DictReader(source).fieldnames
        for label, selected in (("train", months[:-1]), ("validation", months[-1:])):
            count = 0
            with (stage / f"{label}.csv").open("w", encoding="utf-8", newline="") as output:
                writer = csv.DictWriter(output, fieldnames=fields)
                writer.writeheader()
                for month in selected:
                    cursor = db.execute("SELECT payload FROM records WHERE month = ?", (month,))
                    for (payload,) in cursor:
                        writer.writerow(json.loads(payload))
                        count += 1
            if count == 0:
                raise ValueError(f"No rows in {label} months: {selected}")
    (stage / "window.json").write_text(json.dumps({"months": months, "run_id": run_id}), encoding="utf-8")
    return str(stage)


def _load_features(path: Path) -> tuple[np.ndarray, np.ndarray]:
    features = []
    labels = []
    with path.open(encoding="utf-8", newline="") as source:
        for row in csv.DictReader(source):
            features.append(preprocess(row))
            labels.append(int(row["target"]))
    return np.asarray(features, dtype=np.float64), np.asarray(labels, dtype=np.int8)


def _predict(model: dict, features: np.ndarray) -> np.ndarray:
    scaled = (features - np.asarray(model["mean"])) / np.asarray(model["scale"])
    logits = scaled @ np.asarray(model["weights"]) + model["intercept"]
    return 1 / (1 + np.exp(-np.clip(logits, -700, 700)))


def train_model(stage: str) -> str:
    """Reads prepared CSVs and writes a candidate model; repeating the fit only replaces this run's candidate."""
    folder = Path(stage)
    train_x, train_y = _load_features(folder / "train.csv")
    valid_x, valid_y = _load_features(folder / "validation.csv")
    if len(np.unique(train_y)) < 2 or len(np.unique(valid_y)) < 2:
        raise ValueError("Both classes are required in train and validation months")
    mean = train_x.mean(axis=0)
    scale = train_x.std(axis=0)
    scale[scale < 1e-8] = 1.0
    estimator = LogisticRegression(class_weight="balanced", max_iter=150)
    estimator.fit((train_x - mean) / scale, train_y)
    window = json.loads((folder / "window.json").read_text(encoding="utf-8"))
    month = window["months"][-1]
    version = f"{month.replace('-', '')}_{folder.name}"
    model = {
        "model_type": "logistic_regression",
        "version": version,
        "source_month": month,
        "feature_names": list(FEATURE_NAMES),
        "mean": mean.tolist(),
        "scale": scale.tolist(),
        "weights": estimator.coef_[0].tolist(),
        "intercept": float(estimator.intercept_[0]),
        "threshold": 0.5,
        "training_rows": int(len(train_y)),
        "validation_rows": int(len(valid_y)),
        "validation_average_precision": float(average_precision_score(valid_y, estimator.predict_proba((valid_x - mean) / scale)[:, 1])),
    }
    active = json.loads((MODEL_DIR / "active.json").read_text(encoding="utf-8"))
    if active["version"] != "bootstrap":
        previous = json.loads((MODEL_DIR / active["file"]).read_text(encoding="utf-8"))
        model["previous_average_precision"] = float(average_precision_score(valid_y, _predict(previous, valid_x)))
    candidate = folder / "candidate.json"
    candidate.write_text(json.dumps(model, ensure_ascii=False), encoding="utf-8")
    return str(candidate)


def publish_model(candidate_path: str) -> str:
    """Reads the candidate and current model, then atomically publishes an improvement; a failed retry leaves the month pointer unchanged."""
    candidate = Path(candidate_path)
    model = json.loads(candidate.read_text(encoding="utf-8"))
    previous_ap = model.get("previous_average_precision")
    if previous_ap is not None and model["validation_average_precision"] < previous_ap:
        shutil.rmtree(candidate.parent)
        return f"rejected: candidate AP {model['validation_average_precision']:.6f}, current AP {previous_ap:.6f}"
    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    filename = f"model_{model['version']}.json"
    temporary = MODEL_DIR / f".{filename}.tmp"
    temporary.write_text(json.dumps(model, ensure_ascii=False), encoding="utf-8")
    os.replace(temporary, MODEL_DIR / filename)
    manifest = {"version": model["version"], "file": filename}
    pointer = MODEL_DIR / ".active.tmp"
    pointer.write_text(json.dumps(manifest), encoding="utf-8")
    os.replace(pointer, MODEL_DIR / "active.json")
    with connect() as db:
        db.execute("UPDATE state SET value = ? WHERE key = 'committed_month'", (model["source_month"],))
    shutil.rmtree(candidate.parent)
    return model["version"]
