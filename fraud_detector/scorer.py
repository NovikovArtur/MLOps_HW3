from __future__ import annotations

import json
from math import exp
from pathlib import Path
from typing import Mapping

from fraud_detector.preprocessing import FEATURE_NAMES, preprocess


class FraudScorer:
    def __init__(self, model_path: str | Path):
        with Path(model_path).open(encoding="utf-8") as file:
            model = json.load(file)
        if model.get("feature_names") != list(FEATURE_NAMES):
            raise ValueError("Model and preprocessing feature lists differ")
        self.mean = model["mean"]
        self.scale = model["scale"]
        self.weights = model["weights"]
        self.intercept = model["intercept"]
        self.threshold = model["threshold"]
        self.version = model.get("version", "bootstrap")
        if not (len(self.mean) == len(self.scale) == len(self.weights) == len(FEATURE_NAMES)):
            raise ValueError("Invalid model vector lengths")

    def predict(self, record: Mapping[str, object]) -> tuple[float, int]:
        features = preprocess(record)
        logit = self.intercept + sum(
            weight * ((value - mean) / scale)
            for value, mean, scale, weight in zip(features, self.mean, self.scale, self.weights)
        )
        score = 1.0 / (1.0 + exp(-max(-700.0, min(700.0, logit))))
        return score, int(score >= self.threshold)
