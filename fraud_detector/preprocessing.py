from __future__ import annotations

from datetime import datetime
from math import asin, cos, log1p, pi, radians, sin, sqrt
from typing import Mapping

CATEGORIES = (
    "entertainment", "food_dining", "gas_transport", "grocery_net",
    "grocery_pos", "health_fitness", "home", "kids_pets", "misc_net",
    "misc_pos", "personal_care", "shopping_net", "shopping_pos", "travel",
)

NUMERIC_NAMES = (
    "log_amount", "log_amount_squared", "amount_over_100", "small_amount",
    "large_amount", "log_population", "log_distance_km", "hour_sin",
    "hour_cos", "night", "weekend", "female",
)
FEATURE_NAMES = NUMERIC_NAMES + tuple(f"category_{c}" for c in CATEGORIES) + tuple(
    f"amount_x_{c}" for c in CATEGORIES
) + tuple(f"night_x_{c}" for c in CATEGORIES)
REQUIRED_FIELDS = (
    "transaction_time", "cat_id", "amount", "gender", "lat", "lon",
    "population_city", "merchant_lat", "merchant_lon",
)


def _distance_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    p1, p2 = radians(lat1), radians(lat2)
    dp, dl = p2 - p1, radians(lon2 - lon1)
    a = sin(dp / 2) ** 2 + cos(p1) * cos(p2) * sin(dl / 2) ** 2
    return 12742.0 * asin(min(1.0, sqrt(max(0.0, a))))


def preprocess(record: Mapping[str, object]) -> list[float]:
    missing = [key for key in REQUIRED_FIELDS if record.get(key) in (None, "")]
    if missing:
        raise ValueError(f"Missing required fields: {', '.join(missing)}")

    try:
        when = datetime.fromisoformat(str(record["transaction_time"]))
        amount = float(record["amount"])
        population = float(record["population_city"])
        lat, lon = float(record["lat"]), float(record["lon"])
        merchant_lat = float(record["merchant_lat"])
        merchant_lon = float(record["merchant_lon"])
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid transaction values: {exc}") from exc

    if amount < 0 or population < 0 or not (-90 <= lat <= 90 and -90 <= merchant_lat <= 90):
        raise ValueError("Amount, population or latitude is outside the expected range")
    if not (-180 <= lon <= 180 and -180 <= merchant_lon <= 180):
        raise ValueError("Longitude is outside the expected range")

    log_amount = log1p(amount)
    distance = _distance_km(lat, lon, merchant_lat, merchant_lon)
    hour = when.hour + when.minute / 60.0
    night = float(hour < 6 or hour >= 22)
    category = str(record["cat_id"])
    one_hot = [float(category == value) for value in CATEGORIES]

    return [
        log_amount,
        log_amount * log_amount,
        min(amount, 5000.0) / 100.0,
        float(amount < 20.0),
        float(amount >= 500.0),
        log1p(population),
        log1p(distance),
        sin(2 * pi * hour / 24),
        cos(2 * pi * hour / 24),
        night,
        float(when.weekday() >= 5),
        float(str(record["gender"]).upper() == "F"),
        *one_hot,
        *(log_amount * value for value in one_hot),
        *(night * value for value in one_hot),
    ]
