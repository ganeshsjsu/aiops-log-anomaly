"""Central configuration for the AIOps anomaly detection system."""
from __future__ import annotations

import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
MODEL_DIR = BASE_DIR / "models"
MODEL_DIR.mkdir(parents=True, exist_ok=True)

MODEL_PATH = Path(os.getenv("MODEL_PATH", MODEL_DIR / "isolation_forest.joblib"))
VECTORIZER_PATH = Path(os.getenv("VECTORIZER_PATH", MODEL_DIR / "vectorizer.joblib"))

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
LOG_TOPIC = os.getenv("LOG_TOPIC", "application-logs")
SPARK_CHECKPOINT_DIR = Path(
    os.getenv("SPARK_CHECKPOINT_DIR", str(BASE_DIR / "checkpoint"))
)
SPARK_CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)

ANOMALY_SCORE_THRESHOLD = float(os.getenv("ANOMALY_SCORE_THRESHOLD", "0.05"))
