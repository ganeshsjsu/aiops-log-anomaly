"""Train an IsolationForest anomaly detector over historical application logs."""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import joblib
import pandas as pd
from sklearn.ensemble import IsolationForest

from app.feature_extraction import FeatureConfig, build_vectorizer, feature_matrix_from_df
from app import settings


def load_training_frame(dataset_path: Path) -> pd.DataFrame:
    if not dataset_path.exists():
        raise FileNotFoundError(
            f"Training dataset {dataset_path} was not found. Provide logs before training."
        )
    return pd.read_csv(dataset_path)


def train_model(dataset_path: Path | None = None) -> None:
    dataset_path = dataset_path or settings.DATA_DIR / "sample_logs.csv"
    df = load_training_frame(dataset_path)

    vectorizer = build_vectorizer(FeatureConfig())
    features = feature_matrix_from_df(vectorizer, df)

    anomaly_ratio = max(0.01, df.get("label", pd.Series(dtype=int)).mean() or 0.05)

    model = IsolationForest(
        n_estimators=200,
        contamination=anomaly_ratio,
        max_samples="auto",
        random_state=42,
        warm_start=False,
        n_jobs=-1,
    )
    model.fit(features.toarray())

    joblib.dump(model, settings.MODEL_PATH)
    joblib.dump(vectorizer, settings.VECTORIZER_PATH)

    metadata = {
        "samples": int(df.shape[0]),
        "features": int(features.shape[1]),
        "anomaly_ratio": float(anomaly_ratio),
    }
    print(json.dumps(metadata, indent=2))


if __name__ == "__main__":
    train_model()
