"""Structured Streaming job that performs online anomaly detection."""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

os.environ.setdefault(
    "PYSPARK_SUBMIT_ARGS",
    "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 pyspark-shell",
)

import joblib
import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import MapType, StringType, StructField, StructType

from app.feature_extraction import feature_matrix_from_df
from app import settings

SCHEMA = StructType(
    [
        StructField("timestamp", StringType()),
        StructField("service", StringType()),
        StructField("level", StringType()),
        StructField("host", StringType()),
        StructField("trace_id", StringType()),
        StructField("message", StringType()),
        StructField("anomaly_hint", StringType()),
        StructField("extra", MapType(StringType(), StringType()), nullable=True),
    ]
)


def _load_artifacts() -> tuple:
    if not settings.MODEL_PATH.exists() or not settings.VECTORIZER_PATH.exists():
        raise FileNotFoundError(
            "Model or vectorizer missing. Run `python app/train_model.py` first."
        )
    model = joblib.load(settings.MODEL_PATH)
    vectorizer = joblib.load(settings.VECTORIZER_PATH)
    return model, vectorizer


def _detect_anomalies(vectorizer, model, batch_df: DataFrame, batch_id: int) -> None:
    if batch_df.rdd.isEmpty():
        return
    pdf = batch_df.toPandas()
    if pdf.empty:
        return
    features = feature_matrix_from_df(vectorizer, pdf)
    dense = features.toarray()
    predictions = model.predict(dense)
    scores = model.decision_function(dense)
    pdf["prediction"] = predictions
    pdf["score"] = scores
    pdf["is_anomaly"] = pdf["score"] <= settings.ANOMALY_SCORE_THRESHOLD
    min_score = float(pdf["score"].min())
    print(
        f"[batch {batch_id}] processed {len(pdf)} events | "
        f"min_score={min_score:.4f} threshold={settings.ANOMALY_SCORE_THRESHOLD}"
    )
    anomalies = pdf[pdf["is_anomaly"]]
    if anomalies.empty:
        return
    print("Detected anomalies:")
    for record in json.loads(anomalies.to_json(orient="records")):
        print(json.dumps(record))


def main() -> None:
    model, vectorizer = _load_artifacts()

    spark = (
        SparkSession.builder.appName("LogAnomalyDetection")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", settings.LOG_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed = (
        raw_stream.selectExpr("CAST(value AS STRING) as json_payload")
        .select(from_json(col("json_payload"), SCHEMA).alias("data"))
        .select("data.*")
    )

    query = (
        parsed.writeStream.foreachBatch(lambda df, bid: _detect_anomalies(vectorizer, model, df, bid))
        .outputMode("update")
        .option("checkpointLocation", str(settings.SPARK_CHECKPOINT_DIR))
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
