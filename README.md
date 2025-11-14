# AIOps Log Anomaly Detection System

A production-inspired reference implementation of a real-time log anomaly detection stack.  Kafka continuously ingests raw application logs generated in Python, Apache Spark Structured Streaming performs on-line feature extraction, and an Isolation Forest model built with scikit-learn highlights unexpected error patterns.  Everything ships as containers so the entire system can be reproduced anywhere Docker runs.

## Architecture
- **Kafka ingestion** – high-volume log events flow through Kafka (`application-logs` topic) for durable buffering and fan-out.
- **Spark streaming analytics** – a PySpark Structured Streaming application consumes the Kafka topic, vectorizes log text, and scores it with a broadcast Isolation Forest model.
- **Isolation Forest model** – trained offline with scikit-learn using historical/synthetic logs, persisted via Joblib, and mounted into the Spark job for real-time decisions.
- **Synthetic log generator** – Python producer emits realistic INFO/WARN/ERROR events with controllable anomaly ratios to exercise the pipeline.

```
+-------------+        +---------------+        +-------------------+
| Log Source  | -----> | Kafka Topic   | -----> | Spark Streaming   |
| (generator) |        | application-* |        | Isolation Forest  |
+-------------+        +---------------+        +-------------------+
                                                       |
                                              Detected anomalies
```

## Repository layout

```
app/
  log_producer.py     -> Generates synthetic logs and writes them to Kafka.
  train_model.py      -> Trains the Isolation Forest model from sample logs.
  spark_app.py        -> Structured Streaming job with on-line anomaly scoring.
  feature_extraction.py -> Shared feature helpers based on HashingVectorizer.
  settings.py         -> Centralized configuration helpers.
data/sample_logs.csv -> Seed dataset used during model training.
models/              -> Stores the serialized model/vectorizer artifacts.
docker-compose.yml   -> Spins up Kafka, the producer, trainer, and Spark app.
Dockerfile           -> Build recipe for all Python-based services.
```

## Prerequisites
- Docker (20+) and Docker Compose plugin
- Python 3.10+ (only required if you want to run scripts without containers)

## Quick start
1. **Build the base image**
   ```bash
   docker compose build trainer
   ```
2. **Train the anomaly detector** – this populates `models/` with the Isolation Forest and hashing vectorizer artifacts.
   ```bash
   docker compose run --rm trainer
   ```
3. **Launch infrastructure and streaming apps**
   ```bash
   docker compose up kafka zookeeper -d
   docker compose up log-producer spark-app
   ```
   The Spark container will log detected anomalies whenever the model predicts `-1` for a micro-batch.

Stop everything with `docker compose down`.  All artifacts remain on the host via bind mounts (`./models`, `./checkpoint`).

## Local development without Docker
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python app/train_model.py  # writes model artifacts
python app/log_producer.py  # requires Kafka running locally on localhost:9092
python app/spark_app.py
```
Adjust Kafka endpoints via `KAFKA_BOOTSTRAP_SERVERS`, `LOG_TOPIC`, `MODEL_PATH`, etc.

## Extending the pipeline
- Swap `data/sample_logs.csv` with real historical logs to retrain the Isolation Forest.
- Replace the synthetic producer with your application log shippers (Fluent Bit, Filebeat, etc.).
- Use an alternative downstream sink by editing `app/spark_app.py` (e.g., push anomalies into Elasticsearch, Slack, PagerDuty).
- Tune `FeatureConfig` / vectorizer settings to better model your domain-specific log vocabulary.

## Troubleshooting
- Ensure model artifacts exist before starting `spark-app` or it will exit with a descriptive error.
- The `docker compose logs spark-app` command surfaces anomaly outputs; `kafka-topics` and `kafka-console-consumer` utilities from the Confluent images are available for deeper inspection.
- Adjust the anomaly ratio via `ANOMALY_SCORE_THRESHOLD` (default 0.05) or edit the Isolation Forest contamination parameter inside `app/train_model.py` for your data distribution.
