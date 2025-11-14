FROM python:3.10-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-21-jre-headless procps \
    && ln -s /usr/lib/jvm/java-21-openjdk-$(dpkg --print-architecture) /usr/lib/jvm/java-21-openjdk \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk
ENV PATH="${JAVA_HOME}/bin:${PATH}"

WORKDIR /opt/aiops

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY data ./data
COPY models ./models

ENV PYTHONPATH=/opt/aiops

CMD ["python", "app/log_producer.py"]
