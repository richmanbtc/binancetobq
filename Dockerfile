FROM python:3.10.13

RUN apt-get update && apt-get install -y --no-install-recommends \
    jq \
    ripgrep \
    procps \
    tmux \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL https://chatgpt.com/codex/install.sh | sh

# Codex config
RUN mkdir -p /root/.codex
COPY config.toml /root/.codex/config.toml

USER root

RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir \
    'google-cloud-bigquery[bqstorage,pandas]' \
    pandas-gbq \
    python-binance==1.0.19 \
    numpy==1.26.4 pandas==1.5.1 \
    yappi==1.4.0

ADD . /app
RUN python -c "import numpy; import pandas; import pandas_gbq; from google.cloud import bigquery; from binance import Client, ThreadedWebsocketManager"

WORKDIR /app
CMD python -m src.main
ENV BINANCETOBQ_LOG_LEVEL=debug
