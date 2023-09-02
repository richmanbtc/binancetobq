FROM python:3.10.13

USER root

RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir \
    ccxt==1.93.1 \
    "git+https://github.com/richmanbtc/ccxt_rate_limiter.git@v0.0.4#egg=ccxt_rate_limiter" \
    "git+https://github.com/richmanbtc/crypto_data_fetcher.git@v0.0.17#egg=crypto_data_fetcher" \
    'google-cloud-bigquery[bqstorage,pandas]' \
    gql[all] \
    pandas-gbq \
    retry==0.9.2 \
    python-binance==1.0.19 \
    pandas==1.5.1

ADD . /app
WORKDIR /app
CMD python -m src.main
ENV BINANCETOBQ_LOG_LEVEL=debug
