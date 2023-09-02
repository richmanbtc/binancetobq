FROM python:3.10.13

USER root

RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir \
    'google-cloud-bigquery[bqstorage,pandas]' \
    pandas-gbq \
    python-binance==1.0.19 \
    pandas==1.5.1 \
    yappi==1.4.0

ADD . /app
WORKDIR /app
CMD python -m src.main
ENV BINANCETOBQ_LOG_LEVEL=debug
