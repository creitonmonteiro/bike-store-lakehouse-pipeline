FROM apache/airflow:2.9.0-python3.10

USER root

RUN apt-get update && apt-get install -y \
    ca-certificates \
    curl \
    && update-ca-certificates \
    && mkdir -p /etc/pki/tls/certs \
    && ln -s /etc/ssl/certs/ca-certificates.crt /etc/pki/tls/certs/ca-bundle.crt \
    && apt-get clean

ENV SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt
ENV CURL_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt

USER airflow

RUN pip install --no-cache-dir uv

COPY pyproject.toml uv.lock /opt/airflow/
COPY src /opt/airflow/src

WORKDIR /opt/airflow

USER root

RUN uv pip install --system .
USER airflow