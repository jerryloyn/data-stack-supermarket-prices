FROM apache/airflow:2.9.2
USER root
RUN apt-get update && apt-get install unzip
RUN  curl -s -L  https://github.com/duckdb/duckdb/releases/download/v1.0.0/duckdb_cli-linux-aarch64.zip -o duckdb.zip && unzip duckdb.zip -d /opt
USER airflow
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt