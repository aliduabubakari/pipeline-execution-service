FROM apache/airflow:2.8.1-python3.11

USER airflow

WORKDIR /opt/airflow

COPY --chown=airflow:root airflow/requirements.txt /requirements.txt

RUN pip install --no-cache-dir --user \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.8.1/constraints-3.11.txt" \
    -r /requirements.txt

ENV PATH="/home/airflow/.local/bin:${PATH}"