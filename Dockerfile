FROM apache/airflow:2.9.1
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" && \
    pip install -r requirements.txt