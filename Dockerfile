FROM apache/airflow:2.8.4
COPY requirements.txt .
USER airflow
RUN pip install -r requirements.txt
