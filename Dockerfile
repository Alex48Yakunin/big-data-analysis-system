FROM apache:airflow
WORKDIR /opt/airflow
COPY dags /opt/airflow/dags
COPY logs /opt/airflow/logs
CMD ["airflow", "webserver", "-p", "9090"]
