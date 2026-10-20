from datetime import datetime

import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator


def aggregate_requests() -> None:
    # host.docker.internal = your machine, where the pipeline's Postgres publishes port 5432
    conn = psycopg2.connect(
        host="host.docker.internal", port=5432,
        dbname="events", user="pipeline", password="pipeline",
    )
    with conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO request_rollups (window_start, request_type, accepted_count)
            SELECT date_bin('15 minutes', event_timestamp, TIMESTAMPTZ '2000-01-01'),
                   request_type,
                   COUNT(*)
            FROM accepted_requests
            GROUP BY 1, 2
            ON CONFLICT (window_start, request_type)
            DO UPDATE SET accepted_count = EXCLUDED.accepted_count
            """
        )
    conn.close()


with DAG(
    dag_id="request_rollups",
    start_date=datetime(2026, 10, 1),
    schedule="*/15 * * * *",
    catchup=False,
) as dag:
    PythonOperator(task_id="aggregate_requests", python_callable=aggregate_requests)