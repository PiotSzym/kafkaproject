Instructions on how to run the pipeline:

git clone https://github.com/PiotSzym/event-pipeline.git
cd event-pipeline
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt

Kafka + Postgres
docker compose up -d

Airflow DAGs
cd airflow
docker compose up airflow-init   # first run only
docker compose up -d

In 2 separate terminals run these:
python consumer/consume_events.py
python producer/produce_events.py

Airflow UI: http://localhost:8080 (login `airflow` / `airflow`)

To check the results use this cmd:
docker compose exec postgres psql -U pipeline -d events -c "SELECT * FROM request_rollups ORDER BY window_start DESC LIMIT 10;"
