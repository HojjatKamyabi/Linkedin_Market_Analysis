from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'hojjat',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'linkedin_scraper_dag',
    default_args=default_args,
    description='Run LinkedIn scraper with Docker',
    schedule_interval=None,
    start_date=datetime(2025, 4, 14),
    catchup=False,
    tags=['linkedin', 'scraping'],
) as dag:

    run_scraper = BashOperator(
        task_id='run_scraper',
        bash_command='cd /opt/airflow/scraper && python3 main.py',
    )