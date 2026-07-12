import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'retries': 0,
}

with DAG(
    dag_id='linkedin_data_pipeline',
    default_args=default_args,
    description='LinkedIn Market Analysis Pipeline',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['linkedin', 'data engineering', 'llm'],
) as dag:

    extract_jobs = BashOperator(
        task_id='extract_jobs',
        bash_command='python3 /opt/airflow/scraper/extractor.py',
    )

    enrich_and_load_jobs = BashOperator(
        task_id='enrich_and_load_jobs',
        bash_command='python3 /opt/airflow/scraper/parser.py',
        append_env=True,
        env={
            'DB_HOST': 'postgres',
            'DB_PORT': os.environ.get('DB_PORT', '5432'),
            'DB_NAME': 'linkedin_market',
            'RAW_DIR': '/opt/airflow/data/raw',
            'PROCESSED_DIR': '/opt/airflow/data/processed',
            'LLM_BASE_URL': os.environ.get('LLM_BASE_URL', 'https://openrouter.ai/api/v1'),
            'LLM_MODEL': os.environ.get('LLM_MODEL', 'openai/gpt-oss-20b:free'),
        },
    )

    extract_jobs >> enrich_and_load_jobs
