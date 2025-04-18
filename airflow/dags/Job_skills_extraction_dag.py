from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pandas as pd
from keybert import KeyBERT
import json

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 16),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def extract_skills():
    # Connect to PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id="linkedin_postgres_conn")
    conn = pg_hook.get_conn()
    
    # get the last 50 jobs that don't have skills
    query = """
    SELECT id, description 
    FROM jobs 
    WHERE skills IS NULL AND description IS NOT NULL
    LIMIT 50; 
    """
    
    df = pd.read_sql(query, conn)
    
    if df.empty:
        print("No job descriptions to process")
        return
    
    
    kw_model = KeyBERT()
    
    # Extract skills for each job description
    for index, row in df.iterrows():
        job_id = row['id']
        description = row['description']
        
        # Extract skills using KeyBERT
        # We're targeting keywords that are likely to be skills
        keywords = kw_model.extract_keywords(
            description, 
            keyphrase_ngram_range=(1, 2),  # Allow for 1-2 word phrases
            stop_words='english',  # Remove English stop words
            use_maxsum=True,
            nr_candidates=20,
            top_n=10  # Extract top 10 skills
        )
        
        # Convert to list of skills (just the keywords, not the scores)
        skills = [keyword for keyword, _ in keywords]
        
        # Convert to JSON string for storage
        skills_json = json.dumps(skills)
        
        # Update the database
        update_query = """
        UPDATE jobs
        SET skills = %s
        WHERE id = %s;
        """
        
        with conn.cursor() as cur:
            cur.execute(update_query, (skills_json, job_id))
        
        conn.commit()
        print(f"Processed job ID {job_id}, extracted skills: {skills}")
    
    conn.close()
    print(f"Processed {len(df)} job descriptions")

# Define the DAG
dag = DAG(
    'linkedin_skills_extraction',
    default_args=default_args,
    description='Extract skills from LinkedIn job descriptions using KeyBERT',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

extract_skills_task = PythonOperator(
    task_id='extract_skills',
    python_callable=extract_skills,
    dag=dag,
)

# Task dependencies (only one task in this case)
extract_skills_task