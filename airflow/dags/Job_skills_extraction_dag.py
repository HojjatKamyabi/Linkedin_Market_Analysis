from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from keybert import KeyBERT
import json
import psycopg2


default_args = {
    'owner': 'hojjat',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def extract_skills():
    
    conn = psycopg2.connect(
            host='host.docker.internal',
            database='Linkedin_data',
            user='postgres',
            password='95059505',
            port='5432'
        )
    cursor = conn.cursor()

    # get the last 50 jobs that don't have skills
    query = """
    SELECT id, description 
    FROM jobs 
    WHERE skills IS NULL AND description IS NOT NULL
    LIMIT 50; 
    """
    cursor.execute(query)
    data = cursor.fetchall()
    
    kw_model = KeyBERT()
    
    # Extract skills for each job description
    for row in data:
        job_id = row[0]
        description = row[1]
        
        # Extract skills using KeyBERT
        # We're targeting keywords that are likely to be skills
        keywords = kw_model.extract_keywords(
            description, 
            keyphrase_ngram_range=(1, 1),  # Allow for 1 word phrases
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
        cursor.execute(update_query, (skills_json, job_id))
        
        conn.commit()
        print(f"Processed job ID {job_id}, extracted skills: {skills}")
    
    conn.close()
    print(f"Processed {len(data)} job descriptions")

# Define the DAG
dag = DAG(
    'linkedin_skills_extraction',
    default_args=default_args,
    description='Extract skills from LinkedIn job descriptions using KeyBERT',
    schedule_interval=None,
    start_date=datetime(2025, 4, 14),
    catchup=False,
)

extract_skills_task = PythonOperator(
    task_id='extract_skills',
    python_callable=extract_skills,
    dag=dag,
)

# Task dependencies (only one task in this case)
extract_skills_task
