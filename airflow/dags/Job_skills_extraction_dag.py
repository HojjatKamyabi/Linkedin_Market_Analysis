from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2
import re
from rapidfuzz import fuzz
import pandas as pd
from collections import Counter
from datetime import datetime
import json


default_args = {
    'owner': 'hojjat',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


data_analyst_skills = [
    # Technical Skills
    "sql", "mysql", "postgresql", "oracle sql", "t-sql", "pl/sql", "nosql", "mongodb",
    "python", "sas", "spss", "matlab", "julia", "scala", "java", "c++",
    "excel", "microsoft office", "google sheets", "libreoffice calc",
    "power bi", "tableau", "looker", "qlikview", "qlik sense", "domo", "google data studio", "sisense",
    "etl", "data integration", "data pipeline", "data modeling", "data warehousing", "data mining",
    "hadoop", "spark", "hive", "pig", "mapreduce", "big data",
    "aws", "azure", "google cloud platform", "cloud computing",
    "machine learning", "deep learning", "neural networks", "tensorflow", "pytorch", "scikit-learn",
    
    # Data Analysis Skills
    "statistical analysis", "statistical modeling", "a/b testing", "hypothesis testing",
    "regression analysis", "time series analysis", "multivariate analysis", "factor analysis",
    "cluster analysis", "predictive modeling", "descriptive analytics", "prescriptive analytics",
    "diagnostic analytics", "predictive analytics", "text analytics", "web analytics",
    
    # Specific Data Analysis Libraries and Packages
    "pandas", "numpy", "scipy", "matplotlib", "seaborn", "plotly", "bokeh", "d3.js",
    "beautiful soup", "scrapy", "ggplot2", "dplyr", "tidyr", "nltk", "scikit-learn",
    
    # Database Skills
    "database design", "database management", "query optimization", "stored procedures",
    "database administration", "rdbms", "schema design", "data normalization",
    
    # Math & Statistics
    "statistics", "probability", "linear algebra", "calculus", "quantitative analysis",
    "bayesian statistics", "frequentist statistics", "experiment design", "sampling methods",
    "variance analysis", "correlation analysis", "confidence intervals", "p-values",
    
    # Business Intelligence
    "kpi development", "metric definition", "dashboard creation",
    "report development", "executive reporting", "data storytelling", "decision support",
    
    # Domain Knowledge
    "financial analysis", "marketing analytics", "sales analytics", "customer analytics",
    "supply chain analytics", "healthcare analytics", "retail analytics", "digital analytics",
    "product analytics", "user behavior analysis", "churn analysis", "funnel analysis",
    "cohort analysis", "rfm analysis", "ltv analysis", "roi analysis", "conversion rate optimization",
    
    # Soft Skills
    "problem solving", "critical thinking", "analytical thinking", "data-driven decision making",
    "communication", "presentation skills", "stakeholder management", "project management",
    "attention to detail", "organization", "time management", "teamwork", "collaboration",
    
    # Process Skills
    "data quality assessment", "data cleaning", "data wrangling", "data preprocessing",
    "data governance", "data documentation", "metadata management", "version control",
    "git", "github", "gitlab", "bitbucket", "agile methodology", "scrum", "jira",
    
    # Advanced Topics
    "nlp", "natural language processing", "computer vision", "image processing",
    "forecasting", "optimization", "simulation", "pattern recognition", "anomaly detection",
    "sentiment analysis", "recommendation systems", "reinforcement learning",
    "feature engineering", "feature selection", "dimensionality reduction", "pca", "t-sne",
    
    # Tools
    "jupyter notebook", "google colab", "rstudio", "databricks", "apache airflow",
    "docker", "kubernetes", "jenkins", "ci/cd", "linux", "shell scripting", "bash",
    "powershell", "data lake", "data mart", "snowflake", "redshift", "bigquery",
    "microsoft sql server", "oracle", "sap hana", "teradata", "ibm db2",
    "alteryx", "knime", "rapidminer", "datarobot", "h2o.ai", "dataiku"
]




def extract_skills():
    
    conn = psycopg2.connect(
            host='host.docker.internal',
            database='##YOUR DATABASE NAME##',
            user='##YOUR USERNAME##',
            password='##YOUR PASSWORD##',
            port='5432'
        )
    cursor = conn.cursor()

    query = "SELECT id ,description FROM jobs"
    cursor.execute(query)
    data = cursor.fetchall()
    
    found = []
    threshold=85
    # Extract skills for each job description
    for ID, description in data:

        cleaned_description = re.sub(r'[^\w\s]', '', description.lower())
        skillset = []
        for skill in data_analyst_skills:
            if fuzz.partial_ratio(skill, cleaned_description) > threshold:
                found.append(skill)
                skillset.append(skill)
        
        skills_json = json.dumps(skillset)
        update_query = """
        UPDATE jobs
        SET skills = %s
        WHERE id = %s;
        """
        cursor.execute(update_query, (skills_json, ID))

    conn.commit()
    cursor.close()
    conn.close()

    if found:
        skill_counts = Counter(found)
        top_skills = skill_counts.most_common(20)

        #Save back to PostgreSQL
        conn = psycopg2.connect(
                host='host.docker.internal',
                database='##YOUR DATABASE NAME##',
                user='##YOUR USERNAME##',
                password='##YOUR PASSWORD##',
                port='5432'
            )
        cursor = conn.cursor()

        for skill , count in top_skills:
            query = """
            INSERT INTO top_skills (skill , count , date)
            VALUES (%s, %s, %s)
            """
        cursor.execute(query, (skill, count, datetime.now()))

    conn.commit() 
    cursor.close()
    conn.close()
    print(f"Processed {len(data)} job descriptions")

# Define the DAG
dag = DAG(
    'linkedin_skills_extraction',
    default_args=default_args,
    description='Extract skills from LinkedIn job descriptions using fuzzy logic',
    schedule_interval=@weekly,
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
