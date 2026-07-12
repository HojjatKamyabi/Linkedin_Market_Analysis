import json
import logging
import os
import re
import shutil
import urllib.parse
from datetime import datetime, date
from pathlib import Path
from typing import Optional, Any

import psycopg2
from psycopg2.extensions import connection as PgConnection
from psycopg2.extras import Json
import sys

from llm_client import extract_job_info

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
RAW_DIR = Path(os.environ.get("RAW_DIR", PROJECT_ROOT / "data" / "raw"))
PROCESSED_DIR = Path(os.environ.get("PROCESSED_DIR", PROJECT_ROOT / "data" / "processed"))

def get_db_connection() -> PgConnection:
    host = os.environ.get("DB_HOST", "postgres")
    port = os.environ.get("DB_PORT", "5432")
    db_name = os.environ.get("DB_NAME")
    user = os.environ.get("DB_USER")
    password = os.environ.get("DB_PASSWORD")

    if not all([db_name, user, password]):
        raise RuntimeError("Missing required DB configuration (DB_NAME, DB_USER, DB_PASSWORD).")

    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            database=db_name,
            user=user,
            password=password
        )
        return conn
    except Exception as e:
        raise RuntimeError(f"Failed to connect to database: {type(e).__name__}")

def create_table_if_not_exists(conn: PgConnection) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
                id SERIAL PRIMARY KEY,
                linkedin_job_id TEXT,
                title TEXT NOT NULL,
                company TEXT,
                location TEXT,
                posting_date DATE,
                description TEXT NOT NULL,
                job_link TEXT UNIQUE NOT NULL,
                scrape_date TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                skills JSONB,
                required_skills JSONB,
                seniority_level TEXT,
                llm_model TEXT,
                llm_extracted_at TIMESTAMP WITH TIME ZONE
            );
        """)
        conn.commit()

def normalize_whitespace(text: Optional[str]) -> Optional[str]:
    if not isinstance(text, str):
        return None
    cleaned = re.sub(r'\s+', ' ', text).strip()
    return cleaned if cleaned else None

def parse_date(date_str: Any) -> Optional[date]:
    if not isinstance(date_str, str):
        return None
    try:
        dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        return dt.date()
    except ValueError:
        logger.warning("Malformed date: %s", date_str)
        return None

def process_job(conn: PgConnection, job: dict) -> str:
    """Returns state: 'inserted', 'duplicate', 'invalid', 'llm_failure', 'db_failure', 'error'"""
    try:
        job_id = str(job.get('linkedin_job_id', '')).strip()
        title = normalize_whitespace(job.get('title'))
        job_link = str(job.get('job_link', '')).strip()
        description = job.get('description', '')

        if not job_id or not job_id.isdigit() or not title or not job_link or not isinstance(description, str) or not description.strip():
            logger.warning("Invalid job record: missing or malformed required fields.")
            return 'invalid'

        parsed = urllib.parse.urlparse(job_link)
        if parsed.scheme != 'https' or parsed.netloc not in ['linkedin.com', 'www.linkedin.com']:
            logger.warning("Invalid job URL host or scheme for job %s", job_id)
            return 'invalid'
        
        if not parsed.path.startswith('/jobs/view/'):
            logger.warning("Invalid job URL path for job %s", job_id)
            return 'invalid'
            
        id_match = re.search(r'-(\d+)(?:/|$)', parsed.path)
        if not id_match or id_match.group(1) != job_id:
            logger.warning("URL job ID mismatch for job %s", job_id)
            return 'invalid'
            
        clean_url = urllib.parse.urlunparse((parsed.scheme, parsed.netloc, parsed.path, '', '', ''))
        
        company = normalize_whitespace(job.get('company'))
        location = normalize_whitespace(job.get('location'))
        posting_date = parse_date(job.get('posting_date'))
        
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM jobs WHERE job_link = %s LIMIT 1", (clean_url,))
            if cur.fetchone():
                return 'duplicate'
                
        llm_result = extract_job_info(description)
        if llm_result is None:
            logger.warning("LLM extraction failed for job %s", job_id)
            return 'llm_failure'
            
        req_skills_raw = llm_result.get('required_skills')
        seniority = llm_result.get('seniority_level')
        
        if not isinstance(req_skills_raw, list) or not isinstance(seniority, str) or not seniority.strip():
            logger.warning("LLM validation failed for job %s: invalid types", job_id)
            return 'llm_failure'
            
        for s in req_skills_raw:
            if not isinstance(s, str) or not s.strip():
                logger.warning("LLM validation failed for job %s: invalid skill string", job_id)
                return 'llm_failure'
                
        req_skills = [s.strip() for s in req_skills_raw]
        llm_model = os.environ.get("LLM_MODEL", "openrouter/free")
        
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO jobs (
                    linkedin_job_id, title, company, location, posting_date,
                    description, job_link, scrape_date, required_skills,
                    seniority_level, llm_model, llm_extracted_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, %s, %s, %s, CURRENT_TIMESTAMP
                ) ON CONFLICT (job_link) DO NOTHING
            """, (
                job_id, title, company, location, posting_date,
                description, clean_url, Json(req_skills), seniority, llm_model
            ))
            
            if cur.rowcount > 0:
                conn.commit()
                return 'inserted'
            else:
                conn.commit()
                return 'duplicate'
                
    except psycopg2.Error as e:
        conn.rollback()
        logger.error("Database error for job %s: %s", job.get('linkedin_job_id', 'unknown'), type(e).__name__)
        return 'db_failure'
    except Exception as e:
        logger.error("Unexpected error processing job: %s", type(e).__name__)
        return 'error'

def process_batch(conn: PgConnection, filepath: Path) -> dict:
    stats = {
        'total': 0, 'inserted': 0, 'duplicate': 0, 'invalid': 0,
        'llm_failure': 0, 'db_failure': 0, 'error': 0, 'archived': False
    }
    
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        if not isinstance(data, dict) or not isinstance(data.get('jobs'), list):
            logger.error("Malformed batch file: %s", filepath.name)
            return stats
            
        jobs = data['jobs']
        stats['total'] = len(jobs)
        
        if not jobs:
            logger.error("Empty jobs array in batch file: %s", filepath.name)
            return stats
        
        terminal_states = {'inserted', 'duplicate', 'invalid'}
        all_terminal = True
        
        for job in jobs:
            state = process_job(conn, job)
            stats[state] += 1
            if state not in terminal_states:
                all_terminal = False
                
        if all_terminal:
            PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
            dest = PROCESSED_DIR / filepath.name
            if dest.exists():
                logger.error("Archive destination already exists for %s", filepath.name)
            else:
                shutil.move(str(filepath), dest)
                stats['archived'] = True
                logger.info("Archived %s", filepath.name)
                
    except json.JSONDecodeError:
        logger.error("Failed to parse JSON in %s", filepath.name)
    except Exception as e:
        logger.error("Unexpected error processing batch %s: %s", filepath.name, type(e).__name__)
        
    return stats

def run_parser() -> bool:
    try:
        conn = get_db_connection()
    except RuntimeError as e:
        logger.error("Database connection error: %s", e)
        return False
        
    try:
        try:
            create_table_if_not_exists(conn)
        except Exception as e:
            logger.error("Table creation failed: %s", e)
            return False
            
        raw_files = sorted(RAW_DIR.glob("*_jobs.json"))
        if not raw_files:
            logger.info("No matching *_jobs.json files found in %s", RAW_DIR)
            return True
            
        total_stats = {
            'total': 0, 'inserted': 0, 'duplicate': 0, 'invalid': 0,
            'llm_failure': 0, 'db_failure': 0, 'error': 0, 'archived_batches': 0
        }
        
        all_batches_successful = True
        
        for filepath in raw_files:
            logger.info("Processing batch %s", filepath.name)
            stats = process_batch(conn, filepath)
            
            logger.info("Batch %s results: %d total, %d inserted, %d duplicate, %d invalid, %d LLM fail, %d DB fail, %d error, archived=%s",
                filepath.name, stats['total'], stats['inserted'], stats['duplicate'], stats['invalid'],
                stats['llm_failure'], stats['db_failure'], stats['error'], stats['archived'])
                
            if not stats.get('archived'):
                all_batches_successful = False
                
            for k in stats:
                if k == 'archived':
                    if stats[k]: total_stats['archived_batches'] += 1
                else:
                    total_stats[k] += stats[k]
                    
        logger.info("Run summary: %d total jobs, %d inserted, %d duplicate, %d invalid, %d LLM fail, %d DB fail, %d error, %d batches archived",
            total_stats['total'], total_stats['inserted'], total_stats['duplicate'], total_stats['invalid'],
            total_stats['llm_failure'], total_stats['db_failure'], total_stats['error'], total_stats['archived_batches'])
            
        return all_batches_successful
            
    finally:
        conn.close()

def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    success = run_parser()
    if success:
        logger.info("Parser completed successfully")
        sys.exit(0)
    else:
        logger.error("Parser completed with incomplete batches")
        sys.exit(1)

if __name__ == "__main__":
    main()
