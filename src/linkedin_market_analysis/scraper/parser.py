import logging
import os
import re
import shutil
from datetime import datetime
from pathlib import Path
from typing import Optional

import psycopg2
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from psycopg2.extensions import connection as PgConnection

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
RAW_DIR = PROJECT_ROOT / "data" / "raw"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
load_dotenv(PROJECT_ROOT / ".env")


def connect_to_db() -> Optional[PgConnection]:
    """Connect to PostgreSQL and return the connection if successful."""
    try:
        host = os.getenv("DB_HOST")
        database = os.getenv("DB_NAME")
        user = os.getenv("DB_USER")
        password = os.getenv("DB_PASSWORD")
        port = os.getenv("DB_PORT", "5432")

        if not host or not database or not user or not password:
            logger.error("Missing required database environment variables.")
            return None

        conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password,
            port=port,
        )
        return conn
    except psycopg2.OperationalError as exc:
        logger.error("Connection to postgresql failed: %s", exc)
        return None


def create_tables(conn: PgConnection) -> None:
    """Create database tables if they do not exist."""
    with conn.cursor() as cursor:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS jobs (
                id SERIAL PRIMARY KEY,
                title TEXT NOT NULL,
                company TEXT NOT NULL,
                location TEXT NOT NULL,
                date_posted TEXT,
                description TEXT,
                job_link TEXT UNIQUE,
                scrape_date TIMESTAMP NOT NULL,
                skills JSONB,
                -- Columns reserved for AI-enriched data to keep the pipeline forward-compatible.
                required_skills JSONB,
                seniority_level TEXT,
                llm_model TEXT,
                llm_extracted_at TIMESTAMP
            );
            """
        )
        conn.commit()
    logger.info("Database tables created/verified")


def parse_job_description(description_html: str) -> str:
    """Normalize the job description HTML into readable text."""
    soup = BeautifulSoup(description_html, "html.parser")

    for li in soup.find_all("li"):
        li.insert_before("+ ")

    for br in soup.find_all(["br", "p"]):
        br.insert_after("\n")

    description = soup.get_text()
    description = re.sub(r"\n\s*\n", "\n\n", description)
    return description.strip()


def parse_job_card(job_html: str) -> tuple[str, str, str, str, Optional[str]]:
    """Extract job metadata and link from a job card's HTML."""
    job_soup = BeautifulSoup(job_html, "html.parser")
    title_elem = job_soup.select_one(".base-search-card__title")
    title = title_elem.text.strip() if title_elem else "Not available"

    company_elem = job_soup.select_one(".base-search-card__subtitle")
    company = company_elem.text.strip() if company_elem else "Not available"

    location_elem = job_soup.select_one(".job-search-card__location")
    location_name = location_elem.text.strip() if location_elem else "Not available"

    date_elem = job_soup.select_one("time.job-search-card__listdate")
    date = date_elem.get("datetime") if date_elem else "Not available"

    link_elem = job_soup.select_one("a.base-card__full-link")
    job_link = link_elem.get("href") if link_elem else None

    return title, company, location_name, date, job_link


def insert_job(
    conn: PgConnection,
    title: str,
    company: str,
    location: str,
    date_posted: str,
    description: str,
    job_link: str,
) -> None:
    """Insert a job record into the database if it does not already exist."""
    with conn.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO jobs (
                title,
                company,
                location,
                date_posted,
                description,
                job_link,
                scrape_date,
                required_skills,
                seniority_level,
                llm_model,
                llm_extracted_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (job_link) DO NOTHING
            RETURNING id
            """,
            (
                title,
                company,
                location,
                date_posted,
                description,
                job_link,
                datetime.now(),
                # Default AI-enriched fields to NULL until extraction is implemented.
                None,
                None,
                None,
                None,
            ),
        )
        conn.commit()


def ensure_processed_dir(processed_dir: Path) -> None:
    """Create the processed data directory if it does not exist."""
    processed_dir.mkdir(parents=True, exist_ok=True)


def load_raw_pairs(raw_dir: Path) -> list[tuple[Path, Path]]:
    """Collect matching job card and description HTML file pairs."""
    card_files = sorted(raw_dir.glob("*_card.html"))
    pairs: list[tuple[Path, Path]] = []

    for card_path in card_files:
        base_name = card_path.name.replace("_card.html", "")
        desc_path = raw_dir / f"{base_name}_description.html"
        if desc_path.exists():
            pairs.append((card_path, desc_path))
        else:
            logger.warning("Missing description file for %s", card_path.name)

    return pairs


def read_html(path: Path) -> str:
    """Read HTML from disk."""
    return path.read_text(encoding="utf-8", errors="ignore")


def process_pair(conn: PgConnection, card_path: Path, desc_path: Path) -> bool:
    """Parse, insert, and return whether the job was successfully loaded."""
    job_html = read_html(card_path)
    description_html = read_html(desc_path)
    title, company, location, date_posted, job_link = parse_job_card(job_html)

    if not job_link:
        logger.warning("Skipping %s: missing job link", card_path.name)
        return False

    description = parse_job_description(description_html)
    insert_job(conn, title, company, location, date_posted, description, job_link)
    logger.info("Inserted job from %s", card_path.name)
    return True


def archive_files(processed_dir: Path, card_path: Path, desc_path: Path) -> None:
    """Move processed files to the processed directory."""
    ensure_processed_dir(processed_dir)
    shutil.move(str(card_path), processed_dir / card_path.name)
    shutil.move(str(desc_path), processed_dir / desc_path.name)


def run_parser(raw_dir: Path, processed_dir: Path) -> None:
    """Parse raw HTML files and load them into PostgreSQL."""
    conn = connect_to_db()
    if conn is None:
        logger.error("Exiting: Could not connect to the database.")
        return

    create_tables(conn)

    pairs = load_raw_pairs(raw_dir)
    logger.info("Found %s raw job pairs", len(pairs))

    for card_path, desc_path in pairs:
        try:
            if process_pair(conn, card_path, desc_path):
                archive_files(processed_dir, card_path, desc_path)
        except Exception:
            logger.exception("Error processing %s", card_path.name)

    conn.close()
    logger.info("Parsing completed")


def main() -> None:
    """Entry point for parsing raw files into the database."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    run_parser(RAW_DIR, PROCESSED_DIR)


if __name__ == "__main__":
    main()
