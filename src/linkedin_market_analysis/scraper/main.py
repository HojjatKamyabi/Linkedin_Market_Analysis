import logging
import re
import time
from datetime import datetime
from typing import Optional

import psycopg2
from bs4 import BeautifulSoup
from psycopg2.extensions import connection as PgConnection
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.webdriver import WebDriver
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webelement import WebElement

logger = logging.getLogger(__name__)


def connect_to_db() -> Optional[PgConnection]:
    """Connect to PostgreSQL and return the connection if successful."""
    try:
        conn = psycopg2.connect(
            host="host.docker.internal",
            database="## YOUR DATABASE NAME##",
            user="## YOUR USERNAME##",
            password="## YOUR PASSWORD##",
            port="5432",
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
                scrape_date TIMESTAMP NOT NULL
            );
            """
        )
        conn.commit()
    logger.info("Database tables created/verified")


def build_search_url(job_title: str, location: str) -> str:
    """Build the LinkedIn jobs search URL for a job title and location."""
    return (
        "https://www.linkedin.com/jobs/search/?"
        f"keywords={job_title.replace(' ', '%20')}&"
        f"location={location.replace(' ', '%20')}&"
        "geoId=101282230&f_E=1%2C2&f_TPR=r604800&position=1&pageNum=0"
    )


def create_driver() -> WebDriver:
    """Create a headless Chrome WebDriver configured for scraping."""
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-notifications")
    return webdriver.Chrome(options=options)


def load_job_cards(
    driver: WebDriver, search_url: str, scrolls: int = 3, wait_seconds: float = 3.0
) -> list[WebElement]:
    """Load the search page and return the job cards found after scrolling."""
    driver.get(search_url)
    time.sleep(wait_seconds)

    job_cards: list[WebElement] = []
    for _ in range(scrolls):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(wait_seconds)
        job_cards = driver.find_elements(By.CSS_SELECTOR, "div.job-search-card")

    return job_cards


def get_job_link(job: WebElement) -> Optional[str]:
    """Extract the job link from a job card element."""
    link_elem = job.find_element(By.CSS_SELECTOR, "a.base-card__full-link")
    return link_elem.get_attribute("href") if link_elem else None


def fetch_job_description_html(
    driver: WebDriver, job_link: str, wait_seconds: float = 3.0
) -> str:
    """Open the job link in a new tab and return its description HTML."""
    driver.execute_script("window.open(arguments[0], '_blank');", job_link)
    time.sleep(wait_seconds)
    driver.switch_to.window(driver.window_handles[1])
    desc_elem = driver.find_element(By.CSS_SELECTOR, "div.show-more-less-html__markup")
    description_html = desc_elem.get_attribute("innerHTML") or ""
    driver.close()
    driver.switch_to.window(driver.window_handles[0])
    return description_html


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


def parse_job_card(job_html: str) -> tuple[str, str, str, str]:
    """Extract job metadata from a job card's HTML."""
    job_soup = BeautifulSoup(job_html, "html.parser")
    title_elem = job_soup.select_one(".base-search-card__title")
    title = title_elem.text.strip() if title_elem else "Not available"

    company_elem = job_soup.select_one(".base-search-card__subtitle")
    company = company_elem.text.strip() if company_elem else "Not available"

    location_elem = job_soup.select_one(".job-search-card__location")
    location_name = location_elem.text.strip() if location_elem else "Not available"

    date_elem = job_soup.select_one("time.job-search-card__listdate")
    date = date_elem.get("datetime") if date_elem else "Not available"

    return title, company, location_name, date


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
            INSERT INTO jobs (title, company, location, date_posted, description, job_link, scrape_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
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
            ),
        )
        conn.commit()


def scrape_linkedin(job_title: str, location: str, conn: PgConnection) -> None:
    """Scrape LinkedIn job listings for a title and location."""
    driver = create_driver()
    try:
        search_url = build_search_url(job_title, location)
        job_cards = load_job_cards(driver, search_url)
        logger.info("Found %s job cards for %s in %s", len(job_cards), job_title, location)

        for index, job in enumerate(job_cards, start=1):
            job_link = get_job_link(job)
            if not job_link:
                logger.warning("Skipping job %s: missing link", index)
                continue

            try:
                description_html = fetch_job_description_html(driver, job_link)
                description = parse_job_description(description_html)
                job_html = job.get_attribute("outerHTML") or ""
                title, company, location_name, date_posted = parse_job_card(job_html)
                insert_job(
                    conn,
                    title,
                    company,
                    location_name,
                    date_posted,
                    description,
                    job_link,
                )
                logger.info("Processed job %s/%s", index, len(job_cards))
            except Exception:
                logger.exception("Error processing job %s", index)
                if len(driver.window_handles) > 1:
                    driver.close()
                    driver.switch_to.window(driver.window_handles[0])
                continue
    finally:
        driver.quit()


def main() -> None:
    """Run the scraper for configured titles and locations."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

    conn = connect_to_db()
    if conn is None:
        logger.error("Exiting: Could not connect to the database.")
        return

    create_tables(conn)

    locations = ["Germany"]
    job_titles = ["data analyst"]

    for job_title in job_titles:
        for location in locations:
            scrape_linkedin(job_title, location, conn)

    logger.info("Scraping completed")
    conn.close()


if __name__ == "__main__":
    main()
