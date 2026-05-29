import logging
from datetime import datetime
from pathlib import Path

from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
RAW_DIR = PROJECT_ROOT / "data" / "raw"


def build_search_url(job_title: str) -> str:
    """Build the LinkedIn jobs search URL for a job title."""
    return "https://www.linkedin.com/jobs/search/?keywords=" + job_title.replace(" ", "%20")


def ensure_raw_dir(raw_dir: Path) -> None:
    """Create the raw data directory if it does not exist."""
    raw_dir.mkdir(parents=True, exist_ok=True)


def write_raw_html(raw_dir: Path, timestamp: str, job_title: str, html: str) -> Path:
    """Write raw page HTML to a file and return its path."""
    safe_title = job_title.lower().replace(" ", "_")
    file_path = raw_dir / f"{timestamp}_{safe_title}_search.html"
    file_path.write_text(html, encoding="utf-8")
    return file_path


def fetch_search_html(job_title: str, timeout_ms: int = 20000) -> str:
    """Navigate to LinkedIn search and return the raw HTML of the page."""
    search_url = build_search_url(job_title)
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()
        page.set_default_timeout(timeout_ms)

        try:
            page.goto(search_url, wait_until="domcontentloaded", timeout=timeout_ms)
            return page.content()
        except PlaywrightTimeoutError as exc:
            logger.warning("Timeout loading %s: %s", search_url, exc)
            return ""
        except Exception as exc:
            logger.warning("Failed to load %s: %s", search_url, exc)
            return ""
        finally:
            context.close()
            browser.close()


def run_extractor(job_title: str, raw_dir: Path) -> None:
    """Fetch and store the raw HTML for a LinkedIn search page."""
    ensure_raw_dir(raw_dir)
    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    html = fetch_search_html(job_title)

    if not html:
        logger.warning("No HTML captured for %s", job_title)
        return

    file_path = write_raw_html(raw_dir, timestamp, job_title, html)
    logger.info("Saved raw HTML to %s", file_path)


def main() -> None:
    """Run the Playwright extractor with a default job title."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    run_extractor("Data Engineer", RAW_DIR)


if __name__ == "__main__":
    main()
