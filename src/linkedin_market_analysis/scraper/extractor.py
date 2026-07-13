import json
import logging
import os
import re
import tempfile
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path
import sys

from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[3]


def get_config():
    job_title = os.environ.get(
        "LINKEDIN_JOB_TITLE",
        "Data Engineer",
    ).strip()

    location = os.environ.get(
        "LINKEDIN_LOCATION",
        "Germany",
    ).strip()

    try:
        max_jobs = int(
            os.environ.get("MAX_JOBS_TO_EXTRACT", "3")
        )
    except ValueError as exc:
        raise RuntimeError(
            "MAX_JOBS_TO_EXTRACT must be an integer"
        ) from exc

    if not job_title:
        raise RuntimeError("LINKEDIN_JOB_TITLE is required")

    if not location:
        raise RuntimeError("LINKEDIN_LOCATION is required")

    if max_jobs < 1:
        raise RuntimeError(
            "MAX_JOBS_TO_EXTRACT must be greater than zero"
        )

    raw_dir = Path(
        os.environ.get(
            "RAW_DIR",
            PROJECT_ROOT / "data" / "raw",
        )
    )

    return job_title, location, max_jobs, raw_dir



def build_search_url(job_title: str, location: str) -> str:
    query = urllib.parse.urlencode(
        {
            "keywords": job_title,
            "location": location,
        }
    )

    return f"https://www.linkedin.com/jobs/search/?{query}"


def normalize_whitespace(text: str) -> str:
    """Collapse repeated whitespace."""
    if not text:
        return ""
    return re.sub(r'\s+', ' ', text).strip()


def is_linkedin_host(hostname: str | None) -> bool:
    if not hostname:
        return False
        
    normalized_host = hostname.lower().rstrip(".")
    
    return (
        normalized_host == "linkedin.com"
        or normalized_host.endswith(".linkedin.com")
    )


def parse_clean_url(raw_url: str) -> tuple[str, str]:
    """Return a clean URL and the stable job ID if valid, else empty strings."""
    if not raw_url:
        return "", ""
        
    absolute_url = urllib.parse.urljoin(
        "https://www.linkedin.com",
        raw_url,
    )
    
    parsed = urllib.parse.urlparse(absolute_url)
    if parsed.scheme != "https" or not is_linkedin_host(parsed.hostname):
        return "", ""
        
    if not parsed.path.startswith("/jobs/view/"):
        return "", ""
        
    job_id_match = re.search(r'/jobs/view/(?:.*-)?(\d+)(?:/|$)', parsed.path)
    if not job_id_match:
        return "", ""
        
    job_id = job_id_match.group(1)
    
    clean_url = urllib.parse.urlunparse(
        (
            "https",
            "www.linkedin.com",
            parsed.path,
            "",
            "",
            "",
        )
    )
    
    return clean_url, job_id


def write_atomic_json(data: dict, filepath: Path) -> None:
    """Write JSON atomically by writing to a temp file first, then replacing."""
    filepath.parent.mkdir(parents=True, exist_ok=True)
    temp_fd, temp_path = tempfile.mkstemp(dir=filepath.parent, suffix=".tmp")
    try:
        with os.fdopen(temp_fd, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        os.replace(temp_path, filepath)
    except Exception as e:
        if os.path.exists(temp_path):
            os.remove(temp_path)
        raise e


def run_extractor() -> int:
    """Fetch job records from LinkedIn and save to an atomic JSON file."""
    job_title, location, max_jobs, raw_dir = get_config()
    search_url = build_search_url(job_title, location)
    
    timestamp = datetime.now(timezone.utc)
    timestamp_str = timestamp.strftime("%Y%m%dT%H%M%SZ")
    iso_timestamp = timestamp.isoformat()
    
    safe_title = re.sub(r'[^a-zA-Z0-9]+', '_', job_title.lower()).strip('_')
    if not safe_title:
        safe_title = "jobs"
    output_filename = f"{timestamp_str}_{safe_title}_jobs.json"
    output_filepath = raw_dir / output_filename
    
    summary = {
        "candidates_collected": 0,
        "valid_urls": 0,
        "jobs_saved": 0,
        "invalid_urls": 0,
        "navigation_failures": 0,
        "unusable_descriptions": 0,
        "identity_failures": 0
    }
    
    output_data = {
        "search": {
            "job_title": job_title,
            "captured_at": iso_timestamp,
            "source": "linkedin_guest_jobs",
            "max_jobs_requested": max_jobs,
            "cards_found": 0
        },
        "summary": summary,
        "jobs": []
    }
    
    logger.info(
        "Starting extraction for %r in %s (max %d jobs)",
        job_title,
        location,
        max_jobs,
    )

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()
        page.set_default_timeout(20000)

        try:
            response = page.goto(search_url, wait_until="domcontentloaded")
            if not response:
                raise RuntimeError("No response received for search page.")
            if response.status >= 400:
                raise RuntimeError(f"Search page returned HTTP {response.status}")
            
            try:
                page.wait_for_selector("div.base-card", timeout=15000)
            except PlaywrightTimeoutError:
                raise RuntimeError(f"No search cards appeared for {job_title}")
                
            base_cards = page.locator("div.base-card")
            total_cards = base_cards.count()
            output_data["search"]["cards_found"] = total_cards
            
            if total_cards == 0:
                raise RuntimeError("No search cards found on the page.")
                
            count = min(max_jobs, total_cards)
            jobs_to_inspect = []
            
            for i in range(count):
                card = base_cards.nth(i)
                title_loc = card.locator(".base-search-card__title")
                title = title_loc.inner_text().strip() if title_loc.count() > 0 else None
                title = normalize_whitespace(title)
                
                comp_loc = card.locator(".base-search-card__subtitle")
                company = comp_loc.inner_text().strip() if comp_loc.count() > 0 else None
                company = normalize_whitespace(company)
                
                loc_loc = card.locator(".job-search-card__location")
                location = loc_loc.inner_text().strip() if loc_loc.count() > 0 else None
                location = normalize_whitespace(location)
                
                date_loc = card.locator("time.job-search-card__listdate")
                date_val = date_loc.get_attribute("datetime") if date_loc.count() > 0 else None
                
                link_loc = card.locator("a.base-card__full-link")
                raw_url = link_loc.get_attribute("href") if link_loc.count() > 0 else ""
                
                jobs_to_inspect.append({
                    "title": title,
                    "company": company,
                    "location": location,
                    "posting_date": date_val,
                    "raw_url": raw_url
                })
                
            summary["candidates_collected"] = len(jobs_to_inspect)
            
            for candidate in jobs_to_inspect:
                if not candidate["title"]:
                    logger.warning("Missing title for candidate. Skipping.")
                    summary["invalid_urls"] += 1
                    continue
                    
                clean_url, job_id = parse_clean_url(candidate["raw_url"])
                if not clean_url or not job_id:
                    logger.warning("Invalid URL for candidate '%s'. Skipping.", candidate["title"])
                    summary["invalid_urls"] += 1
                    continue
                    
                summary["valid_urls"] += 1
                
                sec_page = None
                try:
                    sec_page = context.new_page()
                    response = sec_page.goto(clean_url, wait_until="domcontentloaded", timeout=20000)
                    status = response.status if response else 0
                    
                    if not status or status >= 400:
                        logger.warning("Navigation failed or HTTP error for %s", job_id)
                        summary["navigation_failures"] += 1
                        continue
                        
                    final_url = sec_page.url
                    page_title = sec_page.title()
                    
                    page_title_lower = page_title.lower()
                    final_url_lower = final_url.lower()
                    
                    if any(x in page_title_lower for x in ["sign in", "log in", "security", "consent", "unavailable", "not found"]) or \
                       any(x in final_url_lower for x in ["login", "challenge", "checkpoint", "cookie"]):
                        logger.warning("Blocked or unavailable page for %s", job_id)
                        summary["navigation_failures"] += 1
                        continue
                        
                    desc_selectors = [
                        ".show-more-less-html__markup",
                        ".description__text",
                        ".jobs-description-content__text",
                        ".jobs-description__content",
                        ".jobs-box__html-content",
                        "[class*=\"description\"]"
                    ]
                    
                    best_text = ""
                    for sel in desc_selectors:
                        try:
                            if sel == "[class*=\"description\"]":
                                locs = sec_page.locator(sel)
                                locs_count = locs.count()
                                longest_text = ""
                                for j in range(locs_count):
                                    nth_loc = locs.nth(j)
                                    if nth_loc.is_visible():
                                        text = nth_loc.inner_text()
                                        if text:
                                            text = normalize_whitespace(text)
                                            if len(text) > len(longest_text):
                                                longest_text = text
                                if longest_text:
                                    best_text = longest_text
                                    break
                            else:
                                loc = sec_page.locator(sel).first
                                loc.wait_for(timeout=3000, state="visible")
                                text = loc.inner_text()
                                if text:
                                    best_text = normalize_whitespace(text)
                                    break
                        except PlaywrightTimeoutError:
                            continue
                            
                    if len(best_text) < 200:
                        logger.warning("Description unusable for %s (len=%d)", job_id, len(best_text))
                        summary["unusable_descriptions"] += 1
                        continue
                        
                    # Identity verification
                    final_parsed = urllib.parse.urlparse(final_url)
                    final_job_id_match = re.search(r'-(\d+)(?:/|$)', final_parsed.path)
                    final_job_id = final_job_id_match.group(1) if final_job_id_match else ""
                    
                    id_matches = (job_id == final_job_id)
                    title_matches = bool(candidate["title"]) and candidate["title"].lower() in page_title_lower
                    
                    body_loc = sec_page.locator("body")
                    body_text = body_loc.inner_text()[:5000].lower() if body_loc.count() > 0 else ""
                    company_matches = bool(candidate["company"]) and candidate["company"].lower() in body_text
                    
                    if not id_matches or not (title_matches or company_matches):
                        logger.warning("Identity mismatch for %s", job_id)
                        summary["identity_failures"] += 1
                        continue
                        
                    # Successful extraction
                    job_record = {
                        "linkedin_job_id": job_id,
                        "title": candidate["title"],
                        "company": candidate["company"],
                        "location": candidate["location"],
                        "posting_date": candidate["posting_date"],
                        "job_link": clean_url,
                        "description": best_text
                    }
                    
                    output_data["jobs"].append(job_record)
                    summary["jobs_saved"] += 1
                    logger.info("Successfully extracted job %s (len=%d)", job_id, len(best_text))
                    
                except PlaywrightTimeoutError as e:
                    logger.warning("Timeout navigating to %s: %s", job_id, e)
                    summary["navigation_failures"] += 1
                except Exception as e:
                    logger.warning("Error extracting job %s: %s - %s", job_id, type(e).__name__, e)
                    summary["navigation_failures"] += 1
                finally:
                    if sec_page:
                        sec_page.close()
                        
            if summary["jobs_saved"] == 0:
                raise RuntimeError("Extraction yielded 0 saved jobs")
                
            write_atomic_json(output_data, output_filepath)
            logger.info("Saved %d jobs to %s", summary["jobs_saved"], output_filepath)
            return summary["jobs_saved"]
            
        except RuntimeError as exc:
            raise exc
        except Exception as exc:
            raise RuntimeError(f"Failed to load search page: {exc}")
        finally:
            context.close()
            browser.close()


def main() -> None:
    """Run the Playwright extractor."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    try:
        saved = run_extractor()
        if saved > 0:
            logger.info("Extraction completed with %d saved jobs", saved)
            sys.exit(0)
        else:
            logger.error("Extraction failed")
            sys.exit(1)
    except RuntimeError as e:
        logger.error("Extraction failed: %s", e)
        sys.exit(1)

if __name__ == "__main__":
    main()
