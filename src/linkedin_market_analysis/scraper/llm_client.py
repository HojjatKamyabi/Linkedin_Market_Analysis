import json
import logging
import os
import re
from typing import Optional

from openai import OpenAI, APIError, APIConnectionError, RateLimitError, AuthenticationError, APITimeoutError

logger = logging.getLogger(__name__)

MAX_DESCRIPTION_CHARS = 4000

def get_client_config() -> tuple[str, str, str]:
    """Read OpenRouter configuration."""
    api_key = os.environ.get("LLM_API_KEY", "")
    if not api_key:
        raise ValueError("LLM_API_KEY environment variable is missing or empty.")
    
    base_url = os.environ.get("LLM_BASE_URL", "https://openrouter.ai/api/v1")
    model = os.environ.get("LLM_MODEL", "openrouter/free")
    
    return api_key, base_url, model

def normalize_payload(data: dict) -> dict | None:
    """Normalize the returned JSON payload."""
    if not isinstance(data, dict):
        return None
        
    skills_raw = data.get("required_skills")
    if skills_raw is None:
        skills = []
    elif isinstance(skills_raw, list):
        skills = skills_raw
    elif isinstance(skills_raw, str):
        skills = skills_raw.split(',')
    else:
        return None
        
    clean_skills = []
    seen = set()
    for s in skills:
        if isinstance(s, str):
            s_clean = s.strip()
            if s_clean:
                s_lower = s_clean.lower()
                if s_lower not in seen:
                    seen.add(s_lower)
                    clean_skills.append(s_clean)
                    if len(clean_skills) >= 30:
                        break
                        
    seniority_raw = data.get("seniority_level")
    canonical_seniority = "Unknown"
    valid_levels = {
        "intern": "Intern",
        "entry": "Entry",
        "junior": "Junior",
        "mid": "Mid",
        "senior": "Senior",
        "lead": "Lead",
        "manager": "Manager",
        "director": "Director",
        "executive": "Executive",
        "unknown": "Unknown"
    }
    
    if isinstance(seniority_raw, str):
        s_lower = seniority_raw.strip().lower()
        if s_lower in valid_levels:
            canonical_seniority = valid_levels[s_lower]
            
    return {
        "required_skills": clean_skills,
        "seniority_level": canonical_seniority
    }

def clean_response_content(content: str) -> dict | None:
    """Extract and parse JSON from the response content."""
    if not content:
        return None
        
    content = content.strip()
    
    # Strip markdown fences
    if content.startswith("```"):
        lines = content.splitlines()
        if len(lines) > 1 and lines[-1].strip() == "```":
            content = "\n".join(lines[1:-1])
            
    # Try to extract json object if it has surrounding text
    start_idx = content.find('{')
    end_idx = content.rfind('}')
    if start_idx != -1 and end_idx != -1 and end_idx > start_idx:
        content = content[start_idx:end_idx+1]
        
    try:
        return json.loads(content)
    except json.JSONDecodeError:
        return None

def extract_job_info(description: str) -> Optional[dict]:
    """Send description to OpenRouter and return extracted info or None on failure."""
    if not description or not description.strip():
        return {
            "required_skills": [],
            "seniority_level": "Unknown"
        }
        
    try:
        api_key, base_url, model = get_client_config()
    except ValueError as e:
        logger.error("Configuration error: %s", e)
        return None
        
    desc_clean = re.sub(r'\s+', ' ', description).strip()
    if len(desc_clean) > MAX_DESCRIPTION_CHARS:
        desc_clean = desc_clean[:MAX_DESCRIPTION_CHARS]
        
    client = OpenAI(
        api_key=api_key,
        base_url=base_url,
        max_retries=2,
        timeout=30.0
    )
    
    system_prompt = (
        "You are a job description analyzer. Extract the required skills and seniority level.\n"
        "Return ONLY a JSON object with two keys: 'required_skills' and 'seniority_level'.\n"
        "'required_skills' must be an array of concise technical skills (e.g. ['Python', 'SQL', 'AWS']). Do not include soft skills.\n"
        "'seniority_level' must be exactly one of: Intern, Entry, Junior, Mid, Senior, Lead, Manager, Director, Executive, Unknown.\n"
        "Do not include Markdown formatting or explanatory text."
    )
    
    try:
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": desc_clean}
            ],
            temperature=0,
            max_tokens=1000,
            response_format={
                "type": "json_schema",
                "json_schema": {
                    "name": "job_intelligence",
                    "strict": True,
                    "schema": {
                        "type": "object",
                        "properties": {
                            "required_skills": {
                                "type": "array",
                                "items": {"type": "string"}
                            },
                            "seniority_level": {
                                "type": "string",
                                "enum": [
                                    "Intern",
                                    "Entry",
                                    "Junior",
                                    "Mid",
                                    "Senior",
                                    "Lead",
                                    "Manager",
                                    "Director",
                                    "Executive",
                                    "Unknown"
                                ]
                            }
                        },
                        "required": [
                            "required_skills",
                            "seniority_level"
                        ],
                        "additionalProperties": False
                    }
                }
            },
            extra_body={
                "provider": {
                    "require_parameters": True
                }
            }
        )
        
        model_id = getattr(response, 'model', 'unknown')
        choices = getattr(response, 'choices', [])
        num_choices = len(choices)
        
        if num_choices == 0:
            logger.warning("No choices returned. (model=%s)", model_id)
            return None
            
        choice = choices[0]
        finish_reason = getattr(choice, 'finish_reason', 'unknown')
        message = getattr(choice, 'message', None)
        content = getattr(message, 'content', None) if message else None
        
        if content is None:
            logger.warning("First message content is null. (model=%s, finish_reason=%s, choices=%d)", model_id, finish_reason, num_choices)
            return None
            
        content_str = content.strip()
        if not content_str:
            logger.warning("First message content is an empty or whitespace-only string. (model=%s, finish_reason=%s, choices=%d)", model_id, finish_reason, num_choices)
            return None
            
        parsed = clean_response_content(content_str)
        if parsed is None:
            logger.warning("Content exists but is not valid JSON. (model=%s, finish_reason=%s, choices=%d, length=%d)", model_id, finish_reason, num_choices, len(content_str))
            return None
            
        if not isinstance(parsed, dict):
            logger.warning("JSON is valid but has an invalid top-level structure. (model=%s, finish_reason=%s, choices=%d, length=%d)", model_id, finish_reason, num_choices, len(content_str))
            return None
            
        normalized = normalize_payload(parsed)
        if normalized is None:
            logger.warning("JSON is valid but payload normalization fails. (model=%s, finish_reason=%s, choices=%d, length=%d)", model_id, finish_reason, num_choices, len(content_str))
            return None
            
        return normalized
        
    except AuthenticationError as e:
        logger.error("Authentication failure: %s", type(e).__name__)
        return None
    except RateLimitError as e:
        logger.error("Rate limiting: %s", type(e).__name__)
        return None
    except APITimeoutError as e:
        logger.error("Timeout: %s", type(e).__name__)
        return None
    except APIConnectionError as e:
        logger.error("Connection failure: %s", type(e).__name__)
        return None
    except APIError as e:
        status_code = getattr(e, 'status_code', getattr(getattr(e, 'response', None), 'status_code', 'unknown'))
        logger.error("API error (status code %s): %s", status_code, type(e).__name__)
        return None
    except Exception as e:
        logger.error("Unexpected error: %s", type(e).__name__)
        return None
