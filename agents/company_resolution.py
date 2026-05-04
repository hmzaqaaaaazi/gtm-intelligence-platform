import json
import logging
import os
import re

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

GROQ_API_URL = "https://api.groq.com/openai/v1/chat/completions"
GROQ_MODEL = "llama-3.1-8b-instant"


def resolve_company_name(raw_name: str) -> dict:
    """
    Use Groq LLM to resolve a raw company name into structured fields.
    Returns a dict with: canonical_name, ticker, domain, industry, hq_city, hq_state.
    """
    api_key = os.environ.get("GROQ_API_KEY")
    if not api_key:
        logger.error("GROQ_API_KEY not set.")
        return _empty_resolution(raw_name)

    prompt = f"""
You are a company data resolution assistant. Given a raw company name, return a JSON object with:
- canonical_name: the clean, official company name
- ticker: stock ticker symbol if public, else null
- domain: primary website domain (e.g. "acme.com"), else null
- industry: one-line industry description
- hq_city: headquarters city, else null
- hq_state: headquarters US state abbreviation (2 chars), else null

Raw company name: "{raw_name}"

Respond ONLY with a valid JSON object, no explanation.
"""

    payload = {
        "model": GROQ_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0,
    }
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    try:
        response = requests.post(GROQ_API_URL, json=payload, headers=headers, timeout=30)
        response.raise_for_status()
        content = response.json()["choices"][0]["message"]["content"]
        json_match = re.search(r"\{.*\}", content, re.DOTALL)
        if json_match:
            return json.loads(json_match.group())
    except Exception as exc:
        logger.error("Company resolution failed for '%s': %s", raw_name, exc)

    return _empty_resolution(raw_name)


def _empty_resolution(raw_name: str) -> dict:
    return {
        "canonical_name": raw_name,
        "ticker": None,
        "domain": None,
        "industry": None,
        "hq_city": None,
        "hq_state": None,
    }


def resolve_batch(names: list[str]) -> list[dict]:
    results = []
    for name in names:
        resolved = resolve_company_name(name)
        resolved["raw_name"] = name
        results.append(resolved)
        logger.info("Resolved: %s -> %s", name, resolved.get("canonical_name"))
    return results


if __name__ == "__main__":
    samples = ["ACCENTURE FEDERAL SERVICES LLC", "Palantir Technologies"]
    for r in resolve_batch(samples):
        print(r)
