import json
import logging
import os
import re

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

GROQ_API_URL = "https://api.groq.com/openai/v1/chat/completions"
GROQ_MODEL = "llama-3.1-8b-instant"


def interpret_signals(company: dict) -> dict:
    """
    Given a company signal dict (from mart_intent_scores), use LLM to produce
    a human-readable GTM recommendation.
    """
    api_key = os.environ.get("GROQ_API_KEY")
    if not api_key:
        logger.error("GROQ_API_KEY not set.")
        return _empty_interpretation(company)

    prompt = f"""
You are a GTM intelligence analyst. Given the following company signals, produce a concise sales recommendation.

Company signals:
{json.dumps(company, indent=2, default=str)}

Return a JSON object with:
- summary: 2-3 sentence plain English summary of why this company is a good prospect
- recommended_action: one of ["Immediate Outreach", "Nurture", "Monitor", "Deprioritize"]
- talking_points: list of 3 specific talking points for a sales rep
- urgency: one of ["High", "Medium", "Low"]

Respond ONLY with valid JSON.
"""

    payload = {
        "model": GROQ_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.3,
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
        logger.error("Signal interpretation failed for '%s': %s", company.get("company_name"), exc)

    return _empty_interpretation(company)


def _empty_interpretation(company: dict) -> dict:
    return {
        "summary": "Insufficient data to generate interpretation.",
        "recommended_action": "Monitor",
        "talking_points": [],
        "urgency": "Low",
    }


def interpret_batch(companies: list[dict]) -> list[dict]:
    results = []
    for company in companies:
        interpretation = interpret_signals(company)
        interpretation["company_name"] = company.get("company_name")
        results.append(interpretation)
    return results


if __name__ == "__main__":
    sample = {
        "company_name": "Accenture Federal Services",
        "ticker": "ACN",
        "federal_award_amount": 5000000,
        "file_date": "2024-03-15",
        "total_intent_score": 85,
        "intent_tier": "High",
    }
    print(json.dumps(interpret_signals(sample), indent=2))
