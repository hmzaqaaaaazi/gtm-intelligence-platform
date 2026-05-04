import logging
import os

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

HUNTER_API_URL = "https://api.hunter.io/v2"


def _api_key() -> str:
    key = os.environ.get("HUNTER_API_KEY")
    if not key:
        raise EnvironmentError("HUNTER_API_KEY not set.")
    return key


def domain_search(domain: str, limit: int = 10) -> list[dict]:
    """Find email addresses for a given company domain."""
    params = {
        "domain": domain,
        "limit": limit,
        "api_key": _api_key(),
    }
    try:
        response = requests.get(f"{HUNTER_API_URL}/domain-search", params=params, timeout=30)
        response.raise_for_status()
        data = response.json().get("data", {})
        emails = data.get("emails", [])
        return [
            {
                "first_name": e.get("first_name"),
                "last_name": e.get("last_name"),
                "email": e.get("value"),
                "position": e.get("position"),
                "seniority": e.get("seniority"),
                "department": e.get("department"),
                "confidence": e.get("confidence"),
                "domain": domain,
            }
            for e in emails
        ]
    except Exception as exc:
        logger.error("Hunter domain search failed for '%s': %s", domain, exc)
        return []


def email_finder(domain: str, first_name: str, last_name: str) -> dict | None:
    """Find a specific person's email by name and domain."""
    params = {
        "domain": domain,
        "first_name": first_name,
        "last_name": last_name,
        "api_key": _api_key(),
    }
    try:
        response = requests.get(f"{HUNTER_API_URL}/email-finder", params=params, timeout=30)
        response.raise_for_status()
        data = response.json().get("data", {})
        if not data.get("email"):
            return None
        return {
            "email": data.get("email"),
            "score": data.get("score"),
            "first_name": data.get("first_name"),
            "last_name": data.get("last_name"),
            "position": data.get("position"),
            "domain": domain,
        }
    except Exception as exc:
        logger.error("Hunter email finder failed for %s %s @ %s: %s", first_name, last_name, domain, exc)
        return None


def enrich_companies(companies: list[dict]) -> list[dict]:
    """
    Given a list of company dicts (with a 'domain' field), enrich each
    with contact emails from Hunter.io.
    """
    enriched = []
    for company in companies:
        domain = company.get("domain")
        if not domain:
            logger.warning("Skipping company without domain: %s", company.get("company_name"))
            enriched.append({**company, "contacts": []})
            continue

        contacts = domain_search(domain)
        logger.info("Found %d contacts for %s (%s)", len(contacts), company.get("company_name"), domain)
        enriched.append({**company, "contacts": contacts})

    return enriched


if __name__ == "__main__":
    sample_companies = [{"company_name": "Accenture", "domain": "accenture.com"}]
    results = enrich_companies(sample_companies)
    for r in results:
        print(f"{r['company_name']}: {len(r['contacts'])} contacts found")
