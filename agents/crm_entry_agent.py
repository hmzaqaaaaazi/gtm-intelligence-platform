import logging
import os
import time

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

HUBSPOT_BASE_URL = "https://api.hubapi.com"

# ── Industry normalisation ────────────────────────────────────────────────────
# Maps free-text industry strings (from Groq) to HubSpot's enum values.
# Keys are lowercase substrings; first match wins.
_INDUSTRY_KEYWORDS: list[tuple[str, str]] = [
    ("aerospace",                   "AVIATION_AEROSPACE"),
    ("defense",                     "DEFENSE_SPACE"),
    ("biotechnology",               "BIOTECHNOLOGY"),
    ("biotech",                     "BIOTECHNOLOGY"),
    ("pharmaceutical",              "PHARMACEUTICALS"),
    ("pharma",                      "PHARMACEUTICALS"),
    ("medical device",              "MEDICAL_DEVICES"),
    ("medical",                     "HOSPITAL_HEALTH_CARE"),
    ("health",                      "HOSPITAL_HEALTH_CARE"),
    ("information technology",      "INFORMATION_TECHNOLOGY_AND_SERVICES"),
    ("software",                    "COMPUTER_SOFTWARE"),
    ("computer hardware",           "COMPUTER_HARDWARE"),
    ("computer network",            "COMPUTER_NETWORKING"),
    ("semiconductor",               "SEMICONDUCTORS"),
    ("internet",                    "INTERNET"),
    ("telecom",                     "TELECOMMUNICATIONS"),
    ("wireless",                    "WIRELESS"),
    ("financial service",           "FINANCIAL_SERVICES"),
    ("investment bank",             "INVESTMENT_BANKING"),
    ("investment manage",           "INVESTMENT_MANAGEMENT"),
    ("insurance",                   "INSURANCE"),
    ("banking",                     "BANKING"),
    ("capital market",              "CAPITAL_MARKETS"),
    ("venture capital",             "VENTURE_CAPITAL_PRIVATE_EQUITY"),
    ("private equity",              "VENTURE_CAPITAL_PRIVATE_EQUITY"),
    ("management consulting",       "MANAGEMENT_CONSULTING"),
    ("consulting",                  "MANAGEMENT_CONSULTING"),
    ("accounting",                  "ACCOUNTING"),
    ("legal",                       "LEGAL_SERVICES"),
    ("law",                         "LAW_PRACTICE"),
    ("real estate",                 "REAL_ESTATE"),
    ("construction",                "CONSTRUCTION"),
    ("civil engineering",           "CIVIL_ENGINEERING"),
    ("mechanical",                  "MECHANICAL_OR_INDUSTRIAL_ENGINEERING"),
    ("industrial automation",       "INDUSTRIAL_AUTOMATION"),
    ("manufacturing",               "ELECTRICAL_ELECTRONIC_MANUFACTURING"),
    ("chemical",                    "CHEMICALS"),
    ("oil",                         "OIL_ENERGY"),
    ("energy",                      "OIL_ENERGY"),
    ("utilities",                   "UTILITIES"),
    ("renewabl",                    "RENEWABLES_ENVIRONMENT"),
    ("environmental",               "ENVIRONMENTAL_SERVICES"),
    ("logistics",                   "LOGISTICS_AND_SUPPLY_CHAIN"),
    ("transport",                   "TRANSPORTATION_TRUCKING_RAILROAD"),
    ("retail",                      "RETAIL"),
    ("consumer goods",              "CONSUMER_GOODS"),
    ("consumer electronics",        "CONSUMER_ELECTRONICS"),
    ("food",                        "FOOD_BEVERAGES"),
    ("hospitality",                 "HOSPITALITY"),
    ("education",                   "EDUCATION_MANAGEMENT"),
    ("research",                    "RESEARCH"),
    ("government",                  "GOVERNMENT_ADMINISTRATION"),
    ("non-profit",                  "NON_PROFIT_ORGANIZATION_MANAGEMENT"),
    ("nonprofit",                   "NON_PROFIT_ORGANIZATION_MANAGEMENT"),
    ("marketing",                   "MARKETING_AND_ADVERTISING"),
    ("media",                       "MEDIA_PRODUCTION"),
    ("entertainment",               "ENTERTAINMENT"),
    ("staffing",                    "STAFFING_AND_RECRUITING"),
    ("human resources",             "HUMAN_RESOURCES"),
    ("security",                    "SECURITY_AND_INVESTIGATIONS"),
    ("insurance",                   "INSURANCE"),
    ("agriculture",                 "FARMING"),
    ("mining",                      "MINING_METALS"),
    ("publishing",                  "PUBLISHING"),
]


def _map_industry(raw: str | None) -> str:
    """Convert a free-text industry string to a valid HubSpot enum value."""
    if not raw:
        return "INFORMATION_TECHNOLOGY_AND_SERVICES"
    lower = raw.lower()
    for keyword, hs_value in _INDUSTRY_KEYWORDS:
        if keyword in lower:
            return hs_value
    return "INFORMATION_TECHNOLOGY_AND_SERVICES"   # safe default


# ── HubSpot helpers ───────────────────────────────────────────────────────────

def _headers() -> dict:
    token = os.environ.get("HUBSPOT_ACCESS_TOKEN")
    if not token:
        raise EnvironmentError("HUBSPOT_ACCESS_TOKEN not set.")
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }


def _api_call_with_retry(method: str, url: str, max_retries: int = 4,
                          **kwargs) -> requests.Response:
    """
    Wrapper that retries on 429 (rate-limit) with exponential back-off.
    Raises the last response on persistent failure.
    """
    backoff = 2.0
    for attempt in range(max_retries):
        response = requests.request(method, url, headers=_headers(),
                                    timeout=30, **kwargs)
        if response.status_code != 429:
            return response
        wait = float(response.headers.get("Retry-After", backoff))
        logger.warning("429 rate-limit — waiting %.1fs (attempt %d/%d)",
                       wait, attempt + 1, max_retries)
        time.sleep(wait)
        backoff *= 2
    return response   # return last response after all retries


def create_or_update_company(company: dict, interpretation: dict) -> tuple[str | None, str]:
    """
    Upsert a HubSpot company.

    Strategy (avoids an extra search call per company):
      1. POST to create.
      2. On 409 CONFLICT (already exists) → extract ID from error body → PATCH.
      3. On 429 → retry with back-off (handled by _api_call_with_retry).
      4. Any other error → log and return ("failed").

    Returns (hubspot_company_id, action) where action is 'created'|'updated'|'failed'.
    """
    company_name = company.get("canonical_name") or company.get("company_name", "")

    properties = {
        "name":           company_name,
        "state":          company.get("hq_state") or company.get("state_code") or "",
        "city":           company.get("hq_city") or "",
        "industry":       _map_industry(company.get("industry")),
        "website":        company.get("domain") or "",
        "description":    interpretation.get("summary") or "",
        "hs_lead_status": _map_action_to_lead_status(
                              interpretation.get("recommended_action")),
    }

    create_url = f"{HUBSPOT_BASE_URL}/crm/v3/objects/companies"

    try:
        response = _api_call_with_retry("POST", create_url,
                                        json={"properties": properties})

        # ── Created successfully ───────────────────────────────────────────
        if response.status_code == 201:
            new_id = response.json()["id"]
            logger.info("Created HubSpot company: %s (ID: %s)", company_name, new_id)
            return new_id, "created"

        # ── Already exists (409) — extract existing ID and PATCH ──────────
        if response.status_code == 409:
            body = response.json()
            # HubSpot embeds the existing ID in context.id or message
            existing_id = None
            ctx = body.get("context", {})
            if ctx.get("id"):
                existing_id = str(ctx["id"][0]) if isinstance(ctx["id"], list) \
                              else str(ctx["id"])
            if not existing_id:
                # Fall back: parse "Existing ID: 12345" from message
                import re
                m = re.search(r"Existing ID[:\s]+(\d+)",
                              body.get("message", ""), re.IGNORECASE)
                if m:
                    existing_id = m.group(1)

            if existing_id:
                patch_url = f"{create_url}/{existing_id}"
                patch_resp = _api_call_with_retry("PATCH", patch_url,
                                                  json={"properties": properties})
                if patch_resp.status_code == 200:
                    logger.info("Updated HubSpot company: %s (ID: %s)",
                                company_name, existing_id)
                    return existing_id, "updated"
                else:
                    logger.error("PATCH failed for '%s' [%s]: %s",
                                 company_name, patch_resp.status_code,
                                 patch_resp.text[:300])
                    return None, "failed"
            else:
                logger.error("409 but no ID extractable for '%s': %s",
                             company_name, body)
                return None, "failed"

        # ── Any other error ────────────────────────────────────────────────
        logger.error("HubSpot upsert failed for '%s' [HTTP %s]: %s",
                     company_name, response.status_code, response.text[:400])
        return None, "failed"

    except Exception as exc:
        logger.error("HubSpot upsert exception for '%s': %s", company_name, exc)
        return None, "failed"


def _map_action_to_lead_status(action: str | None) -> str:
    return {
        "Immediate Outreach": "IN_PROGRESS",
        "Nurture":            "OPEN",
        "Monitor":            "OPEN",
        "Deprioritize":       "UNQUALIFIED",
    }.get(action or "", "OPEN")


def add_note(company_id: str, note_body: str) -> None:
    """Attach a GTM intelligence note to a HubSpot company."""
    url     = f"{HUBSPOT_BASE_URL}/crm/v3/objects/notes"
    payload = {
        "properties": {
            "hs_note_body":  note_body,
            "hs_timestamp":  str(int(__import__("time").time() * 1000)),
        },
        "associations": [{
            "to":    {"id": company_id},
            "types": [{"associationCategory": "HUBSPOT_DEFINED",
                       "associationTypeId": 190}],
        }],
    }
    try:
        response = requests.post(url, json=payload, headers=_headers(), timeout=30)
        response.raise_for_status()
        logger.info("Note added to company ID %s", company_id)
    except Exception as exc:
        logger.error("Failed to add note to company ID %s: %s", company_id, exc)


def process_batch(companies: list[dict], interpretations: list[dict]) -> dict:
    """
    Upsert each company into HubSpot and attach a GTM intelligence note.
    Returns counts: {"created": int, "updated": int, "failed": int}.
    """
    interp_map = {i.get("company_name"): i for i in interpretations}
    counts = {"created": 0, "updated": 0, "failed": 0}

    for company in companies:
        raw_name       = company.get("company_name", "")
        interpretation = interp_map.get(raw_name, {})
        company_id, action = create_or_update_company(company, interpretation)
        counts[action] += 1

        if company_id and interpretation.get("talking_points"):
            bc            = company.get("best_contact") or {}
            contact_line  = ""
            if bc.get("email"):
                contact_line = (
                    f"\nBest Contact: {bc.get('first_name','')} "
                    f"{bc.get('last_name','')} "
                    f"({bc.get('position','')}) — {bc.get('email')}\n"
                )
            talking_points = "\n".join(
                f"- {tp}" for tp in interpretation["talking_points"])
            note = (
                f"GTM Intelligence Update\n"
                f"Intent Score : {company.get('total_intent_score','N/A')} "
                f"({company.get('intent_tier','N/A')})\n"
                f"Urgency      : {interpretation.get('urgency','N/A')}\n"
                f"Action       : {interpretation.get('recommended_action')}\n"
                f"{contact_line}\n"
                f"Talking Points:\n{talking_points}"
            )
            add_note(company_id, note)

    return counts


if __name__ == "__main__":
    print("CRM Entry Agent ready. Call process_batch() with company and interpretation data.")
