#!/usr/bin/env python3
"""Pull remote jobs from a few free boards, clean them up, stash in SQLite, export CSV for Tableau."""

from __future__ import annotations

import json
import logging
import os
import re
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import pandas as pd
import requests
from dateutil import parser as date_parser
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

ROOT = Path(__file__).resolve().parent
SCHEMA_PATH = ROOT / "schema.sql"
DEFAULT_DB = ROOT / "remote_jobs.db"
CSV_PATH = ROOT / "data" / "remote_jobs_tableau.csv"
JUNIOR_CSV_PATH = ROOT / "data" / "remote_jobs_junior_uk.csv"
LOG_DIR = ROOT / "logs"
LOG_FILE = LOG_DIR / "pipeline.log"

JOBICY_URL = "https://jobicy.com/api/v2/remote-jobs"
REMOTIVE_URL = "https://remotive.com/api/remote-jobs"
REMOTEOK_URL = "https://remoteok.com/api"
ARBEITNOW_URL = "https://www.arbeitnow.com/api/job-board-api"
SIMPLYHIRED_UK = "https://www.simplyhired.co.uk/search"
ADZUNA_URL = "https://api.adzuna.com/v1/api/jobs/gb/search/1"

FETCH_COUNT = 100
TIMEOUT = 30
ARBEITNOW_PAGES = 3  # 250 each — enough without hammering their free API

# Rough FX → GBP for the salary ceiling check
FX_TO_GBP = {
    "GBP": 1.0,
    "£": 1.0,
    "USD": 0.78,
    "$": 0.78,
    "EUR": 0.86,
    "€": 0.86,
    "CAD": 0.57,
    "C$": 0.57,
}

SALARY_CEILING_GBP = 35000.0

# Canonical name → pattern. Power BI gets a few spellings because people write it every which way.
SKILL_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    ("SQL", re.compile(r"\bSQL\b", re.IGNORECASE)),
    ("Python", re.compile(r"\bPython\b", re.IGNORECASE)),
    ("Power BI", re.compile(r"\bPower[\s\-]?BI\b|\bPowerBI\b", re.IGNORECASE)),
    ("Tableau", re.compile(r"\bTableau\b", re.IGNORECASE)),
    ("Excel", re.compile(r"\bExcel\b|\bMicrosoft\s+Excel\b", re.IGNORECASE)),
    ("Snowflake", re.compile(r"\bSnowflake\b", re.IGNORECASE)),
    ("dbt", re.compile(r"\bdbt\b", re.IGNORECASE)),
]

DATA_ROLE_RE = re.compile(
    r"\b("
    r"data\s+analyst|data\s+scientist|data\s+engineer|"
    r"analytics|business\s+intelligence|\bBI\b|"
    r"machine\s+learning|ML\s+engineer|"
    r"data\s+analytics|insights\s+analyst|"
    r"reporting\s+analyst|BI\s+developer|"
    r"SQL\s+developer|analytics\s+engineer"
    r")\b",
    re.IGNORECASE,
)

# Geos that usually work from the UK / Europe / Anywhere.
UK_FRIENDLY_RE = re.compile(
    r"anywhere|worldwide|global|united\s*kingdom|\buk\b|britain|"
    r"england|scotland|wales|europe|\beu\b|emea|remote",
    re.IGNORECASE,
)

UK_STRICT_RE = re.compile(
    r"united\s*kingdom|\buk\b|britain|england|scotland|wales|london|"
    r"anywhere|worldwide|global|europe|\beu\b|emea|remote",
    re.IGNORECASE,
)

JUNIOR_RE = re.compile(
    r"\b(junior|entry[\s\-]?level|entry|graduate|grad|intern|internship|"
    r"trainee|apprentice|early[\s\-]?career)\b"
    r"|\bassociate\b(?!\s+director)",
    re.IGNORECASE,
)

SENIOR_RE = re.compile(
    r"\b(senior|sr\.?|lead|director|manager|staff|principal|head\s+of|"
    r"vp|vice\s+president|chief)\b",
    re.IGNORECASE,
)

# Remotive / free-text salary strings: "$170k - $200k", "£30,000 – £35,000", "OTE $25k"
SALARY_NUM_RE = re.compile(
    r"(?P<cur>£|\$|€|USD|EUR|GBP|CAD)?\s*"
    r"(?P<num>\d+(?:[.,]\d+)?)\s*(?P<suffix>[kKmM])?",
    re.IGNORECASE,
)


def is_uk_friendly_location(location: str) -> bool:
    loc = (location or "").strip()
    if not loc:
        return False
    return bool(UK_FRIENDLY_RE.search(loc))


def is_uk_strict_location(location: str) -> bool:
    loc = (location or "").strip()
    if not loc:
        return False
    return bool(UK_STRICT_RE.search(loc))


def setup_logging() -> logging.Logger:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log = logging.getLogger("pipeline")
    log.setLevel(logging.INFO)
    log.handlers.clear()
    fmt = logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s", "%Y-%m-%d %H:%M:%S")
    for handler in (logging.FileHandler(LOG_FILE, encoding="utf-8"), logging.StreamHandler(sys.stdout)):
        handler.setFormatter(fmt)
        log.addHandler(handler)
    return log


log = setup_logging()


def make_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=1.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({
        "User-Agent": "remote-job-market-pipeline/1.0 (github.com/toonice)",
        "Accept": "application/json",
    })
    return session


def join_list(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        return ", ".join(str(v) for v in value if v)
    return str(value)


def normalize_date(pub_date: Any) -> str:
    if pub_date is None or (isinstance(pub_date, float) and pd.isna(pub_date)):
        return ""
    # epoch seconds (Arbeitnow)
    if isinstance(pub_date, (int, float)) and pub_date > 1_000_000_000:
        try:
            return datetime.fromtimestamp(int(pub_date), tz=timezone.utc).strftime("%Y-%m-%d")
        except (ValueError, OSError, OverflowError):
            pass
    text = str(pub_date).strip()
    if not text:
        return ""
    if text.isdigit() and len(text) >= 10:
        try:
            return datetime.fromtimestamp(int(text), tz=timezone.utc).strftime("%Y-%m-%d")
        except (ValueError, OSError, OverflowError):
            pass
    try:
        return date_parser.parse(text).strftime("%Y-%m-%d")
    except (ValueError, TypeError, OverflowError):
        if len(text) >= 10 and text[4] == "-" and text[7] == "-":
            return text[:10]
        log.warning("couldn't parse pubDate=%r", pub_date)
        return ""


def extract_skills(text: str) -> str:
    if not text:
        return ""
    found = [name for name, pat in SKILL_PATTERNS if pat.search(text)]
    return ", ".join(found)


def flag_data_role(title: str, description: str, industry: str, skills: str) -> int:
    if skills.strip():
        return 1
    if DATA_ROLE_RE.search(f"{title} {description} {industry}"):
        return 1
    return 0


def _parse_salary_token(num: str, suffix: str | None) -> Optional[float]:
    try:
        cleaned = num.strip()
        # European-ish "31,2" → 31.2 when there's a single decimal comma
        if cleaned.count(".") == 0 and cleaned.count(",") == 1:
            cleaned = cleaned.replace(",", ".")
        else:
            cleaned = cleaned.replace(",", "")
        val = float(cleaned)
    except ValueError:
        return None
    if suffix:
        s = suffix.lower()
        if s == "k":
            val *= 1000
        elif s == "m":
            val *= 1_000_000
    return val


def parse_salary_text(text: Any) -> tuple[Optional[float], Optional[float], Optional[str]]:
    """Best-effort parse of free-text salaries. Skips obvious hourly figures."""
    if text is None:
        return None, None, None
    raw = str(text).strip()
    if not raw:
        return None, None, None
    lower = raw.lower()
    if "/hour" in lower or "/hr" in lower or "per hour" in lower or "p/h" in lower:
        return None, None, None

    currency = None
    for token, code in (("£", "GBP"), ("€", "EUR"), ("$", "USD"), ("gbp", "GBP"), ("usd", "USD"), ("eur", "EUR"), ("cad", "CAD")):
        if token in lower or token in raw:
            currency = code
            break

    amounts: list[float] = []
    for m in SALARY_NUM_RE.finditer(raw):
        # skip tiny numbers that are probably years / percentages when no k/m
        num = m.group("num")
        suffix = m.group("suffix")
        val = _parse_salary_token(num, suffix)
        if val is None:
            continue
        if suffix is None and val < 1000:
            continue
        amounts.append(val)
        cur = m.group("cur")
        if cur and not currency:
            currency = FX_TO_GBP_KEY(cur)

    if not amounts:
        return None, None, currency
    return min(amounts), max(amounts), currency


def FX_TO_GBP_KEY(cur: str) -> str:
    c = cur.strip().upper()
    if c in ("$", "USD"):
        return "USD"
    if c in ("£", "GBP"):
        return "GBP"
    if c in ("€", "EUR"):
        return "EUR"
    if c in ("C$", "CAD"):
        return "CAD"
    return c


def to_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def salary_midpoint_gbp(
    salary_min: Optional[float],
    salary_max: Optional[float],
    currency: Optional[str],
) -> Optional[float]:
    if salary_min is None and salary_max is None:
        return None
    if salary_min is not None and salary_max is not None:
        mid = (salary_min + salary_max) / 2.0
    else:
        mid = salary_min if salary_min is not None else salary_max
    assert mid is not None

    cur = (currency or "").strip().upper()
    if not cur:
        # RemoteOK etc. often omit currency — treat as USD for the ceiling check
        rate = FX_TO_GBP["USD"]
    elif cur in FX_TO_GBP:
        rate = FX_TO_GBP[cur]
    elif cur in ("£", "$", "€"):
        rate = FX_TO_GBP[cur]
    else:
        # unknown currency (PLN, INR…) — don't invent a rate; leave unknown
        return None
    return mid * rate


def passes_salary_ceiling(
    salary_min: Optional[float],
    salary_max: Optional[float],
    currency: Optional[str],
) -> bool:
    """Keep if salary missing/unknown, or GBP midpoint <= ceiling."""
    mid = salary_midpoint_gbp(salary_min, salary_max, currency)
    if mid is None:
        # missing salary OR unknown FX — keep
        if salary_min is None and salary_max is None:
            return True
        return True
    return mid <= SALARY_CEILING_GBP


def is_juniorish(title: str, job_level: str) -> bool:
    level = (job_level or "").strip().lower()
    if level in ("entry", "junior", "internship", "intern", "graduate", "associate", "trainee"):
        return True
    if "entry" in level or "junior" in level:
        return True
    text = f"{title} {job_level}"
    if JUNIOR_RE.search(text):
        return True
    # Prefer non-senior titles when level isn't explicitly senior
    if SENIOR_RE.search(title or ""):
        return False
    if level in ("senior", "lead", "manager", "director", "staff", "principal"):
        return False
    return True


def is_strict_junior(title: str, job_level: str) -> bool:
    """Stricter — used for the applications CSV."""
    level = (job_level or "").strip().lower()
    title = title or ""
    # Director / manager / lead etc. are not junior even if "associate" appears
    if SENIOR_RE.search(title) and not re.search(
        r"\b(junior|entry|graduate|grad|intern|internship|trainee|apprentice)\b",
        title,
        re.IGNORECASE,
    ):
        return False
    if level in ("entry", "junior", "internship", "intern", "graduate", "associate", "trainee"):
        return True
    if "entry" in level or "junior" in level:
        return True
    return bool(JUNIOR_RE.search(f"{title} {job_level}"))


def make_row(
    *,
    source: str,
    raw_id: Any,
    title: str,
    company: str,
    location: str,
    description: str,
    industry: str,
    job_type: str,
    job_level: str,
    url: str,
    date_posted: str,
    salary_min: Optional[float],
    salary_max: Optional[float],
    salary_currency: Optional[str],
    ingested: str,
) -> Optional[dict[str, Any]]:
    rid = str(raw_id or "").strip()
    if not rid:
        return None
    job_id = f"{source}_{rid}"
    title = (title or "").strip()
    company = (company or "").strip() or "Unknown"
    location = (location or "").strip()
    description = description or ""
    industry = industry or ""
    skills = extract_skills(f"{title}\n{description}\n{industry}")
    is_data = flag_data_role(title, description, industry, skills)
    return {
        "job_id": job_id,
        "source": source,
        "title": title,
        "company_name": company,
        "location": location,
        "date_posted": date_posted,
        "description": description,
        "extracted_skills": skills,
        "url": (url or "").strip(),
        "industry": industry,
        "job_type": job_type or "",
        "job_level": (job_level or "").strip(),
        "salary_min": salary_min,
        "salary_max": salary_max,
        "salary_currency": (salary_currency or "").strip() or None,
        "is_data_role": is_data,
        "ingested_at": ingested,
    }


# ---------------------------------------------------------------------------
# Extractors
# ---------------------------------------------------------------------------

def parse_salary_blob(blob: str) -> tuple[Optional[float], Optional[float], Optional[str]]:
    """Parse strings like '£32,000 a year' or '£25,000 - £30,000 per annum'."""
    if not blob:
        return None, None, None
    text = blob.replace(",", "").strip()
    currency = None
    if "£" in blob or re.search(r"GBP", blob, re.I):
        currency = "GBP"
    elif "$" in blob or re.search(r"USD", blob, re.I):
        currency = "USD"
    elif "€" in blob or re.search(r"EUR", blob, re.I):
        currency = "EUR"

    nums = [float(x) for x in re.findall(r"(\d+(?:\.\d+)?)", text)]
    # drop tiny numbers that are clearly not salaries (ratings etc.)
    nums = [n for n in nums if n >= 1000]
    if not nums:
        return None, None, currency
    # monthly → annual if labelled monthly and value looks monthly
    if re.search(r"month", blob, re.I) and nums and max(nums) < 20000:
        nums = [n * 12 for n in nums]
    if len(nums) == 1:
        return nums[0], nums[0], currency
    return min(nums), max(nums), currency



def fetch_jobicy(session: requests.Session) -> list[dict[str, Any]]:
    by_id: dict[str, dict[str, Any]] = {}

    def _one(tag: Optional[str] = None) -> None:
        params: dict[str, Any] = {"count": FETCH_COUNT}
        if tag:
            params["tag"] = tag
        log.info("GET %s %s", JOBICY_URL, params)
        resp = session.get(JOBICY_URL, params=params, timeout=TIMEOUT)
        if resp.status_code != 200:
            log.error("non-200 from Jobicy: %s %s", resp.status_code, resp.text[:500])
            resp.raise_for_status()
        jobs = resp.json().get("jobs") or []
        log.info("Jobicy%s → %s jobs", f" tag={tag}" if tag else "", len(jobs))
        for job in jobs:
            jid = str(job.get("id", ""))
            if jid:
                by_id[jid] = job

    _one()
    try:
        _one(tag="data")
    except Exception as exc:
        log.warning("Jobicy tag=data failed, keeping broad set: %s", exc)

    ingested = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    rows: list[dict[str, Any]] = []
    for job in by_id.values():
        try:
            smin = float(job["salaryMin"]) if job.get("salaryMin") is not None else None
        except (TypeError, ValueError):
            smin = None
        try:
            smax = float(job["salaryMax"]) if job.get("salaryMax") is not None else None
        except (TypeError, ValueError):
            smax = None
        row = make_row(
            source="jobicy",
            raw_id=job.get("id"),
            title=str(job.get("jobTitle") or ""),
            company=str(job.get("companyName") or ""),
            location=str(job.get("jobGeo") or ""),
            description=str(job.get("jobDescription") or job.get("jobExcerpt") or ""),
            industry=join_list(job.get("jobIndustry")),
            job_type=join_list(job.get("jobType")),
            job_level=str(job.get("jobLevel") or ""),
            url=str(job.get("url") or ""),
            date_posted=normalize_date(job.get("pubDate")),
            salary_min=smin,
            salary_max=smax,
            salary_currency=str(job.get("salaryCurrency") or "") or None,
            ingested=ingested,
        )
        if row:
            rows.append(row)
    return rows


def fetch_remotive(session: requests.Session) -> list[dict[str, Any]]:
    by_id: dict[str, dict[str, Any]] = {}
    queries = [
        {"category": "data"},
        {"search": "junior data analyst"},
    ]
    for params in queries:
        log.info("GET %s %s", REMOTIVE_URL, params)
        try:
            resp = session.get(REMOTIVE_URL, params=params, timeout=TIMEOUT)
        except requests.RequestException as exc:
            log.warning("Remotive request failed: %s", exc)
            continue
        if resp.status_code != 200:
            log.warning("Remotive non-200: %s %s", resp.status_code, resp.text[:300])
            continue
        jobs = resp.json().get("jobs") or []
        log.info("Remotive %s → %s jobs", params, len(jobs))
        for job in jobs:
            jid = str(job.get("id", ""))
            if jid:
                by_id[jid] = job

    ingested = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    rows: list[dict[str, Any]] = []
    for job in by_id.values():
        smin, smax, scur = parse_salary_text(job.get("salary"))
        row = make_row(
            source="remotive",
            raw_id=job.get("id"),
            title=str(job.get("title") or ""),
            company=str(job.get("company_name") or ""),
            location=str(job.get("candidate_required_location") or ""),
            description=str(job.get("description") or ""),
            industry=str(job.get("category") or ""),
            job_type=str(job.get("job_type") or ""),
            job_level="",
            url=str(job.get("url") or ""),
            date_posted=normalize_date(job.get("publication_date")),
            salary_min=smin,
            salary_max=smax,
            salary_currency=scur,
            ingested=ingested,
        )
        if row:
            rows.append(row)
    return rows


def fetch_remoteok(session: requests.Session) -> list[dict[str, Any]]:
    log.info("GET %s", REMOTEOK_URL)
    try:
        resp = session.get(REMOTEOK_URL, timeout=TIMEOUT)
    except requests.RequestException as exc:
        log.warning("RemoteOK request failed: %s", exc)
        return []
    if resp.status_code != 200:
        log.warning("RemoteOK non-200: %s %s", resp.status_code, resp.text[:300])
        return []

    payload = resp.json()
    if not isinstance(payload, list) or len(payload) < 2:
        log.warning("RemoteOK unexpected payload")
        return []

    # First element is metadata — skip it
    jobs = [j for j in payload[1:] if isinstance(j, dict) and j.get("id")]
    log.info("RemoteOK → %s jobs", len(jobs))

    ingested = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    rows: list[dict[str, Any]] = []
    for job in jobs:
        tags = join_list(job.get("tags"))
        smin = to_float(job.get("salary_min"))
        smax = to_float(job.get("salary_max"))
        # RemoteOK rarely sets currency; assume USD when a number is present
        scur = str(job.get("salary_currency") or "").strip() or ("USD" if smin or smax else None)
        row = make_row(
            source="remoteok",
            raw_id=job.get("id"),
            title=str(job.get("position") or job.get("title") or ""),
            company=str(job.get("company") or ""),
            location=str(job.get("location") or "") or "Remote",
            description=str(job.get("description") or ""),
            industry=tags,
            job_type="",
            job_level="",
            url=str(job.get("url") or job.get("apply_url") or ""),
            date_posted=normalize_date(job.get("date") or job.get("epoch")),
            salary_min=smin,
            salary_max=smax,
            salary_currency=scur,
            ingested=ingested,
        )
        if row:
            rows.append(row)
    return rows


def fetch_arbeitnow(session: requests.Session) -> list[dict[str, Any]]:
    by_slug: dict[str, dict[str, Any]] = {}
    for page in range(1, ARBEITNOW_PAGES + 1):
        params = {"page": page}
        log.info("GET %s %s", ARBEITNOW_URL, params)
        try:
            resp = session.get(ARBEITNOW_URL, params=params, timeout=TIMEOUT)
        except requests.RequestException as exc:
            log.warning("Arbeitnow page %s failed: %s", page, exc)
            break
        if resp.status_code != 200:
            log.warning("Arbeitnow non-200: %s %s", resp.status_code, resp.text[:300])
            break
        payload = resp.json()
        jobs = payload.get("data") or []
        log.info("Arbeitnow page %s → %s jobs", page, len(jobs))
        if not jobs:
            break
        for job in jobs:
            slug = str(job.get("slug") or "")
            if slug:
                by_slug[slug] = job
        # stop early if no next page
        links = payload.get("links") or {}
        if not links.get("next"):
            break

    ingested = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    rows: list[dict[str, Any]] = []
    for job in by_slug.values():
        loc = str(job.get("location") or "").strip()
        if job.get("remote"):
            loc = f"{loc}, Remote".strip(", ") if loc else "Remote"
        tags = join_list(job.get("tags"))
        job_types = join_list(job.get("job_types"))
        row = make_row(
            source="arbeitnow",
            raw_id=job.get("slug"),
            title=str(job.get("title") or ""),
            company=str(job.get("company_name") or ""),
            location=loc,
            description=str(job.get("description") or ""),
            industry=tags,
            job_type=job_types,
            job_level="",
            url=str(job.get("url") or ""),
            date_posted=normalize_date(job.get("created_at")),
            salary_min=None,
            salary_max=None,
            salary_currency=None,
            ingested=ingested,
        )
        if row:
            rows.append(row)
    return rows


def fetch_adzuna(session: requests.Session) -> list[dict[str, Any]]:
    app_id = (os.environ.get("ADZUNA_APP_ID") or "").strip()
    app_key = (os.environ.get("ADZUNA_APP_KEY") or "").strip()
    if not app_id or not app_key:
        log.info("ADZUNA_APP_ID / ADZUNA_APP_KEY not set — skipping Adzuna")
        return []

    params = {
        "app_id": app_id,
        "app_key": app_key,
        "what": "junior data analyst",
        "where": "uk",
        "results_per_page": 50,
        "content-type": "application/json",
    }
    log.info("GET %s what=%s where=%s", ADZUNA_URL, params["what"], params["where"])
    try:
        resp = session.get(ADZUNA_URL, params=params, timeout=TIMEOUT)
    except requests.RequestException as exc:
        log.warning("Adzuna request failed: %s", exc)
        return []
    if resp.status_code != 200:
        log.warning("Adzuna non-200: %s %s", resp.status_code, resp.text[:300])
        return []

    results = resp.json().get("results") or []
    log.info("Adzuna → %s jobs", len(results))
    ingested = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    rows: list[dict[str, Any]] = []
    for job in results:
        loc_obj = job.get("location") or {}
        loc_parts = loc_obj.get("display_name") or loc_obj.get("area") or ""
        if isinstance(loc_parts, list):
            location = ", ".join(str(p) for p in loc_parts)
        else:
            location = str(loc_parts or "UK")
        # Adzuna UK salaries are GBP
        smin = to_float(job.get("salary_min"))
        smax = to_float(job.get("salary_max"))
        cat = job.get("category") or {}
        industry = str(cat.get("label") or cat.get("tag") or "")
        row = make_row(
            source="adzuna",
            raw_id=job.get("id"),
            title=str(job.get("title") or ""),
            company=str((job.get("company") or {}).get("display_name") or ""),
            location=location,
            description=str(job.get("description") or ""),
            industry=industry,
            job_type=str(job.get("contract_time") or job.get("contract_type") or ""),
            job_level="",
            url=str(job.get("redirect_url") or job.get("adref") or ""),
            date_posted=normalize_date(job.get("created")),
            salary_min=smin,
            salary_max=smax,
            salary_currency="GBP" if (smin or smax) else None,
            ingested=ingested,
        )
        if row:
            rows.append(row)
    return rows



def fetch_simplyhired_uk(session: requests.Session) -> list[dict[str, Any]]:
    """UK graduate / junior DA board — this is how roles like Escentral show up."""
    queries = [
        "graduate data analyst",
        "junior data analyst",
        "entry level data analyst",
        "graduate analyst data",
    ]
    ingested = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    by_key: dict[str, dict[str, Any]] = {}

    for q in queries:
        params = {"q": q, "l": "United Kingdom"}
        log.info("GET %s %s", SIMPLYHIRED_UK, params)
        try:
            resp = session.get(SIMPLYHIRED_UK, params=params, timeout=TIMEOUT)
        except requests.RequestException as exc:
            log.warning("SimplyHired request failed (%s): %s", q, exc)
            continue
        if resp.status_code != 200:
            log.warning("SimplyHired non-200 for %r: %s", q, resp.status_code)
            continue
        m = re.search(
            r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>',
            resp.text,
        )
        if not m:
            log.warning("SimplyHired: no __NEXT_DATA__ for %r", q)
            continue
        try:
            payload = json.loads(m.group(1))
        except json.JSONDecodeError as exc:
            log.warning("SimplyHired JSON parse failed for %r: %s", q, exc)
            continue
        jobs = (
            payload.get("props", {})
            .get("pageProps", {})
            .get("jobs")
            or []
        )
        log.info("SimplyHired %r → %s cards", q, len(jobs))
        for job in jobs:
            key = str(job.get("jobKey") or "").strip()
            if not key:
                continue
            title = (job.get("title") or "").strip()
            company = (job.get("company") or "").strip()
            location = (job.get("location") or "").strip()
            snippet = job.get("snippet") or ""
            uncategorized = job.get("uncategorized") or []
            remote_attrs = job.get("remoteAttributes") or []
            tags = " ".join([*(job.get("requirements") or []), *uncategorized, *remote_attrs])
            description = (snippet + "\n" + tags).strip()
            # Graduate / Junior from chips
            level = ""
            chips = " ".join(uncategorized).lower()
            if "graduate" in chips or "graduate" in title.lower():
                level = "Graduate"
            elif "junior" in chips or "junior" in title.lower() or "entry" in chips:
                level = "Entry-Level, Junior"
            bot = job.get("botUrl") or f"/job/{key}"
            url = "https://www.simplyhired.co.uk" + bot
            smin, smax, scur = parse_salary_blob(job.get("salaryInfo") or "")
            # Prefer Remote in location when remoteAttributes / Remote label present
            if not location and remote_attrs:
                location = ", ".join(remote_attrs)
            row = make_row(
                source="simplyhired",
                raw_id=key,
                title=title,
                company=company,
                location=location or "United Kingdom",
                description=description,
                industry="Data / Analytics",
                job_type=", ".join(job.get("jobTypes") or []) or "Full-time",
                job_level=level,
                url=url,
                date_posted=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                salary_min=smin,
                salary_max=smax,
                salary_currency=scur,
                ingested=ingested,
            )
            if row:
                by_key[key] = row

    rows = list(by_key.values())
    log.info("SimplyHired unique → %s", len(rows))
    return rows


def extract_all() -> tuple[list[dict[str, Any]], dict[str, int]]:
    session = make_session()
    counts: dict[str, int] = {}
    all_rows: list[dict[str, Any]] = []

    # Jobicy is primary — fail the run if it dies hard
    try:
        jobicy_rows = fetch_jobicy(session)
    except Exception:
        log.exception("Jobicy extract failed")
        raise
    counts["jobicy"] = len(jobicy_rows)
    all_rows.extend(jobicy_rows)

    for name, fn in (
        ("remotive", fetch_remotive),
        ("remoteok", fetch_remoteok),
        ("arbeitnow", fetch_arbeitnow),
        ("adzuna", fetch_adzuna),
        ("simplyhired", fetch_simplyhired_uk),
    ):
        try:
            rows = fn(session)
        except Exception as exc:
            log.warning("%s extract failed, continuing: %s", name, exc)
            rows = []
        counts[name] = len(rows)
        all_rows.extend(rows)

    # de-dupe by prefixed job_id (last write wins)
    by_id: dict[str, dict[str, Any]] = {}
    for row in all_rows:
        by_id[row["job_id"]] = row

    log.info(
        "unique jobs after merge: %s (per source: %s)",
        len(by_id),
        ", ".join(f"{k}={v}" for k, v in counts.items()),
    )
    return list(by_id.values()), counts


def transform(rows: list[dict[str, Any]]) -> pd.DataFrame:
    if not rows:
        log.warning("transform produced nothing")
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df = df.drop_duplicates(subset=["job_id"], keep="last")
    log.info("transformed %s jobs (%s data-ish)", len(df), int(df["is_data_role"].sum()))
    return df


def db_path() -> Path:
    return Path(os.environ.get("DB_PATH", str(DEFAULT_DB))).expanduser().resolve()


def apply_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
    conn.commit()


def load_to_sqlite(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    try:
        apply_schema(conn)
        # Fresh snapshot each run so old unprefixed Jobicy ids don't linger
        conn.execute("DELETE FROM job_postings")
        conn.commit()

        if df.empty:
            return

        n = 0
        for _, row in df.iterrows():
            conn.execute(
                "INSERT OR IGNORE INTO companies (company_name) VALUES (?)",
                (row["company_name"],),
            )
            company_id = conn.execute(
                "SELECT company_id FROM companies WHERE company_name = ?",
                (row["company_name"],),
            ).fetchone()[0]

            conn.execute(
                """
                INSERT OR REPLACE INTO job_postings (
                    job_id, title, company_id, location, date_posted,
                    description, extracted_skills, url, industry, job_type,
                    job_level, salary_min, salary_max, salary_currency,
                    is_data_role, ingested_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row["job_id"],
                    row["title"],
                    company_id,
                    row["location"],
                    row["date_posted"],
                    row["description"],
                    row["extracted_skills"],
                    row["url"],
                    row["industry"],
                    row["job_type"],
                    row["job_level"],
                    row["salary_min"] if pd.notna(row["salary_min"]) else None,
                    row["salary_max"] if pd.notna(row["salary_max"]) else None,
                    row["salary_currency"],
                    int(row["is_data_role"]),
                    row["ingested_at"],
                ),
            )
            n += 1

        conn.commit()
        log.info("wrote %s jobs to %s", n, path)
    finally:
        conn.close()


def _read_export_frame(path: Path) -> pd.DataFrame:
    conn = sqlite3.connect(str(path))
    try:
        return pd.read_sql_query(
            """
            SELECT
                j.job_id, j.title, c.company_name, j.location, j.date_posted,
                j.industry, j.job_type, j.job_level, j.extracted_skills,
                j.salary_min, j.salary_max, j.salary_currency, j.is_data_role,
                j.url, j.ingested_at,
                CASE
                    WHEN j.salary_min IS NOT NULL AND j.salary_max IS NOT NULL
                        THEN (j.salary_min + j.salary_max) / 2.0
                    WHEN j.salary_min IS NOT NULL THEN j.salary_min
                    WHEN j.salary_max IS NOT NULL THEN j.salary_max
                    ELSE NULL
                END AS salary_midpoint
            FROM job_postings j
            JOIN companies c ON c.company_id = j.company_id
            ORDER BY j.date_posted DESC, j.job_id
            """,
            conn,
        )
    finally:
        conn.close()


def _row_salary_ok(row: pd.Series) -> bool:
    smin = row["salary_min"] if pd.notna(row["salary_min"]) else None
    smax = row["salary_max"] if pd.notna(row["salary_max"]) else None
    scur = row["salary_currency"] if pd.notna(row.get("salary_currency")) else None
    return passes_salary_ceiling(
        float(smin) if smin is not None else None,
        float(smax) if smax is not None else None,
        str(scur) if scur else None,
    )


def export_csv(path: Path, csv_path: Path = CSV_PATH) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Tableau CSV: UK-friendly + junior-ish + salary ceiling. Also write strict junior UK file."""
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    full_path = csv_path.parent / "remote_jobs_tableau_all.csv"
    junior_path = JUNIOR_CSV_PATH

    df = _read_export_frame(path)
    df.to_csv(full_path, index=False)

    if df.empty:
        empty = df.copy()
        empty.to_csv(csv_path, index=False)
        empty.to_csv(junior_path, index=False)
        log.info("csv → empty exports")
        return empty, empty

    uk_mask = df["location"].fillna("").map(is_uk_friendly_location)
    # location mentions UK / London remote (already partly covered, keep explicit)
    uk_london = df["location"].fillna("").str.contains(
        r"\bUK\b|United\s*Kingdom|London", case=False, regex=True, na=False
    )
    geo_ok = uk_mask | uk_london

    junior_mask = df.apply(
        lambda r: is_juniorish(str(r.get("title") or ""), str(r.get("job_level") or "")),
        axis=1,
    )
    salary_ok = df.apply(_row_salary_ok, axis=1)

    tableau = df[geo_ok & junior_mask & salary_ok].copy()
    tableau.to_csv(csv_path, index=False)

    strict_junior = df.apply(
        lambda r: is_strict_junior(str(r.get("title") or ""), str(r.get("job_level") or "")),
        axis=1,
    )
    strict_uk = df["location"].fillna("").map(is_uk_strict_location) | uk_london
    junior_uk = df[strict_uk & strict_junior & salary_ok].copy()
    junior_uk.to_csv(junior_path, index=False)

    log.info(
        "csv → %s (%s rows); junior UK → %s (%s rows); full dump %s rows at %s",
        csv_path, len(tableau), junior_path, len(junior_uk), len(df), full_path.name,
    )
    return tableau, junior_uk


def maybe_publish_tableau(csv_path: Path) -> None:
    # Tableau Public doesn't speak TSC — if there's no server URL, just leave the CSV.
    server_url = (os.environ.get("TABLEAU_SERVER_URL") or "").strip()
    token_name = (os.environ.get("TABLEAU_TOKEN_NAME") or "").strip()
    token_value = (os.environ.get("TABLEAU_TOKEN_VALUE") or "").strip()
    project_name = (os.environ.get("TABLEAU_PROJECT_NAME") or "").strip()
    datasource_name = (os.environ.get("TABLEAU_DATASOURCE_NAME") or "Remote Job Market").strip()
    site_id = os.environ.get("TABLEAU_SITE_ID")
    site_id = "" if site_id is None else site_id.strip()

    if not server_url:
        log.info(
            "no TABLEAU_SERVER_URL — skipping publish. "
            "CSV is ready for Tableau Public (GitHub raw URL works fine)."
        )
        return

    if not token_name or not token_value:
        log.warning("server URL set but token missing — skip publish")
        return

    try:
        import tableauserverclient as TSC
    except ImportError:
        log.warning("tableauserverclient not installed — skip publish")
        return

    try:
        auth = TSC.PersonalAccessTokenAuth(token_name, token_value, site_id=site_id)
        server = TSC.Server(server_url, use_server_version=True)
        with server.auth.sign_in(auth):
            project_id = None
            for project in TSC.Pager(server.projects):
                if not project_name or project.name == project_name:
                    project_id = project.id
                    if project_name:
                        break
            if project_id is None:
                log.error("Tableau project %r not found", project_name)
                return
            item = TSC.DatasourceItem(project_id, name=datasource_name)
            published = server.datasources.publish(
                item, str(csv_path), TSC.Server.PublishMode.Overwrite
            )
            log.info("published datasource %s (%s)", published.name, published.id)
    except Exception as exc:
        log.error("Tableau publish failed (csv still ok): %s", exc, exc_info=True)


def skill_freq(df: pd.DataFrame) -> list[tuple[str, int]]:
    counts: dict[str, int] = {}
    if df.empty or "extracted_skills" not in df.columns:
        return []
    for cell in df["extracted_skills"].fillna(""):
        for skill in str(cell).split(","):
            skill = skill.strip()
            if skill:
                counts[skill] = counts.get(skill, 0) + 1
    return sorted(counts.items(), key=lambda x: (-x[1], x[0]))


def run() -> int:
    log.info("pipeline start")
    path = db_path()
    log.info("DB_PATH=%s", path)

    rows, source_counts = extract_all()
    df = transform(rows)
    load_to_sqlite(df, path)
    tableau_df, junior_df = export_csv(path)
    maybe_publish_tableau(CSV_PATH)

    conn = sqlite3.connect(str(path))
    try:
        jobs = conn.execute("SELECT COUNT(*) FROM job_postings").fetchone()[0]
        cos = conn.execute("SELECT COUNT(*) FROM companies").fetchone()[0]
        data = conn.execute(
            "SELECT COUNT(*) FROM job_postings WHERE is_data_role = 1"
        ).fetchone()[0]
    finally:
        conn.close()

    log.info("source counts: %s", source_counts)
    log.info(
        "done — jobs=%s companies=%s data_roles=%s tableau_rows=%s junior_uk_rows=%s top_skills=%s",
        jobs, cos, data, len(tableau_df), len(junior_df), skill_freq(tableau_df)[:10],
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(run())
    except Exception:
        log.exception("pipeline failed")
        raise SystemExit(1)
