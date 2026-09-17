#!/usr/bin/env python3
"""Pull remote jobs from Jobicy, clean them up, stash in SQLite, export CSV for Tableau."""

from __future__ import annotations

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
LOG_DIR = ROOT / "logs"
LOG_FILE = LOG_DIR / "pipeline.log"

JOBICY_URL = "https://jobicy.com/api/v2/remote-jobs"
FETCH_COUNT = 100
TIMEOUT = 30

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


def fetch_jobs(count: int = FETCH_COUNT, tag: Optional[str] = None) -> list[dict[str, Any]]:
    params: dict[str, Any] = {"count": count}
    if tag:
        params["tag"] = tag

    session = make_session()
    log.info("GET %s %s", JOBICY_URL, params)
    try:
        resp = session.get(JOBICY_URL, params=params, timeout=TIMEOUT)
    except requests.RequestException as exc:
        log.error("request failed: %s", exc)
        raise

    if resp.status_code != 200:
        log.error("non-200 from Jobicy: %s %s", resp.status_code, resp.text[:500])
        resp.raise_for_status()

    payload = resp.json()
    jobs = payload.get("jobs") or []
    log.info("got %s jobs (jobCount=%s)", len(jobs), payload.get("jobCount"))
    return jobs


def extract_all() -> list[dict[str, Any]]:
    # Broad pull first, then tag=data so analytics roles aren't under-represented.
    by_id: dict[str, dict[str, Any]] = {}
    for job in fetch_jobs(count=FETCH_COUNT):
        jid = str(job.get("id", ""))
        if jid:
            by_id[jid] = job

    try:
        for job in fetch_jobs(count=FETCH_COUNT, tag="data"):
            jid = str(job.get("id", ""))
            if jid:
                by_id[jid] = job
    except Exception as exc:
        log.warning("tag=data fetch failed, keeping broad set only: %s", exc)

    log.info("unique jobs after merge: %s", len(by_id))
    return list(by_id.values())


def join_list(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        return ", ".join(str(v) for v in value if v)
    return str(value)


def normalize_date(pub_date: Any) -> str:
    if pub_date is None or (isinstance(pub_date, float) and pd.isna(pub_date)):
        return ""
    text = str(pub_date).strip()
    if not text:
        return ""
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


def transform(raw_jobs: list[dict[str, Any]]) -> pd.DataFrame:
    ingested = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    rows: list[dict[str, Any]] = []

    for job in raw_jobs:
        job_id = str(job.get("id", "")).strip()
        if not job_id:
            continue

        title = str(job.get("jobTitle") or "").strip()
        company = str(job.get("companyName") or "").strip() or "Unknown"
        location = str(job.get("jobGeo") or "").strip()
        description = str(job.get("jobDescription") or job.get("jobExcerpt") or "")
        industry = join_list(job.get("jobIndustry"))
        job_type = join_list(job.get("jobType"))
        job_level = str(job.get("jobLevel") or "").strip()
        url = str(job.get("url") or "").strip()
        date_posted = normalize_date(job.get("pubDate"))

        skills = extract_skills(f"{title}\n{description}\n{industry}")
        is_data = flag_data_role(title, description, industry, skills)

        try:
            salary_min = float(job["salaryMin"]) if job.get("salaryMin") is not None else None
        except (TypeError, ValueError):
            salary_min = None
        try:
            salary_max = float(job["salaryMax"]) if job.get("salaryMax") is not None else None
        except (TypeError, ValueError):
            salary_max = None

        rows.append({
            "job_id": job_id,
            "title": title,
            "company_name": company,
            "location": location,
            "date_posted": date_posted,
            "description": description,
            "extracted_skills": skills,
            "url": url,
            "industry": industry,
            "job_type": job_type,
            "job_level": job_level,
            "salary_min": salary_min,
            "salary_max": salary_max,
            "salary_currency": str(job.get("salaryCurrency") or "").strip() or None,
            "is_data_role": is_data,
            "ingested_at": ingested,
        })

    df = pd.DataFrame(rows)
    if df.empty:
        log.warning("transform produced nothing")
        return df

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
        if df.empty:
            return

        n = 0
        for _, row in df.iterrows():
            # INSERT OR IGNORE so re-runs don't blow up on UNIQUE company_name
            conn.execute(
                "INSERT OR IGNORE INTO companies (company_name) VALUES (?)",
                (row["company_name"],),
            )
            company_id = conn.execute(
                "SELECT company_id FROM companies WHERE company_name = ?",
                (row["company_name"],),
            ).fetchone()[0]

            # REPLACE so description/skills refresh if Jobicy updates the same id
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


def export_csv(path: Path, csv_path: Path = CSV_PATH) -> pd.DataFrame:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    try:
        df = pd.read_sql_query(
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

    df.to_csv(csv_path, index=False)
    log.info("csv → %s (%s rows)", csv_path, len(df))
    return df


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
        # Don't fail the whole pipeline over Tableau flakiness
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

    df = transform(extract_all())
    load_to_sqlite(df, path)
    out = export_csv(path)
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

    log.info(
        "done — jobs=%s companies=%s data_roles=%s top_skills=%s",
        jobs, cos, data, skill_freq(out)[:10],
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(run())
    except Exception:
        log.exception("pipeline failed")
        raise SystemExit(1)
