# Remote Job Market Pipeline

I built this to keep an eye on the remote data job market without refreshing five job boards every morning. Originally I was aiming at remote Data Analyst / BI roles around **£40k+**, but the listings that actually fit a UK junior profile sit nearer **£30k**, so the exports now lean that way.

**Manish Trivedi** · [`toonice`](https://github.com/toonice)

Primary data from [Jobicy](https://jobicy.com/), plus free pulls from Remotive, Remote OK and Arbeitnow. Optional Adzuna UK if you set keys. Please credit the boards and link apply buttons back to their original job URLs.

---

## Why I built this

I was spending too long manually scanning remote listings for SQL / Python / BI roles and still missing salary and skill patterns. So I wired up a small daily pipeline: job board APIs → this script → SQLite → CSV → Tableau. GitHub Actions runs it at 09:00 UTC and commits the updated DB + CSVs. That gives me a rolling view of skill demand and which companies are posting, which I use for CV keywords and where to apply next.

---

## How it fits together

```
Jobicy (+ Remotive / RemoteOK / Arbeitnow [/ Adzuna])
        →  pipeline.py  →  remote_jobs.db (SQLite)
                    │
                    ├── data/remote_jobs_tableau.csv      (UK-friendly, junior-ish, ≤£35k or unknown)
                    ├── data/remote_jobs_junior_uk.csv    (stricter junior + UK + £35k — for applications)
                    ├── data/remote_jobs_tableau_all.csv  (everything we pulled)
                    └── optional Tableau Server publish (if secrets set)
                           ↑
              GitHub Actions (daily + manual)
```

1. **Extract** — Jobicy first (`count=100`, then `tag=data`). Also Remotive (`category=data` + `search=junior data analyst`), Remote OK (skip the metadata row), Arbeitnow (a few pages). Adzuna UK only if `ADZUNA_APP_ID` + `ADZUNA_APP_KEY` are set  
2. **Transform** — normalise into one shape; prefix `job_id` by source (`jobicy_123`, `remotive_456`, …); regex for SQL, Python, Power BI, Tableau, Excel, Snowflake, dbt; flag likely data roles; rough salary parse where boards only give text  
3. **Load** — SQLite upsert (companies by name, jobs by id). Each run replaces the job snapshot so ids stay clean  
4. **Tableau / apply lists** — always write the CSVs; TSC publish only if server env vars are present

---

## Run it locally

```bash
git clone https://github.com/toonice/remote-job-market-pipeline.git
cd remote-job-market-pipeline
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python pipeline.py
```

You'll get:

- `remote_jobs.db` — I keep this in the repo so recruiters can open it without running anything  
- `data/remote_jobs_tableau.csv` — UK-friendly geos, prefers junior / non-senior titles, keeps rows with unknown salary or midpoint ≤ ~£35k (USD×0.78 / EUR×0.86 / CAD×0.57)  
- `data/remote_jobs_junior_uk.csv` — stricter junior + UK/remote + £35k filter, handy as an apply shortlist  
- `data/remote_jobs_tableau_all.csv` — full multi-source pull  
- `logs/pipeline.log` — whatever happened on the last run  

`DB_PATH` defaults to `./remote_jobs.db` if you want it elsewhere.

### Optional Adzuna (UK)

Free developer keys from Adzuna. If either env var is missing the adapter just skips.

| Env | Notes |
|-----|--------|
| `ADZUNA_APP_ID` | app id |
| `ADZUNA_APP_KEY` | app key |

Query used: `what=junior data analyst`, `where=uk`.

### Tableau secrets (optional)

Only needed for Tableau Server / Cloud. Leave them blank for Public.

| Secret | Notes |
|--------|--------|
| `TABLEAU_SERVER_URL` | empty = skip publish, CSV only |
| `TABLEAU_SITE_ID` | use `""` for the default site |
| `TABLEAU_TOKEN_NAME` / `TABLEAU_TOKEN_VALUE` | PAT |
| `TABLEAU_PROJECT_NAME` / `TABLEAU_DATASOURCE_NAME` | where to land |

Actions workflow: `.github/workflows/run_pipeline.yml` — cron `0 9 * * *` plus `workflow_dispatch`. Needs `contents: write` so it can commit the refreshed data.

---

## Schema

`companies(company_id, company_name UNIQUE)`

`job_postings` required cols: `job_id`, `title`, `company_id`, `location`, `date_posted`, `description`, `extracted_skills`

I also store `url`, `industry`, `job_type`, `job_level`, salary fields, `is_data_role`, `ingested_at` — same table, doesn't break the core schema. See `schema.sql`. Job ids are prefixed by source so boards don't collide.

---

## SQL I actually use for demos

Skill demand (split the CSV skills column, then rank):

```sql
WITH RECURSIVE split_skills AS (
    SELECT job_id, TRIM(extracted_skills) AS skills_remaining,
           CASE WHEN INSTR(extracted_skills, ',') > 0
                THEN TRIM(SUBSTR(extracted_skills, 1, INSTR(extracted_skills, ',') - 1))
                ELSE TRIM(extracted_skills) END AS skill
    FROM job_postings
    WHERE extracted_skills IS NOT NULL AND TRIM(extracted_skills) != ''
    UNION ALL
    SELECT job_id,
           TRIM(SUBSTR(skills_remaining, INSTR(skills_remaining, ',') + 1)),
           CASE WHEN INSTR(SUBSTR(skills_remaining, INSTR(skills_remaining, ',') + 1), ',') > 0
                THEN TRIM(SUBSTR(SUBSTR(skills_remaining, INSTR(skills_remaining, ',') + 1), 1,
                      INSTR(SUBSTR(skills_remaining, INSTR(skills_remaining, ',') + 1), ',') - 1))
                ELSE TRIM(SUBSTR(skills_remaining, INSTR(skills_remaining, ',') + 1)) END
    FROM split_skills WHERE INSTR(skills_remaining, ',') > 0
),
skill_counts AS (
    SELECT skill, COUNT(DISTINCT job_id) AS job_count
    FROM split_skills WHERE skill != '' GROUP BY skill
)
SELECT skill, job_count,
       ROUND(100.0 * job_count / SUM(job_count) OVER (), 2) AS pct_of_skill_mentions,
       RANK() OVER (ORDER BY job_count DESC) AS skill_rank
FROM skill_counts
ORDER BY job_count DESC;
```

Companies posting the most:

```sql
SELECT c.company_name,
       COUNT(*) AS total_posts,
       COUNT(DISTINCT j.date_posted) AS active_days,
       ROUND(1.0 * COUNT(*) / NULLIF(COUNT(DISTINCT j.date_posted), 0), 2) AS posts_per_active_day,
       RANK() OVER (ORDER BY COUNT(*) DESC) AS velocity_rank
FROM job_postings j
JOIN companies c ON c.company_id = j.company_id
GROUP BY c.company_name
ORDER BY total_posts DESC
LIMIT 25;
```

Rolling week of market volume:

```sql
WITH day_counts AS (
    SELECT date_posted, COUNT(*) AS jobs_posted
    FROM job_postings
    WHERE date_posted IS NOT NULL AND date_posted != ''
    GROUP BY date_posted
)
SELECT date_posted, jobs_posted,
       SUM(jobs_posted) OVER (
           ORDER BY date_posted ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
       ) AS rolling_7d_jobs,
       LAG(jobs_posted, 1) OVER (ORDER BY date_posted) AS prev_day_jobs
FROM day_counts
ORDER BY date_posted;
```

More in `sql/analysis_queries.sql`.

---

## Skills I parse for

SQL, Python, Power BI (incl. PowerBI / power-bi), Tableau, Excel, Snowflake, dbt.

A row gets `is_data_role = 1` if any of those show up, or if the title/description/industry looks like analyst / scientist / analytics / BI work. I still store every job I fetch — the flag is just so Tableau filters are easy.

---

## Credit

Remote listings via **[Jobicy](https://jobicy.com/)**, [Remotive](https://remotive.com/), [Remote OK](https://remoteok.com/), [Arbeitnow](https://www.arbeitnow.com/), and optionally [Adzuna](https://developer.adzuna.com/). Their APIs ask for clear credit and that apply links go to the original URL — both are respected here.
