# Remote Job Market Pipeline

I built this to keep an eye on the remote data job market without refreshing five job boards every morning. I'm targeting remote Data Analyst / BI roles around **£40k+**, and I wanted something that pulls live postings, pulls out the skills that keep showing up, and drops straight into Tableau.

**Manish Trivedi** · [`toonice`](https://github.com/toonice)

Data from [Jobicy](https://jobicy.com/) — please credit them and link apply buttons back to their original job URLs.

---

## Why I built this

I was spending too long manually scanning remote listings for SQL / Python / BI roles and still missing salary and skill patterns. So I wired up a small daily pipeline: Jobicy API → this script → SQLite → CSV → Tableau. GitHub Actions runs it at 09:00 UTC and commits the updated DB + CSV. That gives me a rolling view of skill demand and which companies are posting, which I use for CV keywords and where to apply next.

---

## How it fits together

```
Jobicy API  →  pipeline.py  →  remote_jobs.db (SQLite)
                    │
                    ├── data/remote_jobs_tableau.csv
                    └── optional Tableau Server publish (if secrets set)
                           ↑
              GitHub Actions (daily + manual)
```

1. **Extract** — hit Jobicy (`count=100`), then again with `tag=data` and merge by job id  
2. **Transform** — pandas; normalise dates; regex for SQL, Python, Power BI, Tableau, Excel, Snowflake, dbt; flag likely data roles  
3. **Load** — SQLite upsert (companies by name, jobs by id so re-runs don't duplicate)  
4. **Tableau** — always write the CSV; TSC publish only if server env vars are present (Tableau Public can't use TSC — CSV / raw GitHub URL is fine)

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
- `data/remote_jobs_tableau.csv` — UK-friendly geos only (Anywhere / UK / Europe / EMEA); drop into Tableau Public / Desktop
- `data/remote_jobs_tableau_all.csv` — full Jobicy pull if you want everything  
- `logs/pipeline.log` — whatever happened on the last run  

`DB_PATH` defaults to `./remote_jobs.db` if you want it elsewhere.

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

I also store `url`, `industry`, `job_type`, `job_level`, salary fields, `is_data_role`, `ingested_at` — same table, doesn't break the core schema. See `schema.sql`.

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

Remote listings via **[Jobicy](https://jobicy.com/)**. Their API asks for a clear credit and that apply links go to the original URL — both are respected here.
