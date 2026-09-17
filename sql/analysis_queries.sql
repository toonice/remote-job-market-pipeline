-- Queries I use when demoing the DB (SQLite window functions).
-- Run against remote_jobs.db.

-- Skill demand — explode comma-separated extracted_skills
WITH RECURSIVE split_skills AS (
    SELECT
        job_id,
        TRIM(extracted_skills) AS skills_remaining,
        CASE
            WHEN INSTR(extracted_skills, ',') > 0
                THEN TRIM(SUBSTR(extracted_skills, 1, INSTR(extracted_skills, ',') - 1))
            ELSE TRIM(extracted_skills)
        END AS skill
    FROM job_postings
    WHERE extracted_skills IS NOT NULL AND TRIM(extracted_skills) != ''

    UNION ALL

    SELECT
        job_id,
        TRIM(SUBSTR(skills_remaining, INSTR(skills_remaining, ',') + 1)),
        CASE
            WHEN INSTR(SUBSTR(skills_remaining, INSTR(skills_remaining, ',') + 1), ',') > 0
                THEN TRIM(
                    SUBSTR(
                        SUBSTR(skills_remaining, INSTR(skills_remaining, ',') + 1),
                        1,
                        INSTR(SUBSTR(skills_remaining, INSTR(skills_remaining, ',') + 1), ',') - 1
                    )
                )
            ELSE TRIM(SUBSTR(skills_remaining, INSTR(skills_remaining, ',') + 1))
        END
    FROM split_skills
    WHERE INSTR(skills_remaining, ',') > 0
),
skill_counts AS (
    SELECT skill, COUNT(DISTINCT job_id) AS job_count
    FROM split_skills
    WHERE skill IS NOT NULL AND skill != ''
    GROUP BY skill
)
SELECT
    skill,
    job_count,
    ROUND(100.0 * job_count / SUM(job_count) OVER (), 2) AS pct_of_skill_mentions,
    RANK() OVER (ORDER BY job_count DESC) AS skill_rank,
    DENSE_RANK() OVER (ORDER BY job_count DESC) AS skill_dense_rank
FROM skill_counts
ORDER BY job_count DESC;


-- Who's posting the most
WITH daily AS (
    SELECT
        c.company_name,
        j.date_posted,
        COUNT(*) AS posts
    FROM job_postings j
    JOIN companies c ON c.company_id = j.company_id
    WHERE j.date_posted IS NOT NULL AND j.date_posted != ''
    GROUP BY c.company_name, j.date_posted
),
company_totals AS (
    SELECT
        company_name,
        SUM(posts) AS total_posts,
        COUNT(DISTINCT date_posted) AS active_days,
        MIN(date_posted) AS first_post,
        MAX(date_posted) AS last_post
    FROM daily
    GROUP BY company_name
)
SELECT
    company_name,
    total_posts,
    active_days,
    first_post,
    last_post,
    ROUND(1.0 * total_posts / NULLIF(active_days, 0), 2) AS posts_per_active_day,
    RANK() OVER (ORDER BY total_posts DESC) AS velocity_rank,
    ROUND(100.0 * total_posts / SUM(total_posts) OVER (), 2) AS pct_of_all_posts
FROM company_totals
ORDER BY total_posts DESC
LIMIT 25;


-- Rolling 7-day volume
WITH day_counts AS (
    SELECT date_posted, COUNT(*) AS jobs_posted
    FROM job_postings
    WHERE date_posted IS NOT NULL AND date_posted != ''
    GROUP BY date_posted
)
SELECT
    date_posted,
    jobs_posted,
    SUM(jobs_posted) OVER (
        ORDER BY date_posted
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS rolling_7d_jobs,
    AVG(jobs_posted) OVER (
        ORDER BY date_posted
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS rolling_7d_avg,
    LAG(jobs_posted, 1) OVER (ORDER BY date_posted) AS prev_day_jobs,
    jobs_posted - LAG(jobs_posted, 1) OVER (ORDER BY date_posted) AS day_over_day_delta
FROM day_counts
ORDER BY date_posted;


-- Salary bands on data roles (rough — lots of postings omit pay)
WITH paid AS (
    SELECT
        job_level,
        CASE
            WHEN salary_min IS NOT NULL AND salary_max IS NOT NULL
                THEN (salary_min + salary_max) / 2.0
            ELSE COALESCE(salary_min, salary_max)
        END AS salary_mid
    FROM job_postings
    WHERE is_data_role = 1
      AND (salary_min IS NOT NULL OR salary_max IS NOT NULL)
)
SELECT
    COALESCE(NULLIF(job_level, ''), 'Unspecified') AS job_level,
    COUNT(*) AS roles_with_salary,
    ROUND(AVG(salary_mid), 0) AS avg_mid,
    ROUND(MIN(salary_mid), 0) AS min_mid,
    ROUND(MAX(salary_mid), 0) AS max_mid
FROM paid
GROUP BY COALESCE(NULLIF(job_level, ''), 'Unspecified')
ORDER BY avg_mid DESC;


-- Industry mix: data roles vs everything else
WITH industry_stats AS (
    SELECT
        COALESCE(NULLIF(industry, ''), 'Unspecified') AS industry,
        COUNT(*) AS total_jobs,
        SUM(is_data_role) AS data_jobs
    FROM job_postings
    GROUP BY COALESCE(NULLIF(industry, ''), 'Unspecified')
)
SELECT
    industry,
    total_jobs,
    data_jobs,
    ROUND(100.0 * data_jobs / NULLIF(total_jobs, 0), 1) AS data_role_pct,
    ROUND(100.0 * total_jobs / SUM(total_jobs) OVER (), 2) AS market_share_pct,
    RANK() OVER (ORDER BY data_jobs DESC) AS data_demand_rank
FROM industry_stats
ORDER BY data_jobs DESC, total_jobs DESC;
