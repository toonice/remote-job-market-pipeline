-- SQLite schema for the remote job pipeline.
-- Core columns match the project brief; extras are optional enrichment.

CREATE TABLE IF NOT EXISTS companies (
    company_id INTEGER PRIMARY KEY AUTOINCREMENT,
    company_name TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS job_postings (
    job_id TEXT PRIMARY KEY,
    title TEXT,
    company_id INTEGER,
    location TEXT,
    date_posted TEXT,
    description TEXT,
    extracted_skills TEXT,
    url TEXT,
    industry TEXT,
    job_type TEXT,
    job_level TEXT,
    salary_min REAL,
    salary_max REAL,
    salary_currency TEXT,
    is_data_role INTEGER DEFAULT 0,
    ingested_at TEXT,
    FOREIGN KEY(company_id) REFERENCES companies(company_id)
);

CREATE INDEX IF NOT EXISTS idx_job_postings_company_id ON job_postings(company_id);
CREATE INDEX IF NOT EXISTS idx_job_postings_date_posted ON job_postings(date_posted);
CREATE INDEX IF NOT EXISTS idx_job_postings_is_data_role ON job_postings(is_data_role);
CREATE INDEX IF NOT EXISTS idx_job_postings_industry ON job_postings(industry);
CREATE INDEX IF NOT EXISTS idx_companies_name ON companies(company_name);
