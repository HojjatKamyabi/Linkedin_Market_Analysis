CREATE DATABASE airflow;
CREATE DATABASE jobdata;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS skills JSONB;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP;
ALTER TABLE top_skills ADD COLUMN IF NOT EXISTS Skill TEXT;
ALTER TABLE top_skills ADD COLUMN IF NOT EXISTS Count INT;
