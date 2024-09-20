-- Version: 1.0.0
-- Description: Initial schema for job_schedules table
---------------------------------------------------------------------------------------------------------
-- 1. Create job_schedules table
---------------------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS job_schedules(
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id VARCHAR(256) NOT NULL,
    schedule_type VARCHAR(32) NOT NULL,
    schedule TEXT NOT NULL,
    last_run TIMESTAMP WITHOUT TIME ZONE DEFAULT NULL,
    next_run TIMESTAMP WITHOUT TIME ZONE DEFAULT NULL,
    locked_by VARCHAR(36) DEFAULT NULL,
    locked_until TIMESTAMP WITHOUT TIME ZONE DEFAULT NULL,
    metadata JSONB DEFAULT '{}'
);
---------------------------------------------------------------------------------------------------------
-- 2. Indexes
---------------------------------------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS job_schedules_job_id_idx ON job_schedules(job_id);
CREATE INDEX IF NOT EXISTS job_schedules_next_run_idx ON job_schedules(next_run);
---------------------------------------------------------------------------------------------------------
-- 3. Functions && Procedures
---------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION create_job_schedule(
        _job_id VARCHAR(256),
        _schedule_type VARCHAR(32),
        _schedule TEXT,
        _next_run TIMESTAMP WITHOUT TIME ZONE,
        _metadata JSONB DEFAULT '{}'
    ) RETURNS UUID AS $$
DECLARE job_schedule_id UUID;
BEGIN
INSERT INTO job_schedules(
        job_id,
        schedule_type,
        schedule,
        next_run,
        metadata
    )
VALUES(
        _job_id,
        _schedule_type,
        _schedule,
        _next_run,
        _metadata
    )
RETURNING id INTO job_schedule_id;
RETURN job_schedule_id;
END;
$$ LANGUAGE plpgsql;
---------------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE delete_job_schedule_by_schedule_id(_job_schedule_id UUID) AS $$ BEGIN
DELETE FROM job_schedules
WHERE id = _job_schedule_id;
END;
$$ LANGUAGE plpgsql;
---------------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE delete_job_schedule_by_job_id(_job_id VARCHAR(256)) AS $$ BEGIN
DELETE FROM job_schedules
WHERE job_id = _job_id;
END;
$$ LANGUAGE plpgsql;
---------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION next_executions(_limit INT, _offset INT) RETURNS TABLE(
        id UUID,
        job_id VARCHAR(256),
        schedule_type VARCHAR(32),
        schedule TEXT,
        last_run TIMESTAMP WITHOUT TIME ZONE,
        next_run TIMESTAMP WITHOUT TIME ZONE,
        locked_by VARCHAR(36),
        locked_until TIMESTAMP WITHOUT TIME ZONE,
        metadata JSONB
    ) AS $$ BEGIN RETURN QUERY
SELECT js.id,
    js.job_id,
    js.schedule_type,
    js.schedule,
    js.last_run,
    js.next_run,
    js.locked_by,
    js.locked_until,
    js.metadata
FROM job_schedules js
WHERE (
        js.next_run IS NULL
        OR js.next_run <= NOW()
    )
    AND (
        js.locked_until IS NULL
        OR js.locked_until <= NOW()
    )
ORDER BY js.last_run ASC
LIMIT _limit OFFSET _offset FOR
UPDATE SKIP LOCKED;
END;
$$ LANGUAGE plpgsql;
---------------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE update_next_run(
        _job_schedule_id UUID,
        _last_run TIMESTAMP WITHOUT TIME ZONE,
        _next_run TIMESTAMP WITHOUT TIME ZONE
    ) AS $$ BEGIN
UPDATE job_schedules
SET last_run = _last_run,
    next_run = _next_run,
    locked_until = NULL,
    locked_by = NULL
WHERE id = _job_schedule_id;
END;
$$ LANGUAGE plpgsql;
---------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lock_job_schedule(
        _job_schedule_id UUID,
        _locked_by VARCHAR(36),
        _lock_until TIMESTAMP WITHOUT TIME ZONE
    ) RETURNS BOOL AS $$
DECLARE updated_id UUID;
BEGIN
UPDATE job_schedules
SET locked_until = _lock_until,
    locked_by = _locked_by
WHERE id = _job_schedule_id
    AND (
        locked_until IS NULL
        OR locked_until <= NOW()
    )
RETURNING id INTO updated_id;
RETURN updated_id IS NOT NULL;
END;
$$ LANGUAGE plpgsql;
---------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION unlock_job_schedule(
        _job_schedule_id UUID,
        _locked_by VARCHAR(36)
    ) RETURNS BOOL AS $$
DECLARE updated_id UUID;
BEGIN
UPDATE job_schedules
SET locked_until = NULL,
    locked_by = NULL
WHERE id = _job_schedule_id
    AND locked_by = _locked_by
RETURNING id INTO updated_id;
RETURN updated_id IS NOT NULL;
END;
$$ LANGUAGE plpgsql;
---------------------------------------------------------------------------------------------------------