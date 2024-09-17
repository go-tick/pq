CREATE TABLE IF NOT EXISTS job_schedules(
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id VARCHAR(256) NOT NULL,
    schedule_type VARCHAR(32) NOT NULL,
    schedule TEXT NOT NULL,
    max_delay INTERVAL,
    last_run TIMESTAMP WITHOUT TIME ZONE DEFAULT NULL,
    locked_by VARCHAR(36) DEFAULT NULL,
    locked_until TIMESTAMP WITHOUT TIME ZONE DEFAULT NULL
);
CREATE INDEX IF NOT EXISTS job_schedules_job_id_idx ON job_schedules(job_id);
CREATE INDEX IF NOT EXISTS job_schedules_last_run_idx ON job_schedules(last_run);
CREATE OR REPLACE FUNCTION create_job_schedule(
        _job_id VARCHAR(256),
        _schedule_type VARCHAR(32),
        _schedule TEXT,
        _max_delay INTERVAL
    ) RETURNS UUID AS $$
DECLARE job_schedule_id UUID;
BEGIN
INSERT INTO job_schedules(job_id, schedule_type, schedule, max_delay)
VALUES(_job_id, _schedule_type, _schedule, _max_delay)
RETURNING id INTO job_schedule_id;
RETURN job_schedule_id;
END;
$$ LANGUAGE plpgsql;
CREATE OR REPLACE PROCEDURE delete_job_schedule_by_schedule_id(_job_schedule_id UUID) AS $$ BEGIN
DELETE FROM job_schedules
WHERE id = _job_schedule_id;
END;
$$ LANGUAGE plpgsql;
CREATE OR REPLACE PROCEDURE delete_job_schedule_by_job_id(_job_id UUID) AS $$ BEGIN
DELETE FROM job_schedules
WHERE job_id = _job_id;
END;
$$ LANGUAGE plpgsql;
CREATE OR REPLACE FUNCTION next_executions(_limit INT, _offset INT) RETURNS TABLE(
        id UUID,
        job_id VARCHAR(256),
        schedule_type VARCHAR(32),
        schedule TEXT,
        max_delay INTERVAL,
        last_run TIMESTAMP WITHOUT TIME ZONE,
        locked_by VARCHAR(36),
        locked_until TIMESTAMP WITHOUT TIME ZONE
    ) AS $$ BEGIN
SELECT id,
    job_id,
    schedule_type,
    schedule,
    max_delay,
    last_run,
    locked_by,
    locked_until
FROM job_schedules
WHERE (
        last_run IS NULL
        OR last_run <= NOW()
    )
    AND (
        locked_until IS NULL
        OR locked_until <= NOW()
    )
ORDER BY last_run ASC
LIMIT _limit FOR OFFSET _offset
UPDATE SKIP LOCKED;
END;
$$ LANGUAGE plpgsql;
CREATE OR REPLACE FUNCTION lock_job_schedule(
        _job_schedule_id UUID,
        _locked_by VARCHAR(36),
        _lock_until TIMESTAMP WITHOUT TIME ZONE
    ) RETURNS BOOL AS $$ BEGIN
UPDATE job_schedules
SET locked_until = _lock_until,
    locked_by = _locked_by
WHERE id = _job_schedule_id
    AND (
        locked_until IS NULL
        OR locked_until <= NOW()
    )
RETURNING id IS NOT NULL;
END;
$$ LANGUAGE plpgsql;
CREATE OR REPLACE FUNCTION unlock_job_schedule(_job_schedule_id UUID, _locked_by VARCHAR(36)) RETURNS BOOL AS $$ BEGIN
UPDATE job_schedules
SET locked_until = NULL
WHERE id = _job_schedule_id
    AND locked_by = _locked_by
RETURNING id IS NOT NULL;
END;
$$ LANGUAGE plpgsql;