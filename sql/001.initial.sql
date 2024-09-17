CREATE TABLE IF NOT EXISTS job_schedules(
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id VARCHAR(256) NOT NULL,
    schedule_type VARCHAR(32) NOT NULL,
    schedule TEXT NOT NULL,
    max_delay INTERVAL,
    last_run TIMESTAMP WITHOUT TIME ZONE DEFAULT NULL,
    locked_by VARCHAR(256),
    locked_until TIMESTAMP WITHOUT TIME ZONE DEFAULT NULL
);
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
$$ LANGUAGE SQL;