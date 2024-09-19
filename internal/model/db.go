package model

import "time"

type JobSchedule struct {
	ID           string         `db:"id"`
	JobID        string         `db:"job_id"`
	ScheduleType string         `db:"schedule_type"`
	Schedule     string         `db:"schedule"`
	MaxDelay     *time.Duration `db:"max_delay"`
	LastRun      *time.Time     `db:"last_run"`
	NextRun      *time.Time     `db:"next_run"`
	LockedBy     *string        `db:"locked_by"`
	LockedAt     *time.Time     `db:"locked_at"`
}
