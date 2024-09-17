package repository

import (
	"context"
	"database/sql"
	"time"

	"github.com/jmoiron/sqlx"
)

type Repository interface {
	BeginTransaction(context.Context, *sql.TxOptions) (*sqlx.Tx, error)
	ScheduleJob(ctx context.Context, jobID string, scheduleType string, schedule string, maxDelay *time.Duration) (string, error)
}

type repository struct {
	db *sqlx.DB
}

func (r *repository) BeginTransaction(ctx context.Context, opts *sql.TxOptions) (*sqlx.Tx, error) {
	return r.db.BeginTxx(ctx, opts)
}

func (r *repository) ScheduleJob(ctx context.Context, jobID string, scheduleType string, schedule string, maxDelay *time.Duration) (string, error) {
	var scheduleID string
	err := r.db.GetContext(
		ctx,
		&scheduleID,
		`SELECT create_job_schedule($1, $2, $3, $4)`,
		jobID,
		scheduleType,
		schedule,
		maxDelay,
	)

	return scheduleID, err
}

func NewRepository(ctx context.Context, conn string) (Repository, func() error, error) {
	db, err := sqlx.ConnectContext(ctx, "postgres", conn)
	if err != nil {
		return nil, nil, err
	}

	return &repository{db: db}, db.Close, nil
}
