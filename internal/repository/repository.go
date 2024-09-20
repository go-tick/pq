package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/go-tick/pq/internal/model"
	"github.com/jmoiron/sqlx"

	_ "github.com/lib/pq"
)

var (
	ErrTransactionNotSupported = fmt.Errorf("transaction not supported")
)

type Repository interface {
	Commit(context.Context) error
	ScheduleJob(ctx context.Context, sch model.JobSchedule) (string, error)
	UnscheduleJobByJobID(ctx context.Context, jobID string) error
	UnscheduleJobByScheduleID(ctx context.Context, scheduleID string) error
	NextExecutions(ctx context.Context, limit, offset uint) ([]model.JobSchedule, error)
	LockJobSchedule(ctx context.Context, lockedBy string, scheduleID string, deadline time.Time) (bool, error)
	UnlockJobSchedule(ctx context.Context, lockedBy string, scheduleID string) (bool, error)
	UpdateNextRun(ctx context.Context, scheduleID string, lastRun, nextRun time.Time) error
}

type Connection interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
	SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
}

type repository struct {
	db Connection
}

func (r *repository) Commit(ctx context.Context) error {
	if tx, ok := r.db.(*sqlx.Tx); ok {
		return tx.Commit()
	}

	return ErrTransactionNotSupported
}

func (r *repository) ScheduleJob(ctx context.Context, sch model.JobSchedule) (string, error) {
	var scheduleID string
	err := r.db.GetContext(
		ctx,
		&scheduleID,
		`SELECT create_job_schedule($1, $2, $3, $4, $5)`,
		sch.JobID,
		sch.ScheduleType,
		sch.Schedule,
		sch.NextRun.UTC(),
		sch.Metadata,
	)

	return scheduleID, err
}

func (r *repository) UnscheduleJobByJobID(ctx context.Context, jobID string) error {
	_, err := r.db.ExecContext(ctx, `CALL delete_job_schedule_by_job_id($1)`, jobID)
	return err
}

func (r *repository) UnscheduleJobByScheduleID(ctx context.Context, scheduleID string) error {
	_, err := r.db.ExecContext(ctx, `CALL delete_job_schedule_by_schedule_id($1)`, scheduleID)
	return err
}

func (r *repository) NextExecutions(ctx context.Context, limit, offset uint) ([]model.JobSchedule, error) {
	var schedules []model.JobSchedule
	err := r.db.SelectContext(
		ctx,
		&schedules,
		`SELECT * FROM next_executions($1, $2)`,
		limit,
		offset,
	)

	return schedules, err
}

func (r *repository) LockJobSchedule(ctx context.Context, lockedBy string, scheduleID string, lockedUntil time.Time) (bool, error) {
	var locked bool
	err := r.db.GetContext(
		ctx,
		&locked,
		`SELECT lock_job_schedule($1, $2, $3)`,
		scheduleID,
		lockedBy,
		lockedUntil.UTC(),
	)

	return locked, err
}

func (r *repository) UnlockJobSchedule(ctx context.Context, lockedBy string, scheduleID string) (bool, error) {
	var unlocked bool
	err := r.db.GetContext(
		ctx,
		&unlocked,
		`SELECT unlock_job_schedule($1, $2)`,
		scheduleID,
		lockedBy,
	)

	return unlocked, err
}

func (r *repository) UpdateNextRun(ctx context.Context, scheduleID string, lastRun, nextRun time.Time) error {
	_, err := r.db.ExecContext(
		ctx,
		`CALL update_next_run($1, $2, $3)`,
		scheduleID,
		lastRun.UTC(),
		nextRun.UTC(),
	)

	return err
}

func NewRepositoryWoTx(ctx context.Context, conn string) (Repository, func() error, error) {
	db, err := sqlx.ConnectContext(ctx, "postgres", conn)
	if err != nil {
		return nil, nil, err
	}

	return &repository{db}, db.Close, nil
}

func NewRepositoryWithTx(ctx context.Context, conn string, opts *sql.TxOptions) (Repository, func() error, error) {
	db, err := sqlx.ConnectContext(ctx, "postgres", conn)
	if err != nil {
		return nil, nil, err
	}

	tx, err := db.BeginTxx(ctx, opts)
	if err != nil {
		err1 := db.Close()
		return nil, nil, errors.Join(err, err1)
	}

	close := func() error {
		err1 := tx.Rollback()
		err2 := db.Close()

		return errors.Join(err1, err2)
	}

	return &repository{tx}, close, nil
}
