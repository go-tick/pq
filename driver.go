package pq

import (
	"context"
	"database/sql"
	"slices"
	"time"

	gotick "github.com/go-tick/core"
	"github.com/go-tick/pq/internal/model"
	"github.com/go-tick/pq/internal/repository"
	"github.com/google/uuid"
)

type repositoryFactoryWoTx func(context.Context, string) (repository.Repository, func() error, error)
type repositoryFactoryWithTx func(context.Context, string, *sql.TxOptions) (repository.Repository, func() error, error)

type driver struct {
	cfg                     *PqConfig
	memberID                string
	repositoryFactoryWoTx   repositoryFactoryWoTx
	repositoryFactoryWithTx repositoryFactoryWithTx
	observers               []gotick.ErrorObserver
}

func (d *driver) OnBeforeJobExecution(*gotick.JobExecutionContext) {
}

func (d *driver) OnBeforeJobExecutionPlan(*gotick.JobExecutionContext) {
}

func (d *driver) OnJobExecuted(ctx *gotick.JobExecutionContext) {
	d.onJobExecuted(ctx)
}

func (d *driver) OnJobExecutionDelayed(*gotick.JobExecutionContext) {
}

func (d *driver) OnJobExecutionInitiated(*gotick.JobExecutionContext) {
}

func (d *driver) OnJobExecutionSkipped(ctx *gotick.JobExecutionContext) {
	d.onJobExecuted(ctx)
}

func (d *driver) OnJobExecutionUnplanned(ctx *gotick.JobExecutionContext) {
	d.onJobUnplanned(ctx)
}

func (d *driver) OnStart() {
}

func (d *driver) OnStop() {
}

func (d *driver) Start(context.Context) error {
	return nil
}

func (d *driver) Stop() error {
	return nil
}

func (d *driver) Subscribe(observer gotick.ErrorObserver) {
	d.observers = append(d.observers, observer)
}

func (d *driver) NextExecution(ctx context.Context) (execution *gotick.NextExecutionResult) {
	repo, close, err := d.repositoryFactoryWithTx(ctx, d.cfg.conn, nil)
	if err != nil {
		return nil
	}
	defer close()

	offset := 0

	for {
		schedules, err := repo.NextExecutions(ctx, d.cfg.batchSize, uint(offset))
		offset = offset + len(schedules)

		if err != nil {
			d.onError(err)
			return nil
		}

		if len(schedules) == 0 {
			break
		}

		for _, entry := range schedules {
			sch, err := d.cfg.scheduleDeserializer(PqJobSchedule{
				ScheduleType: entry.ScheduleType,
				Schedule:     entry.Schedule,
				Metadata:     entry.Metadata,
			})
			if err != nil {
				d.onError(err)
				return nil
			}

			if execution == nil || execution.PlannedAt.After(*entry.NextRun) {
				execution = &gotick.NextExecutionResult{
					JobID:      gotick.JobID(entry.JobID),
					Schedule:   sch,
					ScheduleID: entry.ID,
					PlannedAt:  *entry.NextRun,
				}
			}
		}
	}

	if execution != nil {
		lockUntil := time.Now().Add(d.cfg.lockTimeout)
		if timeout, ok := gotick.TimeoutFromJobSchedule(execution.Schedule); ok {
			lockUntil = execution.PlannedAt.Add(timeout)
		}

		locked, err := repo.LockJobSchedule(ctx, d.memberID, execution.ScheduleID, lockUntil)
		if err != nil {
			d.onError(err)
			return nil
		}

		if !locked {
			d.onError(ErrCannotLockJob)
			return nil
		}
	}

	err = repo.Commit(ctx)
	if err != nil {
		d.onError(err)
		return nil
	}

	return execution
}

func (d *driver) ScheduleJob(ctx context.Context, jobID gotick.JobID, schedule gotick.JobSchedule) (string, error) {
	repo, close, err := d.repositoryFactoryWoTx(ctx, d.cfg.conn)
	if err != nil {
		return "", err
	}
	defer close()

	sch, err := d.cfg.scheduleSerializer(schedule)
	if err != nil {
		return "", err
	}

	first := schedule.First()
	entry := model.JobSchedule{
		JobID:        string(jobID),
		ScheduleType: sch.ScheduleType,
		Schedule:     sch.Schedule,
		Metadata:     sch.Metadata,
		NextRun:      &first,
	}

	return repo.ScheduleJob(ctx, entry)
}

func (d *driver) UnscheduleJobByJobID(ctx context.Context, jobID gotick.JobID) error {
	repo, close, err := d.repositoryFactoryWoTx(ctx, d.cfg.conn)
	if err != nil {
		return err
	}
	defer close()

	return repo.UnscheduleJobByJobID(ctx, string(jobID))
}

func (d *driver) UnscheduleJobByScheduleID(ctx context.Context, scheduleID string) error {
	repo, close, err := d.repositoryFactoryWoTx(ctx, d.cfg.conn)
	if err != nil {
		return err
	}
	defer close()

	return repo.UnscheduleJobByScheduleID(ctx, scheduleID)
}

func (d *driver) onJobExecuted(ctx *gotick.JobExecutionContext) {
	repo, close, err := d.repositoryFactoryWoTx(ctx, d.cfg.conn)
	if err != nil {
		d.onError(err)
		return
	}
	defer close()

	// add microsecond to planned at time as Postgres has a limitation on the precision of the timestamp.
	// without this, the next run will be the same as the planned at time.
	nextRun := ctx.Schedule.Next(ctx.PlannedAt.Add(time.Microsecond))
	if nextRun == nil {
		err = repo.UnscheduleJobByScheduleID(ctx, ctx.ScheduleID)
	} else {
		err = repo.UpdateNextRun(ctx, ctx.ScheduleID, ctx.PlannedAt, *nextRun)
	}

	if err != nil {
		d.onError(err)
		return
	}
}

func (d *driver) onJobUnplanned(ctx *gotick.JobExecutionContext) {
	repo, close, err := d.repositoryFactoryWoTx(ctx, d.cfg.conn)
	if err != nil {
		d.onError(err)
		return
	}
	defer close()

	unlocked, err := repo.UnlockJobSchedule(ctx, d.memberID, ctx.ScheduleID)
	if err != nil {
		d.onError(err)
		return
	}

	if !unlocked {
		d.onError(ErrCannotUnlockJob)
		return
	}
}

func (d *driver) onError(err error) {
	for _, listener := range d.observers {
		listener.OnError(err)
	}
}

var _ gotick.SchedulerDriver = &driver{}
var _ gotick.SchedulerObserver = &driver{}
var _ gotick.Publisher[gotick.ErrorObserver] = &driver{}

func newDriver(cfg *PqConfig, factByConnStr repositoryFactoryWoTx, factByConn repositoryFactoryWithTx) *driver {
	return &driver{
		cfg:                     cfg,
		memberID:                uuid.NewString(),
		repositoryFactoryWoTx:   factByConnStr,
		repositoryFactoryWithTx: factByConn,
		observers:               slices.Clone(cfg.errorObservers),
	}
}
