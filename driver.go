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

	_ "github.com/lib/pq"
)

type repositoryFactoryWoTx func(context.Context, string) (repository.Repository, func() error, error)
type repositoryFactoryWithTx func(context.Context, string, *sql.TxOptions) (repository.Repository, func() error, error)

type plannedExecution struct {
	id      string
	nextRun time.Time
}

type driver struct {
	cfg                     *PqConfig
	memberID                string
	repositoryFactoryWoTx   repositoryFactoryWoTx
	repositoryFactoryWithTx repositoryFactoryWithTx
	cancel                  context.CancelFunc
	listeners               []ErrorListener
}

func (d *driver) OnBeforeJobExecution(*gotick.JobExecutionContext) {
	panic("unimplemented")
}

func (d *driver) OnBeforeJobExecutionPlan(*gotick.JobExecutionContext) {
	panic("unimplemented")
}

func (d *driver) OnJobExecuted(*gotick.JobExecutionContext) {
	panic("unimplemented")
}

func (d *driver) OnJobExecutionDelayed(*gotick.JobExecutionContext) {
	panic("unimplemented")
}

func (d *driver) OnJobExecutionInitiated(*gotick.JobExecutionContext) {
	panic("unimplemented")
}

func (d *driver) OnJobExecutionSkipped(*gotick.JobExecutionContext) {
	panic("unimplemented")
}

func (d *driver) OnJobExecutionUnplanned(*gotick.JobExecutionContext) {
	panic("unimplemented")
}

func (d *driver) OnStart() {
	panic("unimplemented")
}

func (d *driver) OnStop() {
	panic("unimplemented")
}

func (d *driver) NextExecution(ctx context.Context) *gotick.JobPlannedExecution {
	repo, close, err := d.repositoryFactoryWithTx(ctx, d.cfg.conn, nil)
	if err != nil {
		return nil
	}
	defer close()

	offset := 0
	jobsToUnschedule := make([]string, 0)
	plannedExecutions := make([]plannedExecution, 0)
	var execution *gotick.JobPlannedExecution

	for {
		schedules, err := repo.NextExecutions(ctx, 10, offset)
		if err != nil {
			d.onError(err)
			return nil
		}

		for _, entry := range schedules {
			sch, err := d.cfg.scheduleDeserializer(PqJobSchedule{
				ScheduleType: entry.ScheduleType,
				Schedule:     entry.Schedule,
				MaxDelay:     entry.MaxDelay,
			})
			if err != nil {
				d.onError(err)
				return nil
			}

			if entry.NextRun == nil {
				jobsToUnschedule = append(jobsToUnschedule, entry.ID)
				continue
			}

			if execution == nil || execution.PlannedAt.After(*entry.NextRun) {
				execution = &gotick.JobPlannedExecution{
					JobScheduledExecution: gotick.JobScheduledExecution{
						Schedule:   sch,
						ScheduleID: entry.ID,
						// ToDo: update contracts to have JobID here
					},
					ExecutionID: uuid.NewString(),
					PlannedAt:   *entry.NextRun,
				}
			} else {
				plannedExecutions = append(plannedExecutions, plannedExecution{
					id:      entry.ID,
					nextRun: *entry.NextRun,
				})
			}
		}
	}

	panic("unimplemented")
}

func (d *driver) ScheduleJob(ctx context.Context, job gotick.Job, schedule gotick.JobSchedule) (string, error) {
	repo, close, err := d.repositoryFactoryWoTx(ctx, d.cfg.conn)
	if err != nil {
		return "", err
	}
	defer close()

	sch, err := d.cfg.scheduleSerializer(schedule)
	if err != nil {
		return "", err
	}

	next := schedule.First()
	entry := model.JobSchedule{
		JobID:        job.ID(),
		ScheduleType: sch.ScheduleType,
		Schedule:     sch.Schedule,
		MaxDelay:     sch.MaxDelay,
		NextRun:      &next,
	}

	return repo.ScheduleJob(ctx, entry)
}

func (d *driver) UnscheduleJobByJobID(ctx context.Context, jobID string) error {
	repo, close, err := d.repositoryFactoryWoTx(ctx, d.cfg.conn)
	if err != nil {
		return err
	}
	defer close()

	return repo.UnscheduleJobByJobID(ctx, jobID)
}

func (d *driver) UnscheduleJobByScheduleID(ctx context.Context, scheduleID string) error {
	repo, close, err := d.repositoryFactoryWoTx(ctx, d.cfg.conn)
	if err != nil {
		return err
	}
	defer close()

	return repo.UnscheduleJobByScheduleID(ctx, scheduleID)
}

func (d *driver) onError(err error) {
	for _, listener := range d.listeners {
		listener.OnError(err)
	}
}

var _ gotick.SchedulerDriver = &driver{}
var _ gotick.SchedulerSubscriber = &driver{}

func newDriver(cfg *PqConfig, factByConnStr repositoryFactoryWoTx, factByConn repositoryFactoryWithTx) *driver {
	return &driver{
		cfg:                     cfg,
		memberID:                uuid.NewString(),
		repositoryFactoryWoTx:   factByConnStr,
		repositoryFactoryWithTx: factByConn,
		cancel:                  func() {},
		listeners:               slices.Clone(cfg.errorListeners),
	}
}
