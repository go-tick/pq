package pq

import (
	"context"

	gotick "github.com/go-tick/core"
	"github.com/go-tick/pq/internal/repository"
	"github.com/google/uuid"

	_ "github.com/lib/pq"
)

type repositoryFactory func(context.Context, string) (repository.Repository, func() error, error)

type driver struct {
	cfg               *PqConfig
	memberID          string
	repositoryFactory repositoryFactory
	cancel            context.CancelFunc
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

func (d *driver) NextExecution(context.Context) *gotick.JobPlannedExecution {
	panic("unimplemented")
}

func (d *driver) ScheduleJob(ctx context.Context, job gotick.Job, schedule gotick.JobSchedule) (string, error) {
	repo, close, err := d.repositoryFactory(ctx, d.cfg.conn)
	if err != nil {
		return "", err
	}
	defer close()

	sch, err := d.cfg.scheduleSerializer(schedule)
	if err != nil {
		return "", err
	}

	return repo.ScheduleJob(ctx, job.ID(), sch.ScheduleType, sch.Schedule, sch.MaxDelay)
}

func (d *driver) UnscheduleJobByJobID(ctx context.Context, jobID string) error {
	panic("unimplemented")
}

func (d *driver) UnscheduleJobByScheduleID(ctx context.Context, scheduleID string) error {
	panic("unimplemented")
}

var _ gotick.SchedulerDriver = &driver{}
var _ gotick.SchedulerSubscriber = &driver{}

func newDriver(cfg *PqConfig, fact repositoryFactory) *driver {
	return &driver{
		cfg:               cfg,
		memberID:          uuid.NewString(),
		repositoryFactory: fact,
		cancel:            func() {},
	}
}
