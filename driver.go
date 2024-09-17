package pq

import (
	"context"
	"database/sql"

	gotick "github.com/go-tick/core"
	"github.com/go-tick/pq/internal/repository"
	"github.com/google/uuid"

	_ "github.com/lib/pq"
)

type repositoryFactoryWoTx func(context.Context, string) (repository.Repository, func() error, error)
type repositoryFactoryWithTx func(context.Context, string, *sql.TxOptions) (repository.Repository, func() error, error)

type driver struct {
	cfg                     *PqConfig
	memberID                string
	repositoryFactoryWoTx   repositoryFactoryWoTx
	repositoryFactoryWithTx repositoryFactoryWithTx
	cancel                  context.CancelFunc
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
	// repo, close, err := d.repositoryFactoryWithTx(ctx, d.cfg.conn, nil)
	// if err != nil {
	// 	return nil
	// }
	// defer close()

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

	return repo.ScheduleJob(ctx, job.ID(), sch.ScheduleType, sch.Schedule, sch.MaxDelay)
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

var _ gotick.SchedulerDriver = &driver{}
var _ gotick.SchedulerSubscriber = &driver{}

func newDriver(cfg *PqConfig, factByConnStr repositoryFactoryWoTx, factByConn repositoryFactoryWithTx) *driver {
	return &driver{
		cfg:                     cfg,
		memberID:                uuid.NewString(),
		repositoryFactoryWoTx:   factByConnStr,
		repositoryFactoryWithTx: factByConn,
		cancel:                  func() {},
	}
}
