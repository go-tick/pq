package pq

import (
	"context"

	"github.com/misikdmytro/gotick"
)

type driver struct {
}

func (d *driver) OnBeforeJobExecution(*gotick.JobExecutionContext) {
	panic("unimplemented")
}

func (d *driver) OnBeforeJobExecutionPlanned(*gotick.JobExecutionContext) {
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

func (d *driver) OnJobExecutionNotPlanned(*gotick.JobExecutionContext) {
	panic("unimplemented")
}

func (d *driver) OnJobExecutionSkipped(*gotick.JobExecutionContext) {
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
	panic("unimplemented")
}

func (d *driver) UnscheduleJobByJobID(ctx context.Context, jobID string) error {
	panic("unimplemented")
}

func (d *driver) UnscheduleJobByScheduleID(ctx context.Context, scheduleID string) error {
	panic("unimplemented")
}

var _ gotick.SchedulerDriver = &driver{}
var _ gotick.SchedulerSubscriber = &driver{}
