package pq

import (
	"context"
	"testing"
	"time"

	gotick "github.com/go-tick/core"
	"github.com/go-tick/pq/internal/repository"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const conn = "host=localhost port=5432 user=postgres password=postgres dbname=gotick sslmode=disable"

type job struct {
	id string
}

func (j *job) Execute(*gotick.JobExecutionContext) {
}

func (j *job) ID() string {
	return j.id
}

var _ gotick.Job = (*job)(nil)

var newJob = func(id string) gotick.Job {
	return &job{id}
}

func TestRepositoryScheduleJobShouldDoIt(t *testing.T) {
	cron, err := gotick.NewCronSchedule("1 * * * *")
	require.NoError(t, err)

	calendar := gotick.NewCalendarSchedule(time.Now())

	seq, err := gotick.NewSequenceSchedule(time.Now(), time.Now().Add(time.Minute))
	require.NoError(t, err)

	tests := []struct {
		name     string
		schedule gotick.JobSchedule
	}{
		{
			name:     "cron",
			schedule: cron,
		},
		{
			name:     "calendar",
			schedule: calendar,
		},
		{
			name:     "seq",
			schedule: seq,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultPqConfig(WithConn(conn))
			driver := newDriver(cfg, repository.NewRepositoryWoTx, repository.NewRepositoryWithTx)

			scheduleID, err := driver.ScheduleJob(context.Background(), newJob(uuid.NewString()), tt.schedule)
			require.NoError(t, err)
			defer func() {
				err := driver.UnscheduleJobByScheduleID(context.Background(), scheduleID)
				assert.NoError(t, err)
			}()

			assert.NotEmpty(t, scheduleID)
		})
	}
}
