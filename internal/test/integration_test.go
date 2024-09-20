package test

import (
	"context"
	"sync"
	"testing"
	"time"

	gotick "github.com/go-tick/core"
	"github.com/go-tick/pq"
	"github.com/go-tick/pq/internal/repository"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const conn = "host=localhost port=5432 user=postgres password=postgres dbname=gotick sslmode=disable"

type jobWithDelay struct {
	id         gotick.JobID
	delay      time.Duration
	executions []*gotick.JobExecutionContext
	once       sync.Once
	done       chan any
}

type jobFactory struct {
	jobs []*jobWithDelay
}

type errorObserver struct {
	t *testing.T
}

func (j *jobWithDelay) Execute(ctx *gotick.JobExecutionContext) {
	time.Sleep(j.delay)
	j.executions = append(j.executions, ctx)
	j.once.Do(func() { close(j.done) })
}

func (j *jobFactory) Create(jobID gotick.JobID) gotick.Job {
	for _, job := range j.jobs {
		if job.id == jobID {
			return job
		}
	}

	return nil
}

func (e *errorObserver) OnError(err error) {
	require.NoError(e.t, err)
}

var _ gotick.Job = (*jobWithDelay)(nil)
var _ gotick.JobFactory = (*jobFactory)(nil)
var _ gotick.ErrorObserver = (*errorObserver)(nil)

func newJobWithDelay(id gotick.JobID, delay time.Duration) *jobWithDelay {
	return &jobWithDelay{
		id:    id,
		delay: delay,
		done:  make(chan any),
	}
}

func TestJobShouldBeExecutedCorrectly(t *testing.T) {
	type testJobs struct {
		job             *jobWithDelay
		scheduleFactory func() gotick.JobSchedule
	}

	observer := &errorObserver{t}

	data := []struct {
		name            string
		jobs            []testJobs
		plannerCfg      func([]*jobWithDelay) *gotick.PlannerConfig
		driverCfg       func() *pq.PqConfig
		schedulerConfig func(*gotick.PlannerConfig, *pq.PqConfig) *gotick.SchedulerConfig
		deadline        time.Duration
		assertion       func([]testJobs)
	}{
		{
			name: "single calendar job",
			jobs: []testJobs{
				{
					job: newJobWithDelay(gotick.JobID(uuid.NewString()), 0),
					scheduleFactory: func() gotick.JobSchedule {
						return gotick.NewCalendarSchedule(time.Now().Add(1 * time.Second))
					},
				},
			},
			plannerCfg: func(j []*jobWithDelay) *gotick.PlannerConfig {
				return gotick.DefaultPlannerConfig(gotick.WithJobFactory(&jobFactory{j}))
			},
			driverCfg: func() *pq.PqConfig {
				return pq.DefaultPqConfig(
					pq.WithErrorObservers(observer),
					pq.WithConn(conn),
				)
			},
			schedulerConfig: func(pc *gotick.PlannerConfig, pqc *pq.PqConfig) *gotick.SchedulerConfig {
				return gotick.DefaultSchedulerConfig(
					gotick.WithDefaultPlannerFactory(pc),
					pq.WithPqDriver(pqc),
				)
			},
			deadline: 3 * time.Second,
			assertion: func(jf []testJobs) {
				job := jf[0].job
				assert.Len(t, job.executions, 1)

				assert.LessOrEqual(t, job.executions[0].PlannedAt, job.executions[0].StartedAt)
				assert.LessOrEqual(t, job.executions[0].StartedAt, job.executions[0].ExecutedAt)
				assert.Equal(t, gotick.JobExecutionStatusExecuted, job.executions[0].ExecutionStatus)
			},
		},
		{
			name: "single cron job",
			jobs: []testJobs{
				{
					job: newJobWithDelay(gotick.JobID(uuid.NewString()), 0),
					scheduleFactory: func() gotick.JobSchedule {
						c, err := gotick.NewCronSchedule("* * * * *")
						require.NoError(t, err)

						return c
					},
				},
			},
			plannerCfg: func(j []*jobWithDelay) *gotick.PlannerConfig {
				return gotick.DefaultPlannerConfig(gotick.WithJobFactory(&jobFactory{j}))
			},
			driverCfg: func() *pq.PqConfig {
				return pq.DefaultPqConfig(
					pq.WithErrorObservers(observer),
					pq.WithConn(conn),
				)
			},
			schedulerConfig: func(pc *gotick.PlannerConfig, pqc *pq.PqConfig) *gotick.SchedulerConfig {
				return gotick.DefaultSchedulerConfig(
					gotick.WithDefaultPlannerFactory(pc),
					pq.WithPqDriver(pqc),
				)
			},
			deadline: 1*time.Minute + 10*time.Second,
			assertion: func(jf []testJobs) {
				job := jf[0].job
				assert.LessOrEqual(t, 1, len(job.executions))
				assert.GreaterOrEqual(t, 2, len(job.executions))

				assert.LessOrEqual(t, job.executions[0].PlannedAt, job.executions[0].StartedAt)
				assert.LessOrEqual(t, job.executions[0].StartedAt, job.executions[0].ExecutedAt)
				assert.Equal(t, gotick.JobExecutionStatusExecuted, job.executions[0].ExecutionStatus)
			},
		},
		{
			name: "single seq job",
			jobs: []testJobs{
				{
					job: newJobWithDelay("job1", 0),
					scheduleFactory: func() gotick.JobSchedule {
						c, err := gotick.NewSequenceSchedule(
							time.Now().Add(1*time.Second),
							time.Now().Add(2*time.Second),
							time.Now().Add(3*time.Second),
							time.Now().Add(5*time.Second),
							time.Now().Add(15*time.Second),
						)
						require.NoError(t, err)

						return c
					},
				},
			},
			plannerCfg: func(j []*jobWithDelay) *gotick.PlannerConfig {
				return gotick.DefaultPlannerConfig(gotick.WithJobFactory(&jobFactory{j}))
			},
			driverCfg: func() *pq.PqConfig {
				return pq.DefaultPqConfig(
					pq.WithErrorObservers(observer),
					pq.WithConn(conn),
				)
			},
			schedulerConfig: func(pc *gotick.PlannerConfig, pqc *pq.PqConfig) *gotick.SchedulerConfig {
				return gotick.DefaultSchedulerConfig(
					gotick.WithDefaultPlannerFactory(pc),
					pq.WithPqDriver(pqc),
				)
			},
			deadline: 30 * time.Second,
			assertion: func(jf []testJobs) {
				job := jf[0].job
				assert.Len(t, job.executions, 5)
			},
		},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			jobs := make([]*jobWithDelay, 0, len(d.jobs))
			for _, job := range d.jobs {
				jobs = append(jobs, job.job)
			}
			defer func() {
				repo, close, err := repository.NewRepositoryWoTx(context.Background(), conn)
				require.NoError(t, err)
				defer close()

				for _, job := range jobs {
					err := repo.UnscheduleJobByJobID(context.Background(), string(job.id))
					require.NoError(t, err)
				}
			}()

			planerCfg := d.plannerCfg(jobs)
			driverCfg := d.driverCfg()
			schedulerCfg := d.schedulerConfig(planerCfg, driverCfg)

			scheduler, err := gotick.NewScheduler(schedulerCfg)
			require.NoError(t, err)

			ctx := context.Background()

			for _, job := range d.jobs {
				_, err := scheduler.ScheduleJob(ctx, job.job.id, job.scheduleFactory())
				require.NoError(t, err)
			}

			err = scheduler.Start(ctx)
			require.NoError(t, err)

			time.Sleep(d.deadline)

			err = scheduler.Stop()
			require.NoError(t, err)

			if d.assertion != nil {
				d.assertion(d.jobs)
			}
		})
	}
}
