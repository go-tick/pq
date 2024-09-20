package pq

import (
	"testing"
	"time"

	gotick "github.com/go-tick/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSerializeDeserializeShouldWork(t *testing.T) {
	cron, err := gotick.NewCronSchedule("1 * * * *")
	require.NoError(t, err)

	calendar := gotick.NewCalendarSchedule(time.Now())

	seq, err := gotick.NewSequenceSchedule(time.Now(), time.Now().Add(time.Minute))
	require.NoError(t, err)

	expectedDelay := time.Second
	expectedTimeout := 2 * time.Second

	tests := []struct {
		name       string
		schedule   gotick.JobSchedule
		additional func(t *testing.T, schedule gotick.JobSchedule)
	}{
		{
			name:     "cron",
			schedule: cron,
		},
		{
			name:     "calendar",
			schedule: calendar,
			additional: func(t *testing.T, schedule gotick.JobSchedule) {
				assert.Nil(t, schedule.Next(time.Now().Add(1*time.Second)))
			},
		},
		{
			name:     "seq",
			schedule: seq,
			additional: func(t *testing.T, schedule gotick.JobSchedule) {
				expectedNext := seq.Next(seq.First())
				actualNext := schedule.Next(schedule.First())

				assert.Equal(t, expectedNext.UnixMilli(), actualNext.UnixMilli())
			},
		},
		{
			name:     "max_delay",
			schedule: gotick.NewJobScheduleWithMaxDelay(cron, expectedDelay),
			additional: func(t *testing.T, schedule gotick.JobSchedule) {
				md, ok := gotick.MaxDelayFromJobSchedule(schedule)
				assert.True(t, ok)
				assert.Equal(t, expectedDelay, md)
			},
		},
		{
			name:     "timeout",
			schedule: gotick.NewJobScheduleWithTimeout(calendar, expectedTimeout),
			additional: func(t *testing.T, schedule gotick.JobSchedule) {
				tm, ok := gotick.TimeoutFromJobSchedule(schedule)
				assert.True(t, ok)
				assert.Equal(t, expectedTimeout, tm)
			},
		},
		{
			name:     "timeout_max_delay",
			schedule: gotick.NewJobScheduleWithTimeout(gotick.NewJobScheduleWithMaxDelay(seq, expectedDelay), expectedTimeout),
			additional: func(t *testing.T, schedule gotick.JobSchedule) {
				tm, ok := gotick.TimeoutFromJobSchedule(schedule)
				assert.True(t, ok)
				assert.Equal(t, expectedTimeout, tm)

				md, ok := gotick.MaxDelayFromJobSchedule(schedule)
				assert.True(t, ok)
				assert.Equal(t, expectedDelay, md)
			},
		},
		{
			name:     "max_delay_timeout",
			schedule: gotick.NewJobScheduleWithMaxDelay(gotick.NewJobScheduleWithTimeout(calendar, expectedTimeout), expectedDelay),
			additional: func(t *testing.T, schedule gotick.JobSchedule) {
				tm, ok := gotick.TimeoutFromJobSchedule(schedule)
				assert.True(t, ok)
				assert.Equal(t, expectedTimeout, tm)

				md, ok := gotick.MaxDelayFromJobSchedule(schedule)
				assert.True(t, ok)
				assert.Equal(t, expectedDelay, md)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pqSchedule, err := DefaultScheduleSerializer(tt.schedule)
			require.NoError(t, err)

			deserialized, err := DefaultScheduleDeserializer(pqSchedule)
			require.NoError(t, err)

			assert.Equal(t, tt.schedule.Schedule(), deserialized.Schedule())
			assert.Equal(t, tt.schedule.First().UnixMilli(), deserialized.First().UnixMilli())

			if tt.additional != nil {
				tt.additional(t, deserialized)
			}
		})
	}
}
