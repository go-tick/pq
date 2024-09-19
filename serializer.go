package pq

import (
	"strings"
	"time"

	gotick "github.com/go-tick/core"
	"github.com/robfig/cron/v3"
)

type PqJobSchedule struct {
	ScheduleType string
	Schedule     string
	MaxDelay     *time.Duration
}

func DefaultScheduleSerializer(schedule gotick.JobSchedule) (PqJobSchedule, error) {
	s := schedule.Schedule()

	var maxDelay *time.Duration
	if mds, ok := schedule.(gotick.MaxDelay); ok {
		md := mds.MaxDelay()
		maxDelay = &md
	}

	if _, err := cron.ParseStandard(s); err == nil {
		// it's cron
		return PqJobSchedule{
			ScheduleType: "cron",
			Schedule:     s,
			MaxDelay:     maxDelay,
		}, nil
	}

	if strings.Contains(s, ",") {
		// it's seq
		for _, ts := range strings.Split(s, ",") {
			if _, err := time.Parse(time.RFC3339, ts); err != nil {
				return PqJobSchedule{}, ErrCannotParseSchedule
			}
		}

		return PqJobSchedule{
			ScheduleType: "seq",
			Schedule:     s,
			MaxDelay:     maxDelay,
		}, nil
	}

	if _, err := time.Parse(time.RFC3339, s); err != nil {
		return PqJobSchedule{}, ErrCannotParseSchedule
	}

	// it's calendar
	return PqJobSchedule{
		ScheduleType: "calendar",
		Schedule:     s,
		MaxDelay:     maxDelay,
	}, nil
}

func DefaultScheduleDeserializer(schedule PqJobSchedule) (gotick.JobSchedule, error) {
	var result gotick.JobSchedule
	var err error

	switch schedule.ScheduleType {
	case "cron":
		result, err = gotick.NewCronSchedule(schedule.Schedule)
		if err != nil {
			return nil, err
		}
	case "seq":
		tt := make([]time.Time, 0)
		for _, ts := range strings.Split(",", schedule.Schedule) {
			t, err := time.Parse(time.RFC3339, ts)
			if err != nil {
				return nil, err
			}

			tt = append(tt, t)
		}

		result, err = gotick.NewSequenceSchedule(tt...)
		if err != nil {
			return nil, err
		}
	case "calendar":
		t, err := time.Parse(time.RFC3339, schedule.Schedule)
		if err != nil {
			return nil, err
		}

		result = gotick.NewCalendarSchedule(t)
	default:
		return nil, ErrUnknownScheduleType
	}

	if schedule.MaxDelay != nil {
		result = gotick.NewJobScheduleWithMaxDelay(result, *schedule.MaxDelay)
	}

	return result, err
}
