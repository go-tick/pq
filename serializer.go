package pq

import (
	"encoding/json"
	"strings"
	"time"

	gotick "github.com/go-tick/core"
	"github.com/robfig/cron/v3"
)

type PqJobSchedule struct {
	ScheduleType string
	Schedule     string
	Metadata     []byte
}

func DefaultScheduleSerializer(schedule gotick.JobSchedule) (PqJobSchedule, error) {
	s := schedule.Schedule()
	metadata := make(map[string]any)

	if md, ok := gotick.MaxDelayFromJobSchedule(schedule); ok {
		metadata["max_delay"] = md
	}

	if tm, ok := gotick.TimeoutFromJobSchedule(schedule); ok {
		metadata["timeout"] = tm
	}

	mdata, err := json.Marshal(metadata)
	if err != nil {
		return PqJobSchedule{}, err
	}

	if _, err := cron.ParseStandard(s); err == nil {
		// it's cron
		return PqJobSchedule{
			ScheduleType: "cron",
			Schedule:     s,
			Metadata:     mdata,
		}, nil
	}

	if strings.Contains(s, ",") {
		// it's seq
		for _, ts := range strings.Split(s, ",") {
			if _, err := time.Parse(time.RFC3339Nano, ts); err != nil {
				return PqJobSchedule{}, ErrCannotParseSchedule
			}
		}

		return PqJobSchedule{
			ScheduleType: "seq",
			Schedule:     s,
			Metadata:     mdata,
		}, nil
	}

	if _, err := time.Parse(time.RFC3339Nano, s); err != nil {
		return PqJobSchedule{}, ErrCannotParseSchedule
	}

	// it's calendar
	return PqJobSchedule{
		ScheduleType: "calendar",
		Schedule:     s,
		Metadata:     mdata,
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
		for _, ts := range strings.Split(schedule.Schedule, ",") {
			t, err := time.Parse(time.RFC3339Nano, ts)
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
		t, err := time.Parse(time.RFC3339Nano, schedule.Schedule)
		if err != nil {
			return nil, err
		}

		result = gotick.NewCalendarSchedule(t)
	default:
		return nil, ErrUnknownScheduleType
	}

	if schedule.Metadata != nil {
		var metadata map[string]any
		if err := json.Unmarshal(schedule.Metadata, &metadata); err != nil {
			return nil, err
		}

		if md, ok := metadata["max_delay"]; ok {
			if delay, ok := md.(float64); ok {
				result = gotick.NewJobScheduleWithMaxDelay(result, time.Duration(delay))
			}
		}

		if tm, ok := metadata["timeout"]; ok {
			if timeout, ok := tm.(float64); ok {
				result = gotick.NewJobScheduleWithTimeout(result, time.Duration(timeout))
			}
		}
	}

	return result, err
}
