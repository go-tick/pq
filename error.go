package pq

import "fmt"

var (
	ErrCannotParseSchedule = fmt.Errorf("cannot parse schedule")
	ErrUnknownScheduleType = fmt.Errorf("unknown schedule type")
)
