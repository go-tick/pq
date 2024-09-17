package pq

import (
	gotick "github.com/go-tick/core"
	"github.com/go-tick/pq/internal/repository"
)

type PqScheduleSerializer func(gotick.JobSchedule) (PqJobSchedule, error)
type PqScheduleDeserializer func(PqJobSchedule) (gotick.JobSchedule, error)

type PqConfig struct {
	conn string

	scheduleSerializer   PqScheduleSerializer
	scheduleDeserializer PqScheduleDeserializer
}

func DefaultPqConfig(options ...gotick.Option[PqConfig]) *PqConfig {
	config := &PqConfig{
		scheduleSerializer:   DefaultScheduleSerializer,
		scheduleDeserializer: DefaultScheduleDeserializer,
	}

	for _, option := range options {
		option(config)
	}

	return config
}

func WithPqDriver(cfg *PqConfig) gotick.Option[gotick.SchedulerConfig] {
	return gotick.WithDriverFactory(func(sc *gotick.SchedulerConfig) gotick.SchedulerDriver {
		driver := newDriver(cfg, repository.NewRepositoryWoTx, repository.NewRepositoryWithTx)
		gotick.WithSubscribers(driver)(sc)
		return driver
	})
}

func WithConn(conn string) gotick.Option[PqConfig] {
	return func(config *PqConfig) {
		config.conn = conn
	}
}

func WithScheduleSerializer(serializer PqScheduleSerializer) gotick.Option[PqConfig] {
	return func(config *PqConfig) {
		config.scheduleSerializer = serializer
	}
}

func WithScheduleDeserializer(deserializer PqScheduleDeserializer) gotick.Option[PqConfig] {
	return func(config *PqConfig) {
		config.scheduleDeserializer = deserializer
	}
}
