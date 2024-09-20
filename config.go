package pq

import (
	"time"

	gotick "github.com/go-tick/core"
	"github.com/go-tick/pq/internal/repository"
)

type PqScheduleSerializer func(gotick.JobSchedule) (PqJobSchedule, error)
type PqScheduleDeserializer func(PqJobSchedule) (gotick.JobSchedule, error)

type PqConfig struct {
	conn        string
	lockTimeout time.Duration
	batchSize   uint

	scheduleSerializer   PqScheduleSerializer
	scheduleDeserializer PqScheduleDeserializer

	errorObservers []gotick.ErrorObserver
}

func DefaultPqConfig(options ...gotick.Option[PqConfig]) *PqConfig {
	config := &PqConfig{
		lockTimeout: 1 * time.Hour,
		batchSize:   10,

		scheduleSerializer:   DefaultScheduleSerializer,
		scheduleDeserializer: DefaultScheduleDeserializer,
	}

	for _, option := range options {
		option(config)
	}

	return config
}

func WithPqDriver(cfg *PqConfig) gotick.Option[gotick.SchedulerConfig] {
	return gotick.WithDriverFactory(func(sc *gotick.SchedulerConfig) (gotick.SchedulerDriver, error) {
		driver := newDriver(cfg, repository.NewRepositoryWoTx, repository.NewRepositoryWithTx)
		gotick.WithSubscribers(driver)(sc)
		return driver, nil
	})
}

func WithLockTimeout(timeout time.Duration) gotick.Option[PqConfig] {
	return func(config *PqConfig) {
		config.lockTimeout = timeout
	}
}

func WithBatchSize(size uint) gotick.Option[PqConfig] {
	return func(config *PqConfig) {
		config.batchSize = size
	}
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

func WithErrorObservers(observers ...gotick.ErrorObserver) gotick.Option[PqConfig] {
	return func(config *PqConfig) {
		config.errorObservers = append(config.errorObservers, observers...)
	}
}
