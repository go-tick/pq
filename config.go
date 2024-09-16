package pq

import gotick "github.com/go-tick/core"

type PqConfig struct {
	host    string
	port    int
	user    string
	pass    string
	dbName  string
	sslMode string
}

func DefaultPqConfig(options ...gotick.Option[PqConfig]) *PqConfig {
	config := &PqConfig{
		host:    "localhost",
		port:    5432,
		user:    "postgres",
		pass:    "",
		dbName:  "gotick",
		sslMode: "disable",
	}

	for _, option := range options {
		option(config)
	}

	return config
}

func WithHost(host string) gotick.Option[PqConfig] {
	return func(c *PqConfig) {
		c.host = host
	}
}

func WithPort(port int) gotick.Option[PqConfig] {
	return func(c *PqConfig) {
		c.port = port
	}
}

func WithUser(user string) gotick.Option[PqConfig] {
	return func(c *PqConfig) {
		c.user = user
	}
}

func WithPass(pass string) gotick.Option[PqConfig] {
	return func(c *PqConfig) {
		c.pass = pass
	}
}

func WithDbName(dbName string) gotick.Option[PqConfig] {
	return func(c *PqConfig) {
		c.dbName = dbName
	}
}

func WithSslMode(sslMode string) gotick.Option[PqConfig] {
	return func(c *PqConfig) {
		c.sslMode = sslMode
	}
}
