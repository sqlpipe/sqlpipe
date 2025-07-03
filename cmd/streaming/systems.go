package main

import (
	"fmt"
	"net/http"
	"time"
)

var (
	Statuses = []string{StatusQueued, StatusRunning, StatusCancelled, StatusError, StatusComplete, ""}

	StatusQueued    = "queued"
	StatusRunning   = "running"
	StatusCancelled = "cancelled"
	StatusError     = "error"
	StatusComplete  = "complete"

	TypePostgreSQL = "postgresql"
	TypeMySQL      = "mysql"
	TypeMSSQL      = "mssql"
	TypeOracle     = "oracle"
	TypeSnowflake  = "snowflake"
	TypeStripe     = "stripe"

	DriverPostgreSQL = "pgx"
	DriverMySQL      = "mysql"
	DriverMSSQL      = "sqlserver"
	DriverOracle     = "oracle"
	DriverSnowflake  = "snowflake"
)

type SystemInfo struct {
	Name               string        `yaml:"name" json:"name"`
	Type               string        `yaml:"type" json:"type"`
	ConnectionString   string        `yaml:"dsn" json:"dsn"`
	MaxOpenConnections int           `yaml:"max_open_connections" json:"max_open_connections"`
	MaxIdleConnections int           `yaml:"max_idle_connections" json:"max_idle_connections"`
	MaxIdleTime        time.Duration `yaml:"max_connection_idle_time" json:"max_connection_idle_time"`
	Hostname           string        `yaml:"hostname,omitempty" json:"hostname,omitempty"`
	Port               int           `yaml:"port,omitempty" json:"port,omitempty"`
	Database           string        `yaml:"database,omitempty" json:"database,omitempty"`
	Username           string        `yaml:"username,omitempty" json:"username,omitempty"`
	Password           string        `yaml:"-" json:"-"`
	Dsn                string        `yaml:"-" json:"-"`
	Route              string        `yaml:"route,omitempty" json:"route,omitempty"`
	ApiKey             string        `yaml:"api_key" json:"-"`
	EndpointSecret     string        `yaml:"-" json:"-"`
	PushFrequency      time.Duration `yaml:"push_frequency" json:"push_frequency"`
	UseCliListener     bool          `yaml:"use_cli_listener,omitempty" json:"use_cli_listener,omitempty"`
}

type System interface {
}

func (app *application) NewSystem(systemInfo SystemInfo, port int, receiveHandlers *map[string]func(http.ResponseWriter, *http.Request)) (system System, err error) {
	// creates a new system

	switch systemInfo.Type {
	case TypePostgreSQL:
		return newPostgresql(systemInfo)
	case TypeSnowflake:
		return newSnowflake(systemInfo)
	case TypeStripe:
		return app.newStripe(systemInfo)
	default:
		return system, fmt.Errorf("unsupported system type %v", systemInfo.Type)
	}
}
