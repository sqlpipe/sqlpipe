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

type Field struct {
	Field     string `yaml:"field" json:"field"`
	SearchKey bool   `yaml:"search_key,omitempty" json:"search_key,omitempty"`
	Hardcode  any    `yaml:"hardcode,omitempty" json:"hardcode,omitempty"`
}

type PullObject map[string]Field
type PullLocation map[string]PullObject
type ReceiveRouter map[string]PullLocation

type PushRouter map[string]PushObject
type PushObject map[string]PushLocation
type PushLocation map[string]Field

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
	ReplicationDsn     string        `yaml:"replication_dsn,omitempty" json:"replication_dsn,omitempty"`
	ApiKey             string        `yaml:"api_key" json:"-"`
	EndpointSecret     string        `yaml:"-" json:"-"`
	RateLimit          int           `yaml:"rate_limit,omitempty" json:"rate_limit,omitempty"`
	RateBucketSize     int           `yaml:"rate_bucket_size,omitempty" json:"rate_bucket_size,omitempty"`
	UseCliListener     bool          `yaml:"use_cli_listener,omitempty" json:"use_cli_listener,omitempty"`
	ReceiveRouter      ReceiveRouter `yaml:"receive_router,omitempty" json:"receive_router,omitempty"`
	PushRouter         PushRouter    `yaml:"push_router,omitempty" json:"push_router,omitempty"`
}

type SystemInterface interface {
	handleWebhook(w http.ResponseWriter, r *http.Request)
	// getFieldMap() map[string]string
	// createModels(obj map[string]interface{}) (map[string]interface{}, error)
}

func (app *application) NewSystem(systemInfo SystemInfo, port int, duplicateChecker map[string][]ExpiringObject) (system SystemInterface, err error) {
	switch systemInfo.Type {
	case TypePostgreSQL:
		return app.newPostgresql(systemInfo, duplicateChecker)
	case TypeSnowflake:
		return newSnowflake(systemInfo)
	case TypeStripe:
		return app.newStripe(systemInfo, duplicateChecker)
	default:
		return system, fmt.Errorf("unsupported system type %v", systemInfo.Type)
	}
}
