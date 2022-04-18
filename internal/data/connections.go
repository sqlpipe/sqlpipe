package data

import (
	"database/sql"
	"errors"
	"time"

	"github.com/sqlpipe/sqlpipe/internal/validator"
)

var (
	ErrDuplicateConnectionName = errors.New("duplicate connection name")
)

type Connection struct {
	ID         int64     `json:"id"`
	CreatedAt  time.Time `json:"createdAt"`
	Name       string    `json:"name"`
	DsType     string    `json:"dsType"`
	DriverName string    `json:"driverName"`
	Username   string    `json:"username"`
	Password   string    `json:"-"`
	AccountId  string    `json:"accountID"`
	Hostname   string    `json:"hostname"`
	Port       int       `json:"port"`
	DbName     string    `json:"dbName"`
}

type ConnectionModel struct {
	DB *sql.DB
}

func (m ConnectionModel) Insert(connection *Connection) (*Connection, error) {
	return connection, nil
}

func (m ConnectionModel) GetAll(filters Filters) ([]*Connection, Metadata, error) {
	return []*Connection{}, Metadata{}, nil
}

func (m ConnectionModel) GetById(id int64) (*Connection, error) {
	return &Connection{}, nil
}

func (m ConnectionModel) Update(connection *Connection) error {
	return nil
}

func (m ConnectionModel) Delete(id int64) error {
	return nil
}

func ValidateConnection(v *validator.Validator, connection *Connection) {
	v.Check(connection.Username != "", "username", "A username is required")
	v.Check(connection.Password != "", "password", "A password is required")
	v.Check(connection.DbName != "", "dbName", "A DB name is required")
	v.Check(connection.Name != "", "name", "A connection name is required")

	switch connection.DsType {
	case "snowflake":
		v.Check(connection.Hostname == "", "hostname", "Do not enter a Hostname if you are configuring a Snowflake connection")
		v.Check(connection.Port == 0, "port", "Do not enter a port if you are configuring a Snowflake connection")
		v.Check(connection.AccountId != "", "accountId", "You must enter an account ID if configuring a snowflake connection")
	case "":
		v.Check(connection.DsType != "", "dsType", "You must select a data system type")
	default:
		v.Check(connection.Hostname != "", "hostname", "You must enter a Hostname")
		v.Check(connection.Port != 0, "port", "You must enter a port number")
		v.Check(connection.AccountId == "", "accountId", "Do not enter an account ID unless you are configuring a snowflake connection")
	}
}
