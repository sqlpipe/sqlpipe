package data

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/calmitchell617/sqlpipe/internal/validator"
)

var (
	ErrDuplicateConnectionName = errors.New("duplicate connection name")
)

type Connection struct {
	ID             int64     `json:"id"`
	Active         bool      `json:"active"`
	CreatedAt      time.Time `json:"createdAt"`
	LastModifiedBy int64     `json:"lastModifiedBy"`
	Name           string    `json:"name"`
	DsType         string    `json:"dsType"`
	Username       string    `json:"username"`
	Password       string    `json:"-"`
	AccountId      string    `json:"accountID"`
	Hostname       string    `json:"hostname"`
	Port           int       `json:"port"`
	DbName         string    `json:"dbName"`
	// CanConnect does not go in the DB, it is kept in memory to show in the UI / API responses
	CanConnect bool `json:"canConnect"`
}

type ConnectionModel struct {
	DB *sql.DB
}

func (m ConnectionModel) Insert(connection *Connection) (*Connection, error) {
	query := `
        INSERT INTO connections (last_modified_by, name, ds_type, username, password, account_id, hostname, port, db_name) 
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        RETURNING id, created_at`

	args := []interface{}{
		connection.LastModifiedBy,
		connection.Name,
		connection.DsType,
		connection.Username,
		connection.Password,
		connection.AccountId,
		connection.Hostname,
		connection.Port,
		connection.DbName,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	err := m.DB.QueryRowContext(ctx, query, args...).Scan(&connection.ID, &connection.CreatedAt)
	if err != nil {
		switch {
		case err.Error() == `pq: duplicate key value violates unique constraint "connections_name_key"`:
			return connection, ErrDuplicateConnectionName
		default:
			return connection, err
		}
	}

	return connection, nil
}

func (m ConnectionModel) GetAll(filters Filters) ([]*Connection, Metadata, error) {
	query := fmt.Sprintf(`
        SELECT count(*) OVER(), id, created_at, last_modified_by, name, ds_type, username, password, account_id, hostname, port, db_name
        FROM connections
        ORDER BY %s %s, id ASC
        LIMIT $1 OFFSET $2`, filters.sortColumn(), filters.sortDirection())

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	args := []interface{}{filters.limit(), filters.offset()}

	rows, err := m.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, Metadata{}, err
	}

	defer rows.Close()

	totalRecords := 0
	connections := []*Connection{}

	for rows.Next() {
		var connection Connection

		err := rows.Scan(
			&totalRecords,
			&connection.ID,
			&connection.CreatedAt,
			&connection.LastModifiedBy,
			&connection.Name,
			&connection.DsType,
			&connection.Username,
			&connection.Password,
			&connection.AccountId,
			&connection.Hostname,
			&connection.Port,
			&connection.DbName,
		)
		if err != nil {
			return nil, Metadata{}, err
		}

		connections = append(connections, &connection)
	}

	if err = rows.Err(); err != nil {
		return nil, Metadata{}, err
	}

	metadata := calculateMetadata(totalRecords, filters.Page, filters.PageSize)

	return connections, metadata, nil
}

func (m ConnectionModel) GetById(id int64) (*Connection, error) {
	query := `
        SELECT id, created_at, last_modified_by, name, ds_type, username, password, account_id, hostname, port, db_name
        FROM connections
        WHERE id = $1`

	var connection Connection

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	err := m.DB.QueryRowContext(ctx, query, id).Scan(
		&connection.ID,
		&connection.CreatedAt,
		&connection.LastModifiedBy,
		&connection.Name,
		&connection.DsType,
		&connection.Username,
		&connection.Password,
		&connection.AccountId,
		&connection.Hostname,
		&connection.Port,
		&connection.DbName,
	)

	if err != nil {
		switch {
		case errors.Is(err, sql.ErrNoRows):
			return nil, ErrRecordNotFound
		default:
			return nil, err
		}
	}

	return &connection, nil
}

func (m ConnectionModel) Update(connection *Connection) error {
	query := `
        UPDATE connections 
        SET name = $1, ds_type = $2, username = $3, password = $4, account_id = $5, hostname = $6, port = $7, db_name = $8, last_modified_by = $9
        WHERE id = $10`

	args := []interface{}{
		connection.Name,
		connection.DsType,
		connection.Username,
		connection.Password,
		connection.AccountId,
		connection.Hostname,
		connection.Port,
		connection.DbName,
		connection.LastModifiedBy,
		connection.ID,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	err := m.DB.QueryRowContext(ctx, query, args...).Scan()
	if err != nil {
		switch {
		case errors.Is(err, sql.ErrNoRows):
			return ErrRecordNotFound
		case err.Error() == `pq: duplicate key value violates unique constraint "connections_name_key"`:
			return ErrDuplicateConnectionName
		default:
			return err
		}
	}

	return nil
}

func (m ConnectionModel) Deactivate(connection *Connection) error {
	if connection.ID < 1 {
		return ErrRecordNotFound
	}

	query := `
		UPDATE connections 
		SET active = false
		WHERE id = $1
	`

	args := []interface{}{
		connection.ID,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	err := m.DB.QueryRowContext(ctx, query, args...).Scan()
	if err != nil {
		switch {
		case errors.Is(err, sql.ErrNoRows):
			return ErrRecordNotFound
		default:
			return err
		}
	}

	return nil
}

func ValidateConnection(v *validator.Validator, connection *Connection) {
	v.Check(connection.Username != "", "username", "A username is required")
	v.Check(connection.Password != "", "password", "A password is required")
	v.Check(connection.DbName != "", "dbName", "A DB name is required")
	v.Check(connection.Name != "", "name", "A connection name is required")
	if connection.LastModifiedBy == 0 {
		panic("you shouldn't be able to create or modify a connection without an authenticated user, exiting program")
	}

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
