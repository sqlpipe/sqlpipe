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
	ID        int64     `json:"id"`
	CreatedAt time.Time `json:"createdAt"`
	Name      string    `json:"name"`
	DsType    string    `json:"dsType"`
	Username  string    `json:"username"`
	Password  string    `json:"-"`
	AccountId string    `json:"accountID"`
	Hostname  string    `json:"hostname"`
	Port      int       `json:"port"`
	DbName    string    `json:"dbName"`
	Version   int       `json:"-"`
	// CanConnect does not go in the DB, it is kept in memory to show in the UI / API responses
	CanConnect bool `json:"canConnect"`
}

type ConnectionModel struct {
	DB *sql.DB
}

func (m ConnectionModel) Insert(connection *Connection) (*Connection, error) {
	query := `
        INSERT INTO connections (name, ds_type, username, password, account_id, hostname, port, db_name) 
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        RETURNING id, created_at, version`

	args := []interface{}{
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

	err := m.DB.QueryRowContext(ctx, query, args...).Scan(&connection.ID, &connection.CreatedAt, &connection.Version)
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
        SELECT count(*) OVER(), id, created_at, name, ds_type, username, password, account_id, hostname, port, db_name, version
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
			&connection.Name,
			&connection.DsType,
			&connection.Username,
			&connection.Password,
			&connection.AccountId,
			&connection.Hostname,
			&connection.Port,
			&connection.DbName,
			&connection.Version,
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
        SELECT id, created_at, name, ds_type, username, password, account_id, hostname, port, db_name, version
        FROM connections
        WHERE id = $1`

	var connection Connection

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	err := m.DB.QueryRowContext(ctx, query, id).Scan(
		&connection.ID,
		&connection.CreatedAt,
		&connection.Name,
		&connection.DsType,
		&connection.Username,
		&connection.Password,
		&connection.AccountId,
		&connection.Hostname,
		&connection.Port,
		&connection.DbName,
		&connection.Version,
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
        SET name = $1, ds_type = $2, username = $3, password = $4, account_id = $5, hostname = $6, port = $7, db_name = $8, version = version + 1
        WHERE id = $9 AND version = $10
        RETURNING version`

	args := []interface{}{
		connection.Name,
		connection.DsType,
		connection.Username,
		connection.Password,
		connection.AccountId,
		connection.Hostname,
		connection.Port,
		connection.DbName,
		connection.ID,
		connection.Version,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	err := m.DB.QueryRowContext(ctx, query, args...).Scan(&connection.Version)
	if err != nil {
		switch {
		case err.Error() == `pq: duplicate key value violates unique constraint "connections_name_key"`:
			return ErrDuplicateConnectionName
		case errors.Is(err, sql.ErrNoRows):
			return ErrEditConflict
		default:
			return err
		}
	}

	return nil
}

func (m ConnectionModel) Delete(id int64) error {
	if id < 1 {
		return ErrRecordNotFound
	}

	query := `
			DELETE FROM connections
			WHERE id = $1`

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	result, err := m.DB.ExecContext(ctx, query, id)
	if err != nil {
		return err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return err
	}

	if rowsAffected == 0 {
		return ErrRecordNotFound
	}

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
