package data

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/calmitchell617/sqlpipe/internal/validator"
)

type Query struct {
	ID              int64      `json:"id"`
	CreatedAt       time.Time  `json:"createdAt"`
	CreatedBy       int64      `json:"createdBy"`
	ConnectionID    int64      `json:"connectionId"`
	Connection      Connection `json:"-"`
	Query           string     `json:"query"`
	Status          string     `json:"status"`
	Error           string     `json:"error"`
	ErrorProperties string     `json:"errorProperties"`
	StoppedAt       time.Time  `json:"stoppedAt"`
	Version         int        `json:"version"`
}

type QueryModel struct {
	DB *sql.DB
}

func (m QueryModel) Insert(query *Query) (*Query, error) {
	queryToRun := `
        INSERT INTO queries (created_by, connection_id, query, stopped_at) 
        VALUES ($1, $2, $3, $4)
        RETURNING id, created_at, status, version`

	args := []interface{}{
		query.CreatedBy,
		query.ConnectionID,
		query.Query,
		query.StoppedAt,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	err := m.DB.QueryRowContext(ctx, queryToRun, args...).Scan(&query.ID, &query.CreatedAt, &query.Status, &query.Version)
	if err != nil {
		return query, err
	}

	return query, nil
}

func ValidateQuery(v *validator.Validator, query *Query) {
	v.Check(query.ConnectionID > 0, "connectionId", "Connection ID is required and must be an integer greater than 0")
	v.Check(query.Query != "", "query", "A query is required")
	if query.CreatedBy == 0 {
		panic("you shouldn't be able to create or modify a query without an authenticated user, exiting program")
	}
}

func (m QueryModel) GetAll(filters Filters) ([]*Query, Metadata, error) {
	queryToRun := fmt.Sprintf(`
	SELECT
	count(*) OVER(),
	queries.id,
	queries.created_at,
	queries.created_by,
	queries.connection_id,
	connections.name,
	connections.ds_type,
	connections.hostname,
	connections.account_id,
	connections.db_name,
	queries.query,
	queries.status,
	queries.error,
	queries.error_properties,
	queries.stopped_at,
	queries.version
FROM
	queries
left join
	connections
on
	queries.connection_id = connections.id
order by
	%s %s,
	id asc
limit
	$1
offset
	$2
`, filters.sortColumn(), filters.sortDirection())

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	args := []interface{}{filters.limit(), filters.offset()}

	rows, err := m.DB.QueryContext(ctx, queryToRun, args...)
	if err != nil {
		return nil, Metadata{}, err
	}

	defer rows.Close()

	totalRecords := 0
	queries := []*Query{}

	for rows.Next() {
		var query Query

		err := rows.Scan(
			&totalRecords,
			&query.ID,
			&query.CreatedAt,
			&query.CreatedBy,
			&query.ConnectionID,
			&query.Connection.Name,
			&query.Connection.DsType,
			&query.Connection.Hostname,
			&query.Connection.AccountId,
			&query.Connection.DbName,
			&query.Query,
			&query.Status,
			&query.Error,
			&query.ErrorProperties,
			&query.StoppedAt,
			&query.Version,
		)
		if err != nil {
			return nil, Metadata{}, err
		}

		queries = append(queries, &query)
	}

	if err = rows.Err(); err != nil {
		return nil, Metadata{}, err
	}

	metadata := calculateMetadata(totalRecords, filters.Page, filters.PageSize)

	return queries, metadata, nil
}

func (m QueryModel) GetQueued() ([]*Query, error) {
	queryToRun := `
	SELECT
	queries.id,
	queries.created_at,
	queries.created_by,
	connections.ID,
	connections.Ds_Type,
	connections.Hostname,
	connections.Port,
	connections.Account_Id,
	connections.Db_Name,
	connections.Username,
	connections.Password,
	queries.query,
	queries.status,
	queries.error,
	queries.error_properties,
	queries.stopped_at,
	queries.version
FROM
	queries
left join
	connections
on
	queries.connection_id = connections.id
where
	status = 'queued'
order by
	queries.id
`

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	rows, err := m.DB.QueryContext(ctx, queryToRun)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	queries := []*Query{}

	for rows.Next() {
		var query Query

		err := rows.Scan(
			&query.ID,
			&query.CreatedAt,
			&query.CreatedBy,
			&query.Connection.ID,
			&query.Connection.DsType,
			&query.Connection.Hostname,
			&query.Connection.Port,
			&query.Connection.AccountId,
			&query.Connection.DbName,
			&query.Connection.Username,
			&query.Connection.Password,
			&query.Query,
			&query.Status,
			&query.Error,
			&query.ErrorProperties,
			&query.StoppedAt,
			&query.Version,
		)
		if err != nil {
			return nil, err
		}

		queries = append(queries, &query)
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	return queries, nil
}

func (m QueryModel) GetById(id int64) (*Query, error) {
	queryToRun := `
	SELECT
	queries.id,
	queries.created_at,
	queries.created_by,
	queries.connection_id,
	connections.name,
	connections.ds_type,
	connections.hostname,
	connections.account_id,
	connections.db_name,
	queries.query,
	queries.status,
	queries.error,
	queries.error_properties,
	queries.stopped_at,
	queries.version
FROM
	queries
left join
	connections
on
	queries.connection_id = connections.id
where
	queries.id = $1
`

	var query Query

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	err := m.DB.QueryRowContext(ctx, queryToRun, id).Scan(
		&query.ID,
		&query.CreatedAt,
		&query.CreatedBy,
		&query.ConnectionID,
		&query.Connection.Name,
		&query.Connection.DsType,
		&query.Connection.Hostname,
		&query.Connection.AccountId,
		&query.Connection.DbName,
		&query.Query,
		&query.Status,
		&query.Error,
		&query.ErrorProperties,
		&query.StoppedAt,
		&query.Version,
	)

	if err != nil {
		switch {
		case errors.Is(err, sql.ErrNoRows):
			return nil, ErrRecordNotFound
		default:
			return nil, err
		}
	}

	return &query, nil
}

func (m QueryModel) Update(query *Query) error {
	queryToRun := `
        UPDATE queries 
        SET status = $1, error = $2, error_properties = $3, stopped_at = $4, version = version + 1
        WHERE id = $5 AND version = $6
        RETURNING version`

	args := []interface{}{
		&query.Status,
		&query.Error,
		&query.ErrorProperties,
		&query.StoppedAt,
		&query.ID,
		&query.Version,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	err := m.DB.QueryRowContext(ctx, queryToRun, args...).Scan(&query.Version)
	if err != nil {
		switch {
		case errors.Is(err, sql.ErrNoRows):
			return ErrEditConflict
		default:
			return err
		}
	}

	return nil
}

func (m QueryModel) Delete(id int64) error {
	if id < 1 {
		return ErrRecordNotFound
	}

	queryToRun := `
			DELETE FROM queries
			WHERE id = $1`

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	result, err := m.DB.ExecContext(ctx, queryToRun, id)
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
