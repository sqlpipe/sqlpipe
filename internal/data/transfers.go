package data

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/calmitchell617/sqlpipe/internal/validator"
)

type Transfer struct {
	ID              int64      `json:"id"`
	CreatedAt       time.Time  `json:"createdAt"`
	CreatedBy       int64      `json:"createdBy"`
	SourceID        int64      `json:"sourceID"`
	Source          Connection `json:"-"`
	TargetID        int64      `json:"targetID"`
	Target          Connection `json:"-"`
	Query           string     `json:"query"`
	TargetSchema    string     `json:"targetSchema"`
	TargetTable     string     `json:"targetTable"`
	Overwrite       bool       `json:"overwrite"`
	Status          string     `json:"status"`
	Error           string     `json:"error"`
	ErrorProperties string     `json:"errorProperties"`
	StoppedAt       time.Time  `json:"stoppedAt"`
	Version         int        `json:"version"`
}

type TransferModel struct {
	DB *sql.DB
}

func (m TransferModel) Insert(transfer *Transfer) (*Transfer, error) {
	query := `
        INSERT INTO transfers (source_id, target_id, query, target_schema, target_table, overwrite, stopped_at, created_by) 
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        RETURNING id, created_at, status, version`

	args := []interface{}{
		transfer.SourceID,
		transfer.TargetID,
		transfer.Query,
		transfer.TargetSchema,
		transfer.TargetTable,
		transfer.Overwrite,
		transfer.StoppedAt,
		transfer.CreatedBy,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	err := m.DB.QueryRowContext(ctx, query, args...).Scan(&transfer.ID, &transfer.CreatedAt, &transfer.Status, &transfer.Version)
	if err != nil {
		return transfer, err
	}

	return transfer, nil
}

func ValidateTransfer(v *validator.Validator, transfer *Transfer) {
	v.Check(transfer.SourceID > 0, "sourceId", "Source ID is required and must be an integer greater than 0")
	v.Check(transfer.TargetID > 0, "targetId", "Source ID is required and must be an integer greater than 0")
	v.Check(transfer.Query != "", "query", "A query is required")
	v.Check(transfer.TargetTable != "", "targetTable", "A target table is required")
	if transfer.CreatedBy == 0 {
		panic("you shouldn't be able to create or modify a transfer without an authenticated user, exiting program")
	}
}

func (m TransferModel) CountTransfers() (int, error) {
	query := `select count(*) from transfers`

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var numTransfers int

	err := m.DB.QueryRowContext(ctx, query).Scan(&numTransfers)
	if err != nil {
		return 0, err
	}

	return numTransfers, nil
}

func (m TransferModel) GetAll(filters Filters) ([]*Transfer, Metadata, error) {
	query := fmt.Sprintf(`
	SELECT
	count(*) OVER(),
	transfers.id,
	transfers.created_at,
	transfers.created_by,
	transfers.source_id,
	source.name,
	source.ds_type,
	source.account_id,
	source.db_name,
	transfers.target_id,
	target.name,
	target.ds_type,
	target.account_id,
	target.db_name,
	transfers.query,
	transfers.target_schema,
	transfers.target_table,
	transfers.overwrite,
	transfers.status,
	transfers.error,
	transfers.error_properties,
	transfers.stopped_at,
	transfers.version
FROM
	transfers
left join
	connections source
on
	transfers.source_id = source.id
left join
	connections target
on
	transfers.target_id = target.id
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

	rows, err := m.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, Metadata{}, err
	}

	defer rows.Close()

	totalRecords := 0
	transfers := []*Transfer{}

	for rows.Next() {
		var transfer Transfer

		err := rows.Scan(
			&totalRecords,
			&transfer.ID,
			&transfer.CreatedAt,
			&transfer.CreatedBy,
			&transfer.SourceID,
			&transfer.Source.Name,
			&transfer.Source.DsType,
			&transfer.Source.AccountId,
			&transfer.Source.DbName,
			&transfer.TargetID,
			&transfer.Target.Name,
			&transfer.Target.DsType,
			&transfer.Target.AccountId,
			&transfer.Target.DbName,
			&transfer.Query,
			&transfer.TargetSchema,
			&transfer.TargetTable,
			&transfer.Overwrite,
			&transfer.Status,
			&transfer.Error,
			&transfer.ErrorProperties,
			&transfer.StoppedAt,
			&transfer.Version,
		)
		if err != nil {
			return nil, Metadata{}, err
		}

		transfers = append(transfers, &transfer)
	}

	if err = rows.Err(); err != nil {
		return nil, Metadata{}, err
	}

	metadata := calculateMetadata(totalRecords, filters.Page, filters.PageSize)

	return transfers, metadata, nil
}

func (m TransferModel) GetQueued() ([]*Transfer, error) {
	query := `
	SELECT
	transfers.id,
	transfers.created_at,
	transfers.created_by,
	source.ID,
	source.Ds_Type,
	source.Hostname,
	source.Port,
	source.Account_Id,
	source.Db_Name,
	source.Username,
	source.Password,
	target.ID,
	target.Ds_Type,
	target.Hostname,
	target.Port,
	target.Account_Id,
	target.Db_Name,
	target.Username,
	target.Password,
	transfers.query,
	transfers.target_schema,
	transfers.target_table,
	transfers.overwrite,
	transfers.version
FROM
	transfers
left join
	connections source
on
	transfers.source_id = source.id
left join
	connections target
on
	transfers.target_id = target.id
where 
	transfers.status = 'queued'
order by 
	transfers.id
`

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	rows, err := m.DB.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	transfers := []*Transfer{}

	for rows.Next() {
		var transfer Transfer

		err := rows.Scan(
			&transfer.ID,
			&transfer.CreatedAt,
			&transfer.CreatedBy,
			&transfer.Source.ID,
			&transfer.Source.DsType,
			&transfer.Source.Hostname,
			&transfer.Source.Port,
			&transfer.Source.AccountId,
			&transfer.Source.DbName,
			&transfer.Source.Username,
			&transfer.Source.Password,
			&transfer.Target.ID,
			&transfer.Target.DsType,
			&transfer.Target.Hostname,
			&transfer.Target.Port,
			&transfer.Target.AccountId,
			&transfer.Target.DbName,
			&transfer.Target.Username,
			&transfer.Target.Password,
			&transfer.Query,
			&transfer.TargetSchema,
			&transfer.TargetTable,
			&transfer.Overwrite,
			&transfer.Version,
		)
		if err != nil {
			return nil, err
		}

		transfers = append(transfers, &transfer)
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	return transfers, nil
}

func (m TransferModel) GetById(id int64) (*Transfer, error) {
	query := `
	SELECT
	transfers.id,
	transfers.created_at,
	transfers.created_by,
	transfers.source_id,
	source.name,
	source.ds_type,
	source.account_id,
	source.db_name,
	transfers.target_id,
	target.name,
	target.ds_type,
	target.account_id,
	target.db_name,
	transfers.query,
	transfers.target_schema,
	transfers.target_table,
	transfers.overwrite,
	transfers.status,
	transfers.error,
	transfers.error_properties,
	transfers.stopped_at,
	transfers.version
FROM
	transfers
left join
	connections source
on
	transfers.source_id = source.id
left join
	connections target
on
	transfers.target_id = target.id
where transfers.id = $1
`

	var transfer Transfer

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	err := m.DB.QueryRowContext(ctx, query, id).Scan(
		&transfer.ID,
		&transfer.CreatedAt,
		&transfer.CreatedBy,
		&transfer.SourceID,
		&transfer.Source.Name,
		&transfer.Source.DsType,
		&transfer.Source.AccountId,
		&transfer.Source.DbName,
		&transfer.TargetID,
		&transfer.Target.Name,
		&transfer.Target.DsType,
		&transfer.Target.AccountId,
		&transfer.Target.DbName,
		&transfer.Query,
		&transfer.TargetSchema,
		&transfer.TargetTable,
		&transfer.Overwrite,
		&transfer.Status,
		&transfer.Error,
		&transfer.ErrorProperties,
		&transfer.StoppedAt,
		&transfer.Version,
	)

	if err != nil {
		switch {
		case errors.Is(err, sql.ErrNoRows):
			return nil, ErrRecordNotFound
		default:
			return nil, err
		}
	}

	return &transfer, nil
}

func (m TransferModel) Update(transfer *Transfer) error {
	query := `
        UPDATE transfers 
        SET status = $1, error = $2, error_properties = $3, stopped_at = $4, version = version + 1
        WHERE id = $5 AND version = $6
        RETURNING version`

	args := []interface{}{
		&transfer.Status,
		&transfer.Error,
		&transfer.ErrorProperties,
		&transfer.StoppedAt,
		&transfer.ID,
		&transfer.Version,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	err := m.DB.QueryRowContext(ctx, query, args...).Scan(&transfer.Version)
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

func (m TransferModel) Delete(id int64) error {
	if id < 1 {
		return ErrRecordNotFound
	}

	query := `
			DELETE FROM transfers
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
