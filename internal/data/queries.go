package data

import (
	"database/sql"
	"time"

	"github.com/sqlpipe/sqlpipe/internal/validator"
)

type Query struct {
	ID              int64      `json:"id"`
	CreatedAt       time.Time  `json:"createdAt"`
	StoppedAt       time.Time  `json:"stoppedAt"`
	ConnectionID    int64      `json:"connectionId"`
	Connection      Connection `json:"-"`
	Query           string     `json:"query"`
	ReturnFormat    string     `json:"returnFormat"`
	Status          string     `json:"status"`
	Error           string     `json:"error"`
	ErrorProperties string     `json:"errorProperties"`
}

type QueryModel struct {
	DB *sql.DB
}

func (m QueryModel) Insert(query *Query) (*Query, error) {
	return query, nil
}

func ValidateQuery(v *validator.Validator, query *Query) {
	v.Check(query.Query != "", "query", "A query is required")
}

func (m QueryModel) GetQueued() ([]*Query, error) {
	return []*Query{}, nil
}

func (m QueryModel) GetById(id int64) (*Query, error) {
	return &Query{}, nil
}

func (m QueryModel) Delete(id int64) error {
	return nil
}
