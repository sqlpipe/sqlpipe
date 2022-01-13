package data

import (
	"database/sql"
	"errors"
)

var (
	ErrRecordNotFound = errors.New("record not found")
	ErrEditConflict   = errors.New("edit conflict")
)

type Models struct {
	Users       UserModel
	Connections ConnectionModel
	Transfers   TransferModel
}

func NewModels(db *sql.DB) Models {
	return Models{
		Users:       UserModel{DB: db},
		Connections: ConnectionModel{DB: db},
		Transfers:   TransferModel{DB: db},
	}
}
