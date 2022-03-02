package data

import (
	"database/sql"
	"time"

	"github.com/calmitchell617/sqlpipe/internal/validator"
)

type Replication struct {
	ID              int64      `json:"id"`
	CreatedAt       time.Time  `json:"createdAt"`
	SourceID        int64      `json:"sourceID"`
	Source          Connection `json:"-"`
	TargetID        int64      `json:"targetID"`
	Target          Connection `json:"-"`
	Query           string     `json:"query"`
	Tables          []string   `json:"tables"`
	TargetSchema    string     `json:"targetSchema"`
	Status          string     `json:"status"`
	Error           string     `json:"error"`
	ErrorProperties string     `json:"errorProperties"`
	StoppedAt       time.Time  `json:"stoppedAt"`
	Version         int        `json:"version"`
	ReplicationSlot string     `json:"replicationSlot"`
}

type ReplicationModel struct {
	DB *sql.DB
}

func ValidateReplication(v *validator.Validator, replication *Replication) {
}
