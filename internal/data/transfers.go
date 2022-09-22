package data

import (
	"time"

	"github.com/sqlpipe/sqlpipe/internal/validator"
)

type Transfer struct {
	Id        string    `json:"transfer_id"`
	CreatedAt time.Time `json:"transfer_created_at"`
	Source    Source    `json:"transfer_source"`
	Target    Target    `json:"transfer_target"`
	Query     string    `json:"query"`
}

func ValidateTransfer(v *validator.Validator, transfer *Transfer) {

}
