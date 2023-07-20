package main

import (
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/julienschmidt/httprouter"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	_ "github.com/microsoft/go-mssqldb"
	_ "github.com/sijms/go-ora/v2"
	_ "github.com/snowflakedb/gosnowflake"
)

var (
	StatusRunning   = "running"
	StatusCancelled = "cancelled"
	StatusError     = "error"
	StatusFinished  = "finished"
)

var transferMap = map[string]Transfer{}

type Transfer struct {
	Id        string     `json:"id"`
	CreatedAt time.Time  `json:"created-at"`
	StoppedAt *time.Time `json:"stopped-at,omitempty"`
	Status    string     `json:"status"`
	Err       string     `json:"error,omitempty"`

	SourceName             string `json:"source-name,omitempty"`
	SourceType             string `json:"source-type"`
	SourceConnectionString string `json:"-"`
	SourceDriver           string `json:"-"`

	TargetName             string `json:"target-name,omitempty"`
	TargetType             string `json:"target-type"`
	TargetConnectionString string `json:"-"`
	TargetDriver           string `json:"-"`
	TargetTable            string `json:"target-table"`
	TargetSchema           string `json:"target-schema,omitempty"`

	Query             string `json:"query"`
	DropTargetTable   bool   `json:"drop-target-table"`
	CreateTargetTable bool   `json:"create-target-table"`
}

func createTransferHandler(w http.ResponseWriter, r *http.Request) {
	var input struct {
		SourceName             string `json:"source-name"`
		SourceType             string `json:"source-type"`
		SourceConnectionString string `json:"source-connection-string"`

		TargetName             string `json:"target-name"`
		TargetType             string `json:"target-type"`
		TargetConnectionString string `json:"target-connection-string"`
		TargetSchema           string `json:"target-schema"`

		Query             string `json:"query"`
		TargetTable       string `json:"target-table"`
		DropTargetTable   bool   `json:"drop-target-table"`
		CreateTargetTable bool   `json:"create-target-table"`
	}

	err := readJSON(w, r, &input)
	if err != nil {
		clientErrorResponse(w, r, http.StatusBadRequest, err)
		return
	}

	transfer := Transfer{
		Id:                     uuid.New().String(),
		CreatedAt:              time.Now(),
		Status:                 StatusRunning,
		SourceName:             input.SourceName,
		SourceType:             input.SourceType,
		SourceConnectionString: input.SourceConnectionString,
		SourceDriver:           getDriver(input.SourceType),
		TargetName:             input.TargetName,
		TargetType:             input.TargetType,
		TargetConnectionString: input.TargetConnectionString,
		TargetDriver:           getDriver(input.TargetType),
		TargetSchema:           input.TargetSchema,
		TargetTable:            input.TargetTable,
		Query:                  input.Query,
		DropTargetTable:        input.DropTargetTable,
		CreateTargetTable:      input.CreateTargetTable,
	}

	if transfer.SourceName == "" {
		transfer.SourceName = transfer.SourceType
	}
	if transfer.TargetName == "" {
		transfer.TargetName = transfer.TargetType
	}

	v := newValidator()

	if validateTransfer(v, transfer); !v.valid() {
		failedValidationResponse(w, r, v.errors)
		return
	}

	transferMap[transfer.Id] = transfer

	go runTransfer(transfer)

	headers := make(http.Header)
	headers.Set("Location", fmt.Sprintf("/v3/transfers/%s", transfer.Id))

	err = writeJSON(w, http.StatusCreated, envelope{"transfer": transfer}, headers)
	if err != nil {
		errorResponse(w, r, http.StatusInternalServerError, err)
		return
	}

	infoLog.Printf(`IP address %v created transfer %v to transfer from "%v" to "%v"`, r.RemoteAddr, transfer.Id, transfer.SourceName, transfer.TargetName)
}

func showTransferHandler(w http.ResponseWriter, r *http.Request) {
	params := httprouter.ParamsFromContext(r.Context())
	id := params.ByName("id")

	transfer, ok := transferMap[id]
	if !ok {
		notFoundResponse(w, r)
		return
	}

	err := writeJSON(w, http.StatusOK, envelope{"transfer": transfer}, nil)
	if err != nil {
		errorResponse(w, r, http.StatusInternalServerError, err)
		return
	}
}

func listTransfersHandler(w http.ResponseWriter, r *http.Request) {
	err := writeJSON(w, http.StatusOK, envelope{"transfers": transferMap}, nil)
	if err != nil {
		errorResponse(w, r, http.StatusInternalServerError, err)
		return
	}
}

func cancelTransferHandler(w http.ResponseWriter, r *http.Request) {
	params := httprouter.ParamsFromContext(r.Context())
	id := params.ByName("id")

	transfer, ok := transferMap[id]
	if !ok {
		notFoundResponse(w, r)
		return
	}

	if transfer.Status != StatusRunning {
		clientErrorResponse(w, r, http.StatusBadRequest, fmt.Errorf("cannot cancel transfer with status of %v", transfer.Status))
		return
	}

	now := time.Now()

	transfer.Status = StatusCancelled
	transfer.StoppedAt = &now

	transferMap[id] = transfer

	err := writeJSON(w, http.StatusOK, envelope{"transfer": transfer}, nil)
	if err != nil {
		errorResponse(w, r, http.StatusInternalServerError, err)
		return
	}
}
