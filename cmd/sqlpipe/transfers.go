package main

import (
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"
)

var (
	StatusRunning   = "running"
	StatusFinished  = "finished"
	StatusCancelled = "cancelled"
	StatusError     = "error"
)

var transferMap = map[string]transfer{}

type transfer struct {
	// generated fields
	Id        string     `json:"id"`
	CreatedAt time.Time  `json:"created-at"`
	StoppedAt *time.Time `json:"stopped-at,omitempty"`
	Status    string     `json:"status"`
	Err       string     `json:"error,omitempty"`

	// required inputs
	SourceName             string `json:"source-name"`
	SourceConnectionString string `json:"-"`
	TargetName             string `json:"target-name"`
	TargetConnectionString string `json:"-"`
	Query                  string `json:"query"`
	TargetTable            string `json:"target-table"`

	// optional inputs
	DropTargetTable   bool `json:"drop-target-table"`
	CreateTargetTable bool `json:"create-target-table"`
}

func (app *application) createTransferHandler(w http.ResponseWriter, r *http.Request) {
	var input struct {
		SourceName             string `json:"source-name"`
		SourceConnectionString string `json:"source-connection-string"`
		TargetName             string `json:"target-name"`
		TargetConnectionString string `json:"target-connection-string"`
		Query                  string `json:"query"`
		TargetTable            string `json:"target-table"`
		DropTargetTable        bool   `json:"drop-target-table"`
		CreateTargetTable      bool   `json:"create-target-table"`
	}

	// set defaults
	input.DropTargetTable = true
	input.CreateTargetTable = true

	err := app.readJSON(w, r, &input)
	if err != nil {
		app.clientErrorResponse(w, r, http.StatusBadRequest, err)
		return
	}

	transfer := transfer{
		Id:                     uuid.New().String(),
		CreatedAt:              time.Now(),
		Status:                 StatusRunning,
		SourceName:             input.SourceName,
		SourceConnectionString: input.SourceConnectionString,
		TargetName:             input.TargetName,
		TargetConnectionString: input.TargetConnectionString,
		TargetTable:            input.TargetTable,
		Query:                  input.Query,
		DropTargetTable:        input.DropTargetTable,
		CreateTargetTable:      input.CreateTargetTable,
	}

	v := newValidator()

	if validateTransfer(v, transfer); !v.valid() {
		app.failedValidationResponse(w, r, v.errors)
		return
	}

	transferMap[transfer.Id] = transfer

	headers := make(http.Header)
	headers.Set("Location", fmt.Sprintf("/v1/transfers/%s", transfer.Id))

	err = app.writeJSON(w, http.StatusCreated, envelope{"transfer": transfer}, headers)
	if err != nil {
		app.errorResponse(w, r, http.StatusInternalServerError, err)
		return
	}

	app.infoLog.Printf(`transfer created from "%v" to "%v" by IP address %v`, transfer.SourceName, transfer.TargetName, r.RemoteAddr)
}

func (app *application) showTransferHandler(w http.ResponseWriter, r *http.Request) {
	id := app.readIdParam(r)

	transfer, ok := transferMap[id]
	if !ok {
		app.notFoundResponse(w, r)
		return
	}

	err := app.writeJSON(w, http.StatusOK, envelope{"transfer": transfer}, nil)
	if err != nil {
		app.errorResponse(w, r, http.StatusInternalServerError, err)
		return
	}
}

func (app *application) listTransfersHandler(w http.ResponseWriter, r *http.Request) {
	err := app.writeJSON(w, http.StatusOK, envelope{"transfers": transferMap}, nil)
	if err != nil {
		app.errorResponse(w, r, http.StatusInternalServerError, err)
		return
	}
}

func (app *application) cancelTransferHandler(w http.ResponseWriter, r *http.Request) {
	id := app.readIdParam(r)

	transfer, ok := transferMap[id]
	if !ok {
		app.notFoundResponse(w, r)
		return
	}

	if transfer.Status != StatusRunning {
		app.clientErrorResponse(w, r, http.StatusBadRequest, fmt.Errorf("cannot cancel transfer with status of %v", transfer.Status))
		return
	}

	now := time.Now()

	transfer.Status = StatusCancelled
	transfer.StoppedAt = &now

	transferMap[id] = transfer

	err := app.writeJSON(w, http.StatusOK, envelope{"transfer": transfer}, nil)
	if err != nil {
		app.errorResponse(w, r, http.StatusInternalServerError, err)
		return
	}
}
