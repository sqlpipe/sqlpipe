package main

import (
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/julienschmidt/httprouter"
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
		Query                  string `json:"query"`
		TargetTable            string `json:"target-table"`
		DropTargetTable        bool   `json:"drop-target-table"`
		CreateTargetTable      bool   `json:"create-target-table"`
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

	transfer = setSourceAndTargetName(transfer)

	v := newValidator()
	if validateTransfer(v, transfer); !v.valid() {
		failedValidationResponse(w, r, v.errors)
		return
	}

	transferMap[transfer.Id] = transfer

	go func() {
		sourceSystem, err := newSystem(
			transfer.SourceType,
			transfer.SourceName,
			transfer.SourceConnectionString,
			transfer.SourceDriver,
		)
		if err != nil {
			transferError(transfer, fmt.Errorf("error creating source system -> %v", err))
			return
		}

		targetSystem, err := newSystem(
			transfer.TargetType,
			transfer.TargetName,
			transfer.TargetConnectionString,
			transfer.TargetDriver,
		)
		if err != nil {
			transferError(transfer, fmt.Errorf("error creating target system -> %v", err))
			return
		}

		if transfer.DropTargetTable {
			err = targetSystem.dropTable(transfer.TargetSchema, transfer.TargetTable)
			if err != nil {
				transferError(transfer, fmt.Errorf("error dropping target table -> %v", err))
				return
			}
		}

		rows, err := sourceSystem.connection.Query(transfer.Query)
		if err != nil {
			transferError(transfer, fmt.Errorf("error querying source -> %v", err))
			return
		}
		defer rows.Close()

		cols, err := rows.Columns()
		if err != nil {
			transferError(transfer, fmt.Errorf("error getting source columns -> %v", err))
			return
		}
		numCols := len(cols)

		colVals := make([]interface{}, numCols)
		colPtrs := make([]interface{}, numCols)
		for i := range colPtrs {
			colPtrs[i] = &colVals[i]
		}

		columnInfo, err := getColumnInfo(rows)
		if err != nil {
			transferError(transfer, fmt.Errorf("error getting source column info -> %v", err))
			return
		}

		if transfer.CreateTargetTable {
			err = targetSystem.createTable(transfer.TargetSchema, transfer.TargetTable, columnInfo)
			if err != nil {
				transferError(transfer, fmt.Errorf("error creating target table -> %v", err))
				return
			}
		}

		for rows.Next() {
			err = rows.Scan(colPtrs...)
			if err != nil {
				transferError(transfer, fmt.Errorf("error scanning source row -> %v", err))
				return
			}

			// for i := range colVals {
			// 	if i == 0 {
			// 		val := colVals[i].([]byte)
			// 		fmt.Printf("%s: %s\n", cols[i], string(val))
			// 	}
			// fmt.Printf("%s: %v\n", cols[i], colVals[i])
			// }
		}
	}()

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
