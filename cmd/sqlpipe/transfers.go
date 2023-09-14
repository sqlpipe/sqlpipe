package main

import (
	"context"
	"fmt"
	"net/http"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	"github.com/julienschmidt/httprouter"
)

var (
	StatusQueued    = "queued"
	StatusRunning   = "running"
	StatusCancelled = "cancelled"
	StatusError     = "error"
	StatusComplete  = "complete"

	TypePostgreSQL = "postgresql"
	TypeMySQL      = "mysql"
	TypeMSSQL      = "mssql"
	TypeOracle     = "oracle"
	TypeSnowflake  = "snowflake"

	DriverPostgreSQL = "pgx"
	DriverMySQL      = "mysql"
	DriverMSSQL      = "sqlserver"
	DriverOracle     = "oracle"
	DriverSnowflake  = "snowflake"
)

type Transfer struct {
	Id        string    `json:"id"`
	CreatedAt time.Time `json:"created-at"`
	StoppedAt string    `json:"stopped-at,omitempty"`
	Status    string    `json:"status"`
	Err       string    `json:"error,omitempty"`

	TmpDir      string `json:"tmp-dir"`
	PipeFileDir string `json:"pipe-file-dir"`
	FinalCsvDir string `json:"final-csv-dir"`
	KeepFiles   bool   `json:"keep-files"`

	Delimiter string `json:"delimiter"`
	NewLine   string `json:"new-line"`
	Null      string `json:"null"`

	SourceName             string `json:"source-name"`
	SourceType             string `json:"source-type"`
	SourceConnectionString string `json:"-"`

	TargetName             string `json:"target-name"`
	TargetType             string `json:"target-type"`
	TargetConnectionString string `json:"-"`

	TargetHostname string `json:"target-hostname,omitempty"`
	TargetPort     int    `json:"target-port,omitempty"`
	TargetUsername string `json:"target-username,omitempty"`
	TargetPassword string `json:"-"`
	TargetDatabase string `json:"target-database,omitempty"`

	Query        string `json:"query"`
	TargetSchema string `json:"target-schema,omitempty"`
	TargetTable  string `json:"target-table"`

	DropTargetTableIfExists bool `json:"drop-target-table-if-exists"`
	CreateTargetTable       bool `json:"create-target-table"`

	CancelChannel    chan string `json:"-"`
	CancelledChannel chan bool   `json:"-"`
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
		TargetHostname         string `json:"target-hostname"`
		TargetPort             int    `json:"target-port"`
		TargetUsername         string `json:"target-username"`
		TargetPassword         string `json:"target-password"`
		TargetDatabase         string `json:"target-database"`
		Query                  string `json:"query"`
		TargetTable            string `json:"target-table"`
		DropTargetTable        bool   `json:"drop-target-table-if-exists"`
		CreateTargetTable      bool   `json:"create-target-table"`
		Delimiter              string `json:"delimiter"`
		NewLine                string `json:"new-line"`
		Null                   string `json:"null"`
		KeepFiles              bool   `json:"keep-files"`
	}

	err := readJSON(w, r, &input)
	if err != nil {
		clientErrorResponse(w, r, http.StatusBadRequest, err)
		return
	}

	transferId := uuid.New().String()
	tmpDir := filepath.Join(globalTmpDir, transferId)

	if input.Delimiter == "" {
		input.Delimiter = "{dlm}"
	}
	if input.NewLine == "" {
		input.NewLine = "{nwln}"
	}
	if input.Null == "" {
		input.Null = "{nll}"
		if input.TargetType == TypeMySQL {
			input.Null = `NULL`
		}
	}
	if input.SourceName == "" {
		input.SourceName = input.SourceType
	}
	if input.TargetName == "" {
		input.TargetName = input.TargetType
	}

	transfer := Transfer{
		Id:                      transferId,
		CreatedAt:               time.Now(),
		Status:                  StatusQueued,
		TmpDir:                  tmpDir,
		PipeFileDir:             filepath.Join(tmpDir, "pipe-files"),
		FinalCsvDir:             filepath.Join(tmpDir, "final-csvs"),
		KeepFiles:               input.KeepFiles,
		Delimiter:               input.Delimiter,
		NewLine:                 input.NewLine,
		Null:                    input.Null,
		SourceName:              input.SourceName,
		SourceType:              input.SourceType,
		SourceConnectionString:  input.SourceConnectionString,
		TargetName:              input.TargetName,
		TargetType:              input.TargetType,
		TargetConnectionString:  input.TargetConnectionString,
		TargetHostname:          input.TargetHostname,
		TargetPort:              input.TargetPort,
		TargetUsername:          input.TargetUsername,
		TargetPassword:          input.TargetPassword,
		TargetDatabase:          input.TargetDatabase,
		Query:                   input.Query,
		TargetSchema:            input.TargetSchema,
		TargetTable:             input.TargetTable,
		DropTargetTableIfExists: input.DropTargetTable,
		CreateTargetTable:       input.CreateTargetTable,
		CancelChannel:           make(chan string),
		CancelledChannel:        make(chan bool),
	}

	v := newValidator()
	if validateTransferInput(v, transfer); !v.valid() {
		failedValidationResponse(w, r, v.errors)
		return
	}

	transferMap.Set(transfer.Id, transfer)

	infoLog.Printf(`ip %v created transfer %v to transfer from %v to %v`, r.RemoteAddr,
		transfer.Id, input.SourceName, input.TargetName)
	infoLog.Printf("transfer %v is now queued", transfer.Id)

	ctx, cancel := context.WithCancel(context.Background())

	go runTransfer(ctx, cancel, transfer)

	headers := make(http.Header)
	headers.Set("Location", fmt.Sprintf("/transfers/%s", transfer.Id))

	err = writeJSON(w, http.StatusCreated, envelope{"transfer": transfer}, headers)
	if err != nil {
		serverErrorResponse(w, r, http.StatusInternalServerError, err)
		return
	}
}

func showTransferHandler(w http.ResponseWriter, r *http.Request) {
	params := httprouter.ParamsFromContext(r.Context())
	id := params.ByName("id")

	transfer, ok := transferMap.Get(id)
	if !ok {
		notFoundResponse(w, r)
		return
	}

	err := writeJSON(w, http.StatusOK, envelope{"transfer": transfer}, nil)
	if err != nil {
		serverErrorResponse(w, r, http.StatusInternalServerError, err)
		return
	}
}

func listTransfersHandler(w http.ResponseWriter, r *http.Request) {

	// get query params
	q := r.URL.Query()
	status := q.Get("status")

	transfers := transferMap.GetEntireMap()

	v := newValidator()
	v.check(
		permittedValue(status,
			StatusQueued,
			StatusRunning,
			StatusComplete,
			StatusCancelled,
			StatusError,
			"",
		),
		"status",
		fmt.Sprintf("must be empty or one of: %s, %s, %s, %s, %s",
			StatusQueued,
			StatusRunning,
			StatusComplete,
			StatusCancelled,
			StatusError,
		),
	)

	if !v.valid() {
		failedValidationResponse(w, r, v.errors)
		return
	}

	if status != "" {
		filteredTransfers := make(map[string]Transfer)
		for id, transfer := range transfers {
			if transfer.Status == status {
				filteredTransfers[id] = transfer
			}
		}
		transfers = filteredTransfers
	}

	err := writeJSON(w, http.StatusOK, envelope{"transfers": transfers}, nil)
	if err != nil {
		serverErrorResponse(w, r, http.StatusInternalServerError, err)
		return
	}
}

func safeCancel(ch chan<- string, ip string) (closed bool) {
	defer func() {
		if recover() != nil {
			closed = true
		}
	}()
	ch <- ip
	return false
}

func cancelTransferHandler(w http.ResponseWriter, r *http.Request) {

	params := httprouter.ParamsFromContext(r.Context())
	id := params.ByName("id")

	transfer, ok := transferMap.Get(id)
	if !ok {
		notFoundResponse(w, r)
		return
	}

	if transfer.Status != StatusRunning {
		clientErrorResponse(w, r, http.StatusBadRequest,
			fmt.Errorf("cannot cancel transfer with status of %v", transfer.Status),
		)
		return
	}

	closed := safeCancel(transfer.CancelChannel, r.RemoteAddr)
	if closed {
		serverErrorResponse(w, r, http.StatusInternalServerError,
			fmt.Errorf("cancel channel was closed, cannot cancel transfer"),
		)
		return
	}

	<-transfer.CancelledChannel

	transfer, ok = transferMap.Get(id)
	if !ok {
		err := fmt.Errorf("transfer %v not found when trying to write response", id)
		serverErrorResponse(w, r, http.StatusInternalServerError, err)
		return
	}

	err := writeJSON(w, http.StatusOK, envelope{"transfer": transfer}, nil)
	if err != nil {
		serverErrorResponse(w, r, http.StatusInternalServerError, err)
		return
	}
}
