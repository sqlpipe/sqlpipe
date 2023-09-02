package main

import (
	"database/sql"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	"github.com/julienschmidt/httprouter"
)

var (
	StatusRunning   = "running"
	StatusCancelled = "cancelled"
	StatusError     = "error"
	StatusFinished  = "finished"

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

var transferMap = map[string]*Transfer{}

type Transfer struct {
	Id        string     `json:"id"`
	CreatedAt time.Time  `json:"created-at"`
	StoppedAt *time.Time `json:"stopped-at,omitempty"`
	Status    string     `json:"status"`
	Err       string     `json:"error,omitempty"`

	SourceName             string `json:"source-name"`
	SourceType             string `json:"source-type"`
	SourceConnectionString string `json:"-"`
	Source                 System `json:"-"`
	SourceTimezone         string `json:"source-timezone,omitempty"`

	TargetName             string `json:"target-name"`
	TargetType             string `json:"target-type"`
	TargetConnectionString string `json:"-"`
	Target                 System `json:"-"`
	TargetHostname         string `json:"target-hostname,omitempty"`
	TargetPort             int    `json:"target-port,omitempty"`
	TargetUsername         string `json:"target-username,omitempty"`
	TargetPassword         string `json:"-"`
	TargetDatabase         string `json:"target-database,omitempty"`
	TargetSchema           string `json:"target-schema,omitempty"`

	Query       string `json:"query"`
	TargetTable string `json:"target-table"`

	DropTargetTable   bool `json:"drop-target-table"`
	CreateTargetTable bool `json:"create-target-table"`

	Rows        *sql.Rows    `json:"-"`
	ColumnInfo  []ColumnInfo `json:"-"`
	TmpDir      string       `json:"tmp-dir"`
	PipeFileDir string       `json:"pipe-file-dir"`
	FinalCsvDir string       `json:"final-csv-dir"`

	Delimiter string `json:"delimiter"`
	Newline   string `json:"newline"`
	Null      string `json:"null"`

	KeepFiles bool `json:"keep-files"`
}

func createTransferHandler(w http.ResponseWriter, r *http.Request) {
	var input struct {
		SourceName             string `json:"source-name"`
		SourceType             string `json:"source-type"`
		SourceConnectionString string `json:"source-connection-string"`
		SourceTimezone         string `json:"source-timezone"`
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
		DropTargetTable        bool   `json:"drop-target-table"`
		CreateTargetTable      bool   `json:"create-target-table"`
		Delimiter              string `json:"delimiter"`
		Newline                string `json:"newline"`
		Null                   string `json:"null"`
		KeepFiles              bool   `json:"keep-files"`
	}

	err := readJSON(w, r, &input)
	if err != nil {
		clientErrorResponse(w, r, http.StatusBadRequest, err)
		return
	}

	sourceSystem, err := newSystem(input.SourceName, input.SourceType, input.SourceConnectionString, input.SourceTimezone)
	if err != nil {
		clientErrorResponse(w, r, http.StatusBadRequest, err)
		return
	}

	targetSystem, err := newSystem(input.TargetName, input.TargetType, input.TargetConnectionString, input.SourceTimezone)
	if err != nil {
		clientErrorResponse(w, r, http.StatusBadRequest, err)
		return
	}

	if input.SourceName == "" {
		input.SourceName = input.SourceType
	}

	if input.TargetName == "" {
		input.TargetName = input.TargetType
	}

	if input.Delimiter == "" {
		input.Delimiter = "{dlm}"
	}

	if input.Newline == "" {
		input.Newline = "{nwln}"
	}

	if input.Null == "" {
		input.Null = "{nll}"
		if input.TargetType == "mysql" {
			input.Null = `NULL`
		}
	}

	transfer := &Transfer{
		Id:                     uuid.New().String(),
		CreatedAt:              time.Now(),
		Status:                 StatusRunning,
		SourceName:             input.SourceName,
		SourceType:             input.SourceType,
		SourceConnectionString: input.SourceConnectionString,
		Source:                 sourceSystem,
		TargetName:             input.TargetName,
		TargetType:             input.TargetType,
		TargetConnectionString: input.TargetConnectionString,
		Target:                 targetSystem,
		TargetSchema:           input.TargetSchema,
		TargetTable:            input.TargetTable,
		Query:                  input.Query,
		DropTargetTable:        input.DropTargetTable,
		CreateTargetTable:      input.CreateTargetTable,
		Delimiter:              input.Delimiter,
		Newline:                input.Newline,
		Null:                   input.Null,
		TargetHostname:         input.TargetHostname,
		TargetPort:             input.TargetPort,
		TargetUsername:         input.TargetUsername,
		TargetPassword:         input.TargetPassword,
		TargetDatabase:         input.TargetDatabase,
		KeepFiles:              input.KeepFiles,
	}
	infoLog.Printf(`IP address %v created transfer %v to transfer from "%v" to "%v"`, r.RemoteAddr, transfer.Id, transfer.SourceName, transfer.TargetName)

	transfer.TmpDir = filepath.Join(globalTmpDir, transfer.Id)
	err = os.Mkdir(transfer.TmpDir, os.ModePerm)
	if err != nil {
		transferError(transfer, fmt.Errorf("error creating transfer dir :: %v", err))
		return
	}

	transfer.PipeFileDir = filepath.Join(transfer.TmpDir, "pipe-files")
	err = os.Mkdir(transfer.PipeFileDir, os.ModePerm)
	if err != nil {
		transferError(transfer, fmt.Errorf("error creating pipe file dir :: %v", err))
		return
	}

	transfer.FinalCsvDir = filepath.Join(transfer.TmpDir, "final-csv")
	err = os.Mkdir(transfer.FinalCsvDir, os.ModePerm)
	if err != nil {
		transferError(transfer, fmt.Errorf("error creating final csv dir :: %v", err))
		return
	}

	infoLog.Printf("temp dir %v created", transfer.TmpDir)

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
		serverErrorResponse(w, r, http.StatusInternalServerError, err)
		return
	}

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
		serverErrorResponse(w, r, http.StatusInternalServerError, err)
		return
	}
}

func listTransfersHandler(w http.ResponseWriter, r *http.Request) {
	err := writeJSON(w, http.StatusOK, envelope{"transfers": transferMap}, nil)
	if err != nil {
		serverErrorResponse(w, r, http.StatusInternalServerError, err)
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
		serverErrorResponse(w, r, http.StatusInternalServerError, err)
		return
	}
}
