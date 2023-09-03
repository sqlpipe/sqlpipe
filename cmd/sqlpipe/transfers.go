package main

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

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

type SafeMap struct {
	mu sync.RWMutex
	m  map[string]Transfer
}

func NewSafeMap() *SafeMap {
	return &SafeMap{
		m: make(map[string]Transfer),
	}
}

func (sm *SafeMap) Set(key string, value Transfer) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.m[key] = value
}

func (sm *SafeMap) Get(key string) (Transfer, bool) {
	sm.mu.RLock()
	defer sm.mu.Unlock()
	val, ok := sm.m[key]
	return val, ok
}

func (sm *SafeMap) Delete(key string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	delete(sm.m, key)
}

var transferMap = NewSafeMap()

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
	Newline   string `json:"newline"`
	Null      string `json:"null"`

	SourceName             string `json:"source-name"`
	SourceType             string `json:"source-type"`
	SourceConnectionString string `json:"source-connection-string"`

	TargetName             string `json:"target-name"`
	TargetType             string `json:"target-type"`
	TargetConnectionString string `json:"target-connection-string"`

	TargetHostname string `json:"target-hostname,omitempty"`
	TargetPort     int    `json:"target-port,omitempty"`
	TargetUsername string `json:"target-username,omitempty"`
	TargetPassword string `json:"-"`
	TargetDatabase string `json:"target-database,omitempty"`
	TargetSchema   string `json:"target-schema,omitempty"`

	Query       string `json:"query"`
	TargetTable string `json:"target-table"`

	DropTargetTableIfExists bool `json:"drop-target-table-if-exists"`
	CreateTargetTable       bool `json:"create-target-table"`

	CancelChannel   chan string `json:"-"`
	ErrorChannel    chan error  `json:"-"`
	CompleteChannel chan bool   `json:"-"`
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
		Newline                string `json:"newline"`
		Null                   string `json:"null"`
		KeepFiles              bool   `json:"keep-files"`
	}

	err := readJSON(w, r, &input)
	if err != nil {
		clientErrorResponse(w, r, http.StatusBadRequest, err)
		return
	}

	transfer := Transfer{
		SourceName:              input.SourceName,
		SourceType:              input.SourceType,
		SourceConnectionString:  input.SourceConnectionString,
		TargetName:              input.TargetName,
		TargetType:              input.TargetType,
		TargetConnectionString:  input.TargetConnectionString,
		TargetSchema:            input.TargetSchema,
		TargetHostname:          input.TargetHostname,
		TargetPort:              input.TargetPort,
		TargetUsername:          input.TargetUsername,
		TargetPassword:          input.TargetPassword,
		TargetDatabase:          input.TargetDatabase,
		Query:                   input.Query,
		TargetTable:             input.TargetTable,
		DropTargetTableIfExists: input.DropTargetTable,
		CreateTargetTable:       input.CreateTargetTable,
		Delimiter:               input.Delimiter,
		Newline:                 input.Newline,
		Null:                    input.Null,
		KeepFiles:               input.KeepFiles,
	}

	transfer.TmpDir = filepath.Join(globalTmpDir, transfer.Id)
	err = os.MkdirAll(transfer.TmpDir, 0600)
	if err != nil {
		serverErrorResponse(w, r, http.StatusInternalServerError, fmt.Errorf("error creating temp dir :: %v", err))
		return
	}

	infoLog.Printf("temp dir %v created", transfer.TmpDir)

	transfer.PipeFileDir = filepath.Join(transfer.TmpDir, "pipe-files")
	err = os.MkdirAll(transfer.PipeFileDir, 0600)
	if err != nil {
		serverErrorResponse(w, r, http.StatusInternalServerError, fmt.Errorf("error creating pipe file dir :: %v", err))
		return
	}

	infoLog.Printf("pipe file dir %v created", transfer.PipeFileDir)

	transfer.FinalCsvDir = filepath.Join(transfer.TmpDir, "final-csv")
	err = os.MkdirAll(transfer.FinalCsvDir, 0600)
	if err != nil {
		serverErrorResponse(w, r, http.StatusInternalServerError, fmt.Errorf("error creating final csv dir :: %v", err))
		return
	}

	infoLog.Printf("final csv dir %v created", transfer.FinalCsvDir)

	if input.Delimiter == "" {
		input.Delimiter = "{dlm}"
	}
	transfer.Delimiter = input.Delimiter

	if input.Newline == "" {
		input.Newline = "{nwln}"
	}
	transfer.Newline = input.Newline

	if input.Null == "" {
		input.Null = "{nll}"
		if input.TargetType == TypeMySQL {
			input.Null = `NULL`
		}
	}
	transfer.Null = input.Null

	if input.SourceName == "" {
		input.SourceName = input.SourceType
	}
	transfer.SourceName = input.SourceName

	if input.TargetName == "" {
		input.TargetName = input.TargetType
	}
	transfer.TargetName = input.TargetName

	v := newValidator()
	if validateTransferInput(v, transfer); !v.valid() {
		failedValidationResponse(w, r, v.errors)
		return
	}

	transfer.CancelChannel = make(chan string)
	transfer.ErrorChannel = make(chan error)
	transfer.CompleteChannel = make(chan bool)

	transferMap.Set(transfer.Id, transfer)

	go runTransfer(transfer)

	infoLog.Printf(`ip %v created transfer %v to transfer from %v to %v`, r.RemoteAddr, transfer.Id, input.SourceName, input.TargetName)

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
	err := writeJSON(w, http.StatusOK, envelope{"transfers": transferMap}, nil)
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
		clientErrorResponse(w, r, http.StatusBadRequest, fmt.Errorf("cannot cancel transfer with status of %v", transfer.Status))
		return
	}

	closed := safeCancel(transfer.CancelChannel, r.RemoteAddr)
	if closed {
		serverErrorResponse(w, r, http.StatusInternalServerError, fmt.Errorf("cancel channel was closed, cannot cancel transfer"))
		return
	}

	err := writeJSON(w, http.StatusOK, envelope{"transfer": transfer}, nil)
	if err != nil {
		serverErrorResponse(w, r, http.StatusInternalServerError, err)
		return
	}
}
