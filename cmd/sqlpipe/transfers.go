package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/julienschmidt/httprouter"
)

type ConnectionInfo struct {
	Name             string `json:"instance-name"`
	Type             string `json:"type"`
	ConnectionString string `json:"-"`
	Hostname         string `json:"hostname"`
	Port             int    `json:"port,omitempty"`
	Database         string `json:"database"`
	Username         string `json:"username"`
	Password         string `json:"-"`
}

type Transfer struct {
	Id                            string             `json:"id"`
	CreatedAt                     time.Time          `json:"created-at"`
	StoppedAt                     string             `json:"stopped-at,omitempty"`
	Status                        string             `json:"status"`
	Error                         string             `json:"error,omitempty"`
	KeepFiles                     bool               `json:"keep-files"`
	TmpDir                        string             `json:"tmp-dir"`
	PipeFileDir                   string             `json:"pipe-file-dir"`
	FinalCsvDir                   string             `json:"final-csv-dir"`
	Context                       context.Context    `json:"-"`
	Cancel                        context.CancelFunc `json:"-"`
	SourceConnectionInfo          ConnectionInfo     `json:"source-connection-info"`
	TargetConnectionInfo          ConnectionInfo     `json:"target-connection-info"`
	DropTargetTableIfExists       bool               `json:"drop-target-table-if-exists"`
	CreateTargetSchemaIfNotExists bool               `json:"create-target-schema-if-not-exists"`
	CreateTargetTableIfNotExists  bool               `json:"create-target-table-if-not-exists"`
	SourceSchema                  string             `json:"source-schema,omitempty"`
	SourceTable                   string             `json:"source-table,omitempty"`
	TargetSchema                  string             `json:"target-schema,omitempty"`
	TargetTable                   string             `json:"target-name"`
	Query                         string             `json:"query,omitempty"`
	Delimiter                     string             `json:"delimiter"`
	Newline                       string             `json:"newline"`
	Null                          string             `json:"null"`
}

var transferMap = NewSafeTransferMap()

type SafeTransferMap struct {
	mu sync.Mutex
	m  map[string]Transfer
}

func NewSafeTransferMap() *SafeTransferMap {
	return &SafeTransferMap{
		m: make(map[string]Transfer),
	}
}

func (sm *SafeTransferMap) Set(key string, value Transfer) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.m[key] = value
}

func (sm *SafeTransferMap) SetStatus(key, status string, transfer Transfer) Transfer {
	transfer.Status = status
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.m[key] = transfer
	infoLog.Printf("transfer %v status set to %v", transfer.Id, status)
	return transfer
}

func (sm *SafeTransferMap) CancelAndSetStatus(key string, transfer Transfer, newStatus string) Transfer {
	transfer.Cancel()
	infoLog.Printf("transfer %v cancelled", transfer.Id)
	transfer = sm.SetStatus(key, newStatus, transfer)
	return transfer
}

func (sm *SafeTransferMap) Get(key string) (Transfer, bool) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	val, ok := sm.m[key]
	return val, ok
}

func (sm *SafeTransferMap) GetEntireMap() map[string]Transfer {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	return sm.m
}

func (sm *SafeTransferMap) Delete(key string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	delete(sm.m, key)
}

func createTransferHandler(w http.ResponseWriter, r *http.Request) {
	var input struct {
		KeepFiles                     bool   `json:"keep-files"`
		SourceName                    string `json:"source-name"`
		SourceType                    string `json:"source-type"`
		SourceConnectionString        string `json:"source-connection-string"`
		TargetName                    string `json:"target-name"`
		TargetType                    string `json:"target-type"`
		TargetConnectionString        string `json:"target-connection-string"`
		TargetHostname                string `json:"target-hostname"`
		TargetPort                    int    `json:"target-port"`
		TargetDatabase                string `json:"target-database"`
		TargetUsername                string `json:"target-username"`
		TargetPassword                string `json:"target-password"`
		DropTargetTableIfExists       bool   `json:"drop-target-table-if-exists"`
		CreateTargetSchemaIfNotExists bool   `json:"create-target-schema-if-not-exists"`
		CreateTargetTableIfNotExists  bool   `json:"create-target-table-if-not-exists"`
		SourceSchema                  string `json:"source-schema"`
		SourceTable                   string `json:"source-table"`
		TargetSchema                  string `json:"target-schema"`
		TargetTable                   string `json:"target-table"`
		Query                         string `json:"query"`
		Delimiter                     string `json:"delimiter"`
		Newline                       string `json:"newline"`
		Null                          string `json:"null"`
	}

	err := readJSON(w, r, &input)
	if err != nil {
		clientErrorResponse(w, r, http.StatusBadRequest, err)
		return
	}

	id := uuid.New().String()

	tmpDir, pipeFileDir, finalCsvDir, err := createTransferTmpDirs(id)
	if err != nil {
		serverErrorResponse(w, r, http.StatusInternalServerError, err)
		return
	}

	if input.Delimiter == "" {
		input.Delimiter = "{dlm}"
	}
	if input.Newline == "" {
		input.Newline = "{nwln}"
	}
	if input.Null == "" {
		input.Null = "{nll}"
		if input.TargetType == TypeMySQL {
			input.Null = `NULL`
		}
	}

	sourceConnectionInfo := ConnectionInfo{
		Name:             input.SourceName,
		Type:             input.SourceType,
		ConnectionString: input.SourceConnectionString,
	}

	targetConnectionInfo := ConnectionInfo{
		Name:             input.TargetName,
		Type:             input.TargetType,
		ConnectionString: input.TargetConnectionString,
		Hostname:         input.TargetHostname,
		Port:             input.TargetPort,
		Database:         input.TargetDatabase,
		Username:         input.TargetUsername,
		Password:         input.TargetPassword,
	}

	ctx, cancel := context.WithCancel(context.Background())

	transfer := Transfer{
		Id:                            id,
		CreatedAt:                     time.Now(),
		Status:                        StatusQueued,
		KeepFiles:                     input.KeepFiles,
		TmpDir:                        tmpDir,
		PipeFileDir:                   pipeFileDir,
		FinalCsvDir:                   finalCsvDir,
		Delimiter:                     input.Delimiter,
		Newline:                       input.Newline,
		Null:                          input.Null,
		Context:                       ctx,
		Cancel:                        cancel,
		SourceConnectionInfo:          sourceConnectionInfo,
		TargetConnectionInfo:          targetConnectionInfo,
		DropTargetTableIfExists:       input.DropTargetTableIfExists,
		CreateTargetSchemaIfNotExists: input.CreateTargetSchemaIfNotExists,
		CreateTargetTableIfNotExists:  input.CreateTargetTableIfNotExists,
		SourceSchema:                  input.SourceSchema,
		SourceTable:                   input.SourceTable,
		TargetSchema:                  input.TargetSchema,
		TargetTable:                   input.TargetTable,
		Query:                         input.Query,
	}

	v := newValidator()

	v.check(transfer.SourceConnectionInfo.Name != "", "source-name", "must be provided")
	v.check(transfer.SourceConnectionInfo.Type != "", "source-type", "must be provided")
	v.check(transfer.SourceConnectionInfo.ConnectionString != "", "source-connection-string", "must be provided")
	v.check(permittedValue(transfer.SourceConnectionInfo.Type, permittedTransferSources...),
		"source-type", fmt.Sprintf("must be one of %v", permittedTransferSources))

	v.check(transfer.TargetConnectionInfo.Name != "", "target-name", "must be provided")
	v.check(transfer.TargetConnectionInfo.Type != "", "target-type", "must be provided")
	v.check(transfer.TargetConnectionInfo.ConnectionString != "", "target-connection-string", "must be provided")
	v.check(permittedValue(transfer.TargetConnectionInfo.Type, permittedTransferTargets...),
		"target-type", fmt.Sprintf("must be one of %v", permittedTransferTargets))

	if transfer.Query == "" {
		if schemaRequired[transfer.SourceConnectionInfo.Type] {
			v.check(transfer.SourceSchema != "", "source-schema", fmt.Sprintf("if query is not provided, must be provided for source type %v", transfer.TargetConnectionInfo.Type))
		}
		v.check(transfer.SourceTable != "", "source-table", "must be provided if query is not provided")
	} else {
		v.check(transfer.SourceSchema == "", "source-schema", "must not be provided if query is provided")
		v.check(transfer.SourceTable == "", "source-table", "must not be provided if query is provided")
	}

	if transfer.SourceSchema == "" && transfer.SourceTable == "" {
		v.check(transfer.Query != "", "query", "must be provided if source-schema and source-table are not provided")
	}

	if transfer.SourceSchema != "" || transfer.SourceTable != "" {
		v.check(transfer.Query == "", "query", "must not be provided if source-schema or source-table are provided")
	}

	v.check(transfer.TargetTable != "", "target-table", "must be provided")
	if schemaRequired[transfer.TargetConnectionInfo.Type] {
		v.check(transfer.TargetSchema != "", "target-schema", fmt.Sprintf("must be provided for target type %v", transfer.TargetConnectionInfo.Type))
	}

	v.check(transfer.TmpDir != "", "tmp-dir", "was not set - this is a bug")
	v.check(transfer.PipeFileDir != "", "pipe-file-dir", "was not set - this is a bug")
	v.check(transfer.FinalCsvDir != "", "final-csv-dir", "was not set - this is a bug")

	switch transfer.SourceConnectionInfo.Type {
	case TypeMySQL:
		v.check(strings.Contains(transfer.SourceConnectionInfo.ConnectionString, "parseTime=true"), "source-connection-string", "must contain parseTime=true to move timestamp with time zone data from mysql")
		v.check(strings.Contains(transfer.SourceConnectionInfo.ConnectionString, "loc="), "source-connection-string", `must contain loc=<URL_ENCODED_IANA_TIME_ZONE> to move timestamp with time zone data from mysql - example: loc=US%2FPacific`)
	}

	switch transfer.TargetConnectionInfo.Type {
	case TypePostgreSQL:
		v.check(psqlAvailable, "target-type", "you must install psql to transfer data to postgresql")
	case TypeMSSQL:
		v.check(bcpAvailable, "target-type", "you must install bcp to transfer data to mssql")
		v.check(transfer.TargetConnectionInfo.Port == 0, "target-port", "to change the target port for mssql, enter it after a comma in the -target-hostname flag like 127.0.0.1,1433")
		v.check(transfer.TargetConnectionInfo.Hostname != "", "target-hostname", "must be provided for target type mssql")
		v.check(transfer.TargetConnectionInfo.Username != "", "target-username", "must be provided for target type mssql")
		v.check(transfer.TargetConnectionInfo.Password != "", "target-password", "must be provided for target type mssql")
		v.check(transfer.TargetConnectionInfo.Database != "", "target-database", "must be provided for target type mssql")
	case TypeMySQL:
	case TypeOracle:
		v.check(sqlldrAvailable, "target-type", "you must install SQL*Loader to transfer data to oracle")
		v.check(transfer.TargetConnectionInfo.Hostname != "", "target-hostname", "must be provided for target type oracle")
		v.check(transfer.TargetConnectionInfo.Username != "", "target-username", "must be provided for target type oracle")
		v.check(transfer.TargetConnectionInfo.Password != "", "target-password", "must be provided for target type oracle")
		v.check(transfer.TargetConnectionInfo.Database != "", "target-database", "must be provided for target type oracle")
	case TypeSnowflake:
	}

	if !v.valid() {
		failedValidationResponse(w, r, v.errors)
		return
	}

	transferMap.Set(transfer.Id, transfer)

	infoLog.Printf(`ip %v created transfer %v from %v to %v`,
		r.RemoteAddr, transfer.Id, input.SourceName, input.TargetName)

	go func() {
		if !transfer.KeepFiles {
			defer func() {
				err = os.RemoveAll(transfer.TmpDir)
				if err != nil {
					errorLog.Printf("error removing temp dir %v :: %v", transfer.TmpDir, err)
					return
				}
				infoLog.Printf("temp dir %v removed", transfer.TmpDir)
			}()
		}

		err = runTransfer(transfer)
		if err != nil {
			transfer.Error = fmt.Sprintf("error running transfer %v :: %v", transfer.Id, err)
			transferMap.CancelAndSetStatus(transfer.Id, transfer, StatusError)
			errorLog.Println(transfer.Error)
		}
	}()

	headers := make(http.Header)
	headers.Set("Location", fmt.Sprintf("/transfers/%s", transfer.Id))

	err = writeJSON(w, http.StatusCreated, envelope{"transfer": transfer}, headers)
	if err != nil {
		serverErrorResponse(w, r, http.StatusInternalServerError, err)
		return
	}
}

func showTransferHandler(w http.ResponseWriter, r *http.Request) {

	id := httprouter.ParamsFromContext(r.Context()).ByName("id")

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
		permittedValue(status, Statuses...),
		"status", fmt.Sprintf("must be empty or one of: %v", strings.Join(Statuses, ", ")),
	)

	if !v.valid() {
		failedValidationResponse(w, r, v.errors)
		return
	}

	if status != "" {
		// filter by status
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

	transfer = transferMap.CancelAndSetStatus(id, transfer, StatusCancelled)

	err := writeJSON(w, http.StatusOK, envelope{"transfer": transfer}, nil)
	if err != nil {
		serverErrorResponse(w, r, http.StatusInternalServerError, err)
		return
	}
}

func runTransfer(transfer Transfer) (err error) {

	transferMap.SetStatus(transfer.Id, StatusRunning, transfer)

	source, err := newSystem(transfer.SourceConnectionInfo)
	if err != nil {
		return fmt.Errorf("error creating source system :: %v", err)
	}
	defer source.closeConnectionPool(true)

	target, err := newSystem(transfer.TargetConnectionInfo)
	if err != nil {
		return fmt.Errorf("error creating target system :: %v", err)
	}
	defer target.closeConnectionPool(true)

	if target.schemaRequired() && transfer.CreateTargetSchemaIfNotExists {
		err = createSchemaIfNotExists(transfer.TargetSchema, target)
		if err != nil {
			return fmt.Errorf("error creating target schema :: %v", err)
		}
	}

	if transfer.DropTargetTableIfExists {
		err = dropTableIfExists(transfer.TargetSchema, transfer.TargetTable, target)
		if err != nil {
			return fmt.Errorf("error dropping target table :: %v", err)
		}
	}

	escapedSourceSchemaPeriodTable := getSchemaPeriodTable(transfer.SourceSchema, transfer.SourceTable, source, true)
	query := transfer.Query
	initialLoad := true
	var columnInfos []ColumnInfo

	if transfer.SourceTable != "" {

		columnInfos, err = getTableColumnInfos(transfer.SourceSchema, transfer.SourceTable, source)
		if err != nil {
			return fmt.Errorf("error getting source table column infos :: %v", err)
		}
		query = fmt.Sprintf(`SELECT * FROM %v`, escapedSourceSchemaPeriodTable)

	}

	rows, err := source.query(query)
	if err != nil {
		return fmt.Errorf("error querying source :: %v", err)
	}
	defer rows.Close()

	if transfer.Query != "" {
		columnInfos, err = getQueryColumnInfos(rows, source)
		if err != nil {
			return fmt.Errorf("error getting query column infos :: %v", err)
		}
	}

	if transfer.CreateTargetTableIfNotExists {
		err = createTableIfNotExists(transfer.TargetSchema, transfer.TargetTable, columnInfos, target, false)
		if err != nil {
			return fmt.Errorf("error creating target table :: %v", err)
		}
	}

	newPipeFiles := createPipeFiles(columnInfos, transfer, rows, source, target, false)

	pksProcessedPipeFiles := deletePks(newPipeFiles, columnInfos, transfer, target, false, initialLoad)

	err = insertPipeFiles(pksProcessedPipeFiles, transfer, columnInfos, target, "")
	if err != nil {
		return fmt.Errorf("error inserting pipe files :: %v", err)
	}

	transferMap.SetStatus(transfer.Id, StatusComplete, transfer)
	infoLog.Printf("transfer %v complete", transfer.Id)

	return nil
}

func createTransferTmpDirs(transferId string) (tmpDir, pipeFileDir, finalCsvDir string, err error) {
	tmpDir = filepath.Join(globalTmpDir, transferId)

	err = os.MkdirAll(tmpDir, 0600)
	if err != nil {
		return tmpDir, pipeFileDir, finalCsvDir, fmt.Errorf("error creating temp dir :: %v", err)
	}

	infoLog.Printf("temp dir %v created", tmpDir)

	pipeFileDir = filepath.Join(tmpDir, "pipe-files")
	err = os.MkdirAll(pipeFileDir, 0600)
	if err != nil {
		go func() {
			err = os.RemoveAll(tmpDir)
			if err != nil {
				errorLog.Printf("error removing temp dir %v :: %v", tmpDir, err)
				return
			}
			infoLog.Printf("temp dir %v removed because pipe file dir creation failed", tmpDir)
		}()
		return tmpDir, pipeFileDir, finalCsvDir, fmt.Errorf("error creating pipe file dir :: %v", err)
	}

	infoLog.Printf("pipe file dir %v created", pipeFileDir)

	finalCsvDir = filepath.Join(tmpDir, "final-csv")
	err = os.MkdirAll(finalCsvDir, 0600)
	if err != nil {
		go func() {
			err = os.RemoveAll(tmpDir)
			if err != nil {
				errorLog.Printf("error removing temp dir %v :: %v", tmpDir, err)
				return
			}
			infoLog.Printf("temp dir %v removed because final csv dir creation failed", tmpDir)
		}()
		return tmpDir, pipeFileDir, finalCsvDir, fmt.Errorf("error creating final csv dir :: %v", err)
	}

	infoLog.Printf("final csv dir %v created", finalCsvDir)

	return tmpDir, pipeFileDir, finalCsvDir, nil
}
