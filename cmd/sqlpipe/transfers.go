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
	IncrementalColumn             string             `json:"incremental-column,omitempty"`
	Vacuum                        bool               `json:"vacuum"`
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
		CreateTargetTableIfNotExists  bool   `json:"create-target-table-if-not-exists"`
		CreateTargetSchemaIfNotExists bool   `json:"create-target-schema-if-not-exists"`
		SourceSchema                  string `json:"source-schema"`
		SourceTable                   string `json:"source-table"`
		TargetSchema                  string `json:"target-schema"`
		TargetTable                   string `json:"target-table"`
		Query                         string `json:"query"`
		Delimiter                     string `json:"delimiter"`
		Newline                       string `json:"newline"`
		Null                          string `json:"null"`
		IncrementalColumn             string `json:"incremental-column"`
		Vacuum                        bool   `json:"vacuum"`
	}

	err := readJSON(w, r, &input)
	if err != nil {
		clientErrorResponse(w, http.StatusBadRequest, err)
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
		IncrementalColumn:             input.IncrementalColumn,
		Vacuum:                        input.Vacuum,
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
		v.check(!transfer.Vacuum, "vacuum", "must not be true if query is provided")
	}

	if transfer.SourceSchema == "" && transfer.SourceTable == "" {
		v.check(transfer.Query != "", "query", "must be provided if source-schema and source-table are not provided")
	}

	if transfer.Vacuum {
		v.check(transfer.Query == "", "query", "must not be provided if vacuum is true")
		v.check(transfer.SourceSchema != "", "source-schema", "must be provided if vacuum is true")
		v.check(transfer.SourceTable != "", "source-table", "must be provided if vacuum is true")
		v.check(transfer.IncrementalColumn == "", "incremental-column", "must not be provided if vacuum is true")
		v.check(!transfer.CreateTargetTableIfNotExists, "create-target-table-if-not-exists", "must not be true if vacuum is true")
		v.check(!transfer.DropTargetTableIfExists, "drop-target-table-if-exists", "must not be true if vacuum is true")
	}

	if transfer.SourceSchema != "" || transfer.SourceTable != "" {
		v.check(transfer.Query == "", "query", "must not be provided if source-schema or source-table are provided")
	}

	if transfer.IncrementalColumn != "" {
		if schemaRequired[transfer.SourceConnectionInfo.Type] {
			v.check(transfer.SourceSchema != "", "source-schema", fmt.Sprintf("must be provided for source type %v if incremental-column is provided", transfer.TargetConnectionInfo.Type))
		}
		v.check(transfer.SourceTable != "", "source-table", "must be provided if incremental-column is provided")
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
	}

	if !v.valid() {
		failedValidationResponse(w, v.errors)
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
		failedValidationResponse(w, v.errors)
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
		clientErrorResponse(w, http.StatusBadRequest,
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
	incremental := false
	initialLoad := true
	var incrementalTime time.Time
	var columnInfos []ColumnInfo
	var incrementalColumnInfo ColumnInfo

	if transfer.SourceTable != "" {

		columnInfos, err = getTableColumnInfos(transfer.SourceSchema, transfer.SourceTable, source)
		if err != nil {
			return fmt.Errorf("error getting source table column infos :: %v", err)
		}

		if transfer.IncrementalColumn != "" {
			incremental = true

			// check for existence of incremental column in columnInfos
			var found bool
			for _, columnInfo := range columnInfos {
				if strings.EqualFold(columnInfo.Name, transfer.IncrementalColumn) {
					found = true
					incrementalColumnInfo = columnInfo
					break
				}
			}
			if !found {
				return fmt.Errorf("incremental column %v not found in source table %v", transfer.IncrementalColumn, transfer.SourceTable)
			}

			initialLoad, incrementalTime, err = getIncrementalTime(transfer.TargetSchema, transfer.TargetTable, transfer.IncrementalColumn, initialLoad, target)
			if err != nil {
				return fmt.Errorf("error getting incremental time :: %v", err)
			}
		}

		if initialLoad {
			query = fmt.Sprintf(`SELECT * FROM %v`, escapedSourceSchemaPeriodTable)
		} else {

			sqlFormatters := source.getSqlFormatters()

			timeStringVal, err := sqlFormatters[incrementalColumnInfo.PipeType](incrementalTime.Format(time.RFC3339Nano))
			if err != nil {
				return fmt.Errorf("error formatting incremental time :: %v", err)
			}

			query = fmt.Sprintf(`SELECT * FROM %v WHERE %v > %v`, escapedSourceSchemaPeriodTable, transfer.IncrementalColumn, timeStringVal)
		}
	}

	vacuumTableName := ""
	schemaPeriodVacuumTableName := ""

	if transfer.Vacuum {

		randomLetters, err := RandomLetters(16)
		if err != nil {
			return fmt.Errorf("error generating random letters :: %v", err)
		}

		vacuumTableName = fmt.Sprintf("sqlpipe_vacuum_%v_%v", randomLetters, transfer.TargetTable)

		// shorten vacuum table name to 64 chars if necessary
		if len(vacuumTableName) > 64 {
			vacuumTableName = vacuumTableName[:64]
		}

		schemaPeriodVacuumTableName = getSchemaPeriodTable(transfer.TargetSchema, vacuumTableName, target, true)

		columnInfos, err = getTableColumnInfos(transfer.SourceSchema, transfer.SourceTable, source)
		if err != nil {
			return fmt.Errorf("error getting source table column infos :: %v", err)
		}

		pkColumnInfos := []ColumnInfo{}

		for _, columnInfo := range columnInfos {
			if columnInfo.IsPrimaryKey {
				pkColumnInfos = append(pkColumnInfos, columnInfo)
			}
		}

		columnInfos = pkColumnInfos

		err = createTableIfNotExists(transfer.TargetSchema, vacuumTableName, columnInfos, target, incremental)
		if err != nil {
			return fmt.Errorf("error creating target table :: %v", err)
		}

		err = source.exec(fmt.Sprintf(`DELETE FROM %v`, schemaPeriodVacuumTableName))
		if err != nil {
			return fmt.Errorf("error deleting rows from source table :: %v", err)
		}

		if !transfer.KeepFiles {
			defer func() {
				err = dropTableIfExists(transfer.TargetSchema, vacuumTableName, target)
				if err != nil {
					errorLog.Printf("error dropping vacuum table %v :: %v", vacuumTableName, err)
					return
				}
				infoLog.Printf("vacuum table %v dropped", vacuumTableName)
			}()
		}

		queryBuilder := strings.Builder{}

		queryBuilder.WriteString("select ")

		for i, columnInfo := range columnInfos {
			if i != 0 {
				queryBuilder.WriteString(", ")
			}
			queryBuilder.WriteString(columnInfo.Name)
		}

		queryBuilder.WriteString(fmt.Sprintf(" from %v", escapedSourceSchemaPeriodTable))

		query = queryBuilder.String()
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
		err = createTableIfNotExists(transfer.TargetSchema, transfer.TargetTable, columnInfos, target, incremental)
		if err != nil {
			return fmt.Errorf("error creating target table :: %v", err)
		}
	}

	newPipeFiles := createPipeFiles(columnInfos, transfer, rows, source, incremental)

	pksProcessedPipeFiles := deletePks(newPipeFiles, columnInfos, transfer, target, incremental, initialLoad)

	err = insertPipeFiles(pksProcessedPipeFiles, transfer, columnInfos, target, "")
	if err != nil {
		return fmt.Errorf("error inserting pipe files :: %v", err)
	}

	if transfer.Vacuum {

		escapedTargetSchemaPeriodTable := getSchemaPeriodTable(transfer.TargetSchema, transfer.TargetTable, target, true)

		queryBuilder := strings.Builder{}

		queryBuilder.WriteString(`DELETE FROM `)
		queryBuilder.WriteString(escapedTargetSchemaPeriodTable)
		queryBuilder.WriteString(` where (`)
		for i, columnInfo := range columnInfos {
			escapedColumnName := escapeIfNeeded(columnInfo.Name, target)
			if i != 0 {
				queryBuilder.WriteString(", ")
			}

			queryBuilder.WriteString(escapedColumnName)
		}
		queryBuilder.WriteString(`) not in (select `)
		for i, columnInfo := range columnInfos {
			escapedColumnName := escapeIfNeeded(columnInfo.Name, target)
			if i != 0 {
				queryBuilder.WriteString(", ")
			}

			queryBuilder.WriteString(escapedColumnName)
		}
		queryBuilder.WriteString(` from `)
		queryBuilder.WriteString(schemaPeriodVacuumTableName)
		queryBuilder.WriteString(`)`)

		query = queryBuilder.String()

		err = target.exec(query)
		if err != nil {
			return fmt.Errorf("error vacuuming target table :: %v", err)
		}
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

type CliTransferInput struct {
	KeepFiles                     bool
	SourceName                    string
	SourceType                    string
	SourceConnectionString        string
	TargetName                    string
	TargetType                    string
	TargetConnectionString        string
	TargetHostname                string
	TargetPort                    int
	TargetDatabase                string
	TargetUsername                string
	TargetPassword                string
	DropTargetTableIfExists       bool
	CreateTargetSchemaIfNotExists bool
	CreateTargetTableIfNotExists  bool
	SourceSchema                  string
	SourceTable                   string
	TargetSchema                  string
	TargetTable                   string
	Query                         string
	Delimiter                     string
	Newline                       string
	Null                          string
}

func handleCliTransfer(cliTransferInput CliTransferInput) {

	id := uuid.New().String()

	tmpDir, pipeFileDir, finalCsvDir, err := createTransferTmpDirs(id)
	if err != nil {
		errorLog.Fatalf("error creating transfer tmp dirs :: %v", err)
	}

	if cliTransferInput.Delimiter == "" {
		cliTransferInput.Delimiter = "{dlm}"
	}
	if cliTransferInput.Newline == "" {
		cliTransferInput.Newline = "{nwln}"
	}
	if cliTransferInput.Null == "" {
		cliTransferInput.Null = "{nll}"
		if cliTransferInput.TargetType == TypeMySQL {
			cliTransferInput.Null = `NULL`
		}
	}

	sourceConnectionInfo := ConnectionInfo{
		Name:             cliTransferInput.SourceName,
		Type:             cliTransferInput.SourceType,
		ConnectionString: cliTransferInput.SourceConnectionString,
	}

	targetConnectionInfo := ConnectionInfo{
		Name:             cliTransferInput.TargetName,
		Type:             cliTransferInput.TargetType,
		ConnectionString: cliTransferInput.TargetConnectionString,
		Hostname:         cliTransferInput.TargetHostname,
		Port:             cliTransferInput.TargetPort,
		Database:         cliTransferInput.TargetDatabase,
		Username:         cliTransferInput.TargetUsername,
		Password:         cliTransferInput.TargetPassword,
	}

	ctx, cancel := context.WithCancel(context.Background())

	transfer := Transfer{
		Id:                            id,
		CreatedAt:                     time.Now(),
		Status:                        StatusQueued,
		KeepFiles:                     cliTransferInput.KeepFiles,
		TmpDir:                        tmpDir,
		PipeFileDir:                   pipeFileDir,
		FinalCsvDir:                   finalCsvDir,
		Delimiter:                     cliTransferInput.Delimiter,
		Newline:                       cliTransferInput.Newline,
		Null:                          cliTransferInput.Null,
		Context:                       ctx,
		Cancel:                        cancel,
		SourceConnectionInfo:          sourceConnectionInfo,
		TargetConnectionInfo:          targetConnectionInfo,
		DropTargetTableIfExists:       cliTransferInput.DropTargetTableIfExists,
		CreateTargetSchemaIfNotExists: cliTransferInput.CreateTargetSchemaIfNotExists,
		CreateTargetTableIfNotExists:  cliTransferInput.CreateTargetTableIfNotExists,
		SourceSchema:                  cliTransferInput.SourceSchema,
		SourceTable:                   cliTransferInput.SourceTable,
		TargetSchema:                  cliTransferInput.TargetSchema,
		TargetTable:                   cliTransferInput.TargetTable,
		Query:                         cliTransferInput.Query,
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
		errorLog.Fatalf("error validating transfer :: %v", v.errors)
	}

	transferMap.Set(transfer.Id, transfer)

	infoLog.Printf(`created transfer %v from %v to %v`,
		transfer.Id, cliTransferInput.SourceName, cliTransferInput.TargetName)

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
		errorLog.Fatalf(transfer.Error)
	}
}
