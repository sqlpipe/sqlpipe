package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/sqlpipe/sqlpipe/internal/commonHelpers"
)

func runTransfer() error {

	var err error

	transferInfo.TmpDir, transferInfo.PipeFileDir, transferInfo.FinalCsvDir, err = commonHelpers.CreateTransferTmpDirs(transferInfo.Id, globalTmpDir, logger)
	if err != nil {
		logger.Error(fmt.Sprintf("error creating transfer tmp dirs :: %v", err))
	}

	if transferInfo.Delimiter == "" {
		transferInfo.Delimiter = "{dlm}"
	}
	if transferInfo.Newline == "" {
		transferInfo.Newline = "{nwln}"
	}
	if transferInfo.Null == "" {
		transferInfo.Null = "{nll}"
		if transferInfo.TargetType == "mysql" {
			transferInfo.Null = `NULL`
		}
	}

	sourceConnectionInfo := ConnectionInfo{
		Name:             transferInfo.SourceName,
		Type:             transferInfo.SourceType,
		ConnectionString: transferInfo.SourceConnectionString,
	}

	source, err := newSystem(sourceConnectionInfo)
	if err != nil {
		return fmt.Errorf("error creating source system :: %v", err)
	}
	defer source.closeConnectionPool(true)

	targetConnectionInfo := ConnectionInfo{
		Name:             transferInfo.TargetName,
		Type:             transferInfo.TargetType,
		ConnectionString: transferInfo.TargetConnectionString,
	}

	target, err := newSystem(targetConnectionInfo)
	if err != nil {
		return fmt.Errorf("error creating target system :: %v", err)
	}
	defer target.closeConnectionPool(true)

	if target.schemaRequired() && transferInfo.CreateTargetSchemaIfNotExists {
		err = createSchemaIfNotExists(transferInfo.TargetSchema, target)
		if err != nil {
			return fmt.Errorf("error creating target schema :: %v", err)
		}
	}

	if transferInfo.DropTargetTableIfExists {
		err = dropTableIfExists(transferInfo.TargetSchema, transferInfo.TargetTable, target)
		if err != nil {
			return fmt.Errorf("error dropping target table :: %v", err)
		}
	}

	escapedSourceSchemaPeriodTable := getSchemaPeriodTable(transferInfo.SourceSchema, transferInfo.SourceTable, source, true)
	query := transferInfo.Query
	incremental := false
	initialLoad := true
	var incrementalTime time.Time
	var columnInfos []ColumnInfo
	var incrementalColumnInfo ColumnInfo

	if transferInfo.SourceTable != "" {

		columnInfos, err = getTableColumnInfos(transferInfo.SourceSchema, transferInfo.SourceTable, source)
		if err != nil {
			return fmt.Errorf("error getting source table column infos :: %v", err)
		}

		if transferInfo.IncrementalColumn != "" {
			incremental = true

			// check for existence of incremental column in columnInfos
			var found bool
			for _, columnInfo := range columnInfos {
				if strings.EqualFold(columnInfo.Name, transferInfo.IncrementalColumn) {
					found = true
					incrementalColumnInfo = columnInfo
					break
				}
			}
			if !found {
				return fmt.Errorf("incremental column %v not found in source table %v", transferInfo.IncrementalColumn, transferInfo.SourceTable)
			}

			initialLoad, incrementalTime, err = getIncrementalTime(transferInfo.TargetSchema, transferInfo.TargetTable, transferInfo.IncrementalColumn, initialLoad, target)
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

			query = fmt.Sprintf(`SELECT * FROM %v WHERE %v > %v`, escapedSourceSchemaPeriodTable, transferInfo.IncrementalColumn, timeStringVal)
		}
	}

	vacuumTableName := ""
	schemaPeriodVacuumTableName := ""

	if transferInfo.Vacuum {

		randomLetters, err := RandomLetters(16)
		if err != nil {
			return fmt.Errorf("error generating random letters :: %v", err)
		}

		vacuumTableName = fmt.Sprintf("sqlpipe_vacuum_%v_%v", randomLetters, transferInfo.TargetTable)

		// shorten vacuum table name to 64 chars if necessary
		if len(vacuumTableName) > 64 {
			vacuumTableName = vacuumTableName[:64]
		}

		schemaPeriodVacuumTableName = getSchemaPeriodTable(transferInfo.TargetSchema, vacuumTableName, target, true)

		columnInfos, err = getTableColumnInfos(transferInfo.SourceSchema, transferInfo.SourceTable, source)
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

		err = createTableIfNotExists(transferInfo.TargetSchema, vacuumTableName, columnInfos, target, incremental)
		if err != nil {
			return fmt.Errorf("error creating target table :: %v", err)
		}

		err = source.exec(fmt.Sprintf(`DELETE FROM %v`, schemaPeriodVacuumTableName))
		if err != nil {
			return fmt.Errorf("error deleting rows from source table :: %v", err)
		}

		if !transferInfo.KeepFiles {
			defer func() {
				err = dropTableIfExists(transferInfo.TargetSchema, vacuumTableName, target)
				if err != nil {
					logger.Error(fmt.Sprintf("error dropping vacuum table %v :: %v", vacuumTableName, err))
					return
				}
				logger.Info(fmt.Sprintf("vacuum table %v dropped", vacuumTableName))
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

	if transferInfo.Query != "" {
		columnInfos, err = getQueryColumnInfos(rows, source)
		if err != nil {
			return fmt.Errorf("error getting query column infos :: %v", err)
		}
	}

	if transferInfo.CreateTargetTableIfNotExists {
		err = createTableIfNotExists(transferInfo.TargetSchema, transferInfo.TargetTable, columnInfos, target, incremental)
		if err != nil {
			return fmt.Errorf("error creating target table :: %v", err)
		}
	}

	newPipeFiles := createPipeFiles(columnInfos, transferInfo, rows, source, incremental)

	pksProcessedPipeFiles := deletePks(newPipeFiles, columnInfos, transferInfo, target, incremental, initialLoad)

	err = insertPipeFiles(pksProcessedPipeFiles, transferInfo, columnInfos, target, "")
	if err != nil {
		return fmt.Errorf("error inserting pipe files :: %v", err)
	}

	if transferInfo.Vacuum {

		escapedTargetSchemaPeriodTable := getSchemaPeriodTable(transferInfo.TargetSchema, transferInfo.TargetTable, target, true)

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

	logger.Info(fmt.Sprintf("transfer %v complete", transferInfo.Id))

	return nil
}
