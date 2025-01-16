package main

import (
	"fmt"

	"github.com/sqlpipe/sqlpipe/internal/commonHelpers"
	"github.com/sqlpipe/sqlpipe/internal/data"
)

func runTransfer(transferInfo data.TransferInfo) error {

	var err error

	transferInfo.TmpDir, transferInfo.PipeFileDir, transferInfo.FinalCsvDir, err = commonHelpers.CreateTransferTmpDirs(transferInfo.ID, globalTmpDir, logger)
	if err != nil {
		return fmt.Errorf("error creating transfer tmp dirs :: %v", err)
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
		Type:     transferInfo.SourceInstance.Type,
		Hostname: transferInfo.SourceInstance.Host,
		Port:     transferInfo.SourceInstance.Port,
		Database: transferInfo.SourceDatabase,
		Username: transferInfo.SourceInstance.Username,
		Password: transferInfo.SourceInstance.Password,
	}

	source, err := newSystem(sourceConnectionInfo)
	if err != nil {
		return fmt.Errorf("error creating source system :: %v", err)
	}
	defer source.closeConnectionPool(true)

	targetConnectionInfo := ConnectionInfo{
		Type:     transferInfo.TargetType,
		Hostname: transferInfo.TargetHost,
		Port:     transferInfo.TargetPort,
		Username: transferInfo.TargetUsername,
		Password: transferInfo.TargetPassword,
	}

	target, err := newSystem(targetConnectionInfo)
	if err != nil {
		return fmt.Errorf("error creating target system :: %v", err)
	}
	defer target.closeConnectionPool(true)

	err = createDbIfNotExists(transferInfo, target)
	if err != nil {
		return fmt.Errorf("error creating target database :: %v", err)
	}

	err = createStagingDbIfNotExists(transferInfo, target)
	if err != nil {
		return fmt.Errorf("error creating staging database :: %v", err)
	}

	defer func() {
		dropStagingDbQuery := fmt.Sprintf(`DROP DATABASE IF EXISTS %v`, transferInfo.StagingDbName)
		err = target.exec(dropStagingDbQuery)
		if err != nil {
			logger.Error("error dropping staging database", "error", err)
		}
	}()

	targetConnectionInfo.Database = transferInfo.StagingDbName

	target, err = newSystem(targetConnectionInfo)
	if err != nil {
		return fmt.Errorf("error creating target system :: %v", err)
	}
	defer target.closeConnectionPool(true)

	if target.schemaRequired() && transferInfo.CreateTargetSchemaIfNotExists {
		err = createSchemaIfNotExists(transferInfo, target)
		if err != nil {
			return fmt.Errorf("error creating target schema :: %v", err)
		}
	}

	if transferInfo.DropTargetTableIfExists {
		err = dropTableIfExists(transferInfo, target)
		if err != nil {
			return fmt.Errorf("error dropping target table :: %v", err)
		}
	}

	escapedSourceSchemaPeriodTable := getSchemaPeriodTable(transferInfo.SourceSchema, transferInfo.SourceTable, source, true)
	query := transferInfo.Query

	var columnInfos []ColumnInfo

	if transferInfo.SourceTable != "" {

		columnInfos, err = getTableColumnInfos(transferInfo.SourceSchema, transferInfo.SourceTable, source)
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

	if transferInfo.Query != "" {
		columnInfos, err = getQueryColumnInfos(rows, source)
		if err != nil {
			return fmt.Errorf("error getting query column infos :: %v", err)
		}
	}

	if transferInfo.CreateTargetTableIfNotExists {
		err = createTableIfNotExists(transferInfo, columnInfos, target)
		if err != nil {
			return fmt.Errorf("error creating target table :: %v", err)
		}
	}

	newPipeFiles := createPipeFiles(columnInfos, transferInfo, rows, source)

	err = insertPipeFiles(newPipeFiles, transferInfo, columnInfos, target, "")
	if err != nil {
		return fmt.Errorf("error inserting pipe files :: %v", err)
	}

	// drop the target db name if exists, replace with the staging db name
	targetConnectionInfo.Database = transferInfo.TargetDatabase

	target, err = newSystem(targetConnectionInfo)
	if err != nil {
		return fmt.Errorf("error creating target system :: %v", err)
	}

	err = createSchemaIfNotExists(transferInfo, target)
	if err != nil {
		return fmt.Errorf("error creating target schema :: %v", err)
	}

	err = dropTableIfExists(transferInfo, target)
	if err != nil {
		return fmt.Errorf("error dropping target table :: %v", err)
	}

	err = target.exec(fmt.Sprintf(`ALTER TABLE %v.%v.%v RENAME TO %v.%v.%v`, transferInfo.StagingDbName, transferInfo.TargetSchema, transferInfo.TargetTable, transferInfo.TargetDatabase, transferInfo.TargetSchema, transferInfo.TargetTable))
	if err != nil {
		return fmt.Errorf("error renaming staging table to target table :: %v", err)
	}

	logger.Info("transfer complete", "transfer-id", transferInfo.ID)

	return nil
}
