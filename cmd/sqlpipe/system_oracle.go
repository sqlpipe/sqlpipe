package main

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

type Oracle struct {
	Name       string
	Connection *sql.DB
}

func (system Oracle) getSystemName() (name string) {
	return system.Name
}

func newOracle(connectionInfo ConnectionInfo) (oracle Oracle, err error) {
	db, err := openConnectionPool(connectionInfo.Name, connectionInfo.ConnectionString, DriverOracle)
	if err != nil {
		return oracle, fmt.Errorf("error opening oracle db :: %v", err)
	}
	oracle.Connection = db
	oracle.Name = connectionInfo.Name
	return oracle, nil
}

// func (system Oracle) getConnectionPool() (db *sql.DB) {
// 	return system.Connection
// }

func (system Oracle) query(query string) (rows *sql.Rows, err error) {
	rows, err = system.Connection.Query(query)
	if err != nil {
		return nil, fmt.Errorf("error running dql on %v :: %v :: %v", system.Name, query, err)
	}
	return rows, nil
}

func (system Oracle) queryRow(query string) (row *sql.Row) {
	row = system.Connection.QueryRow(query)
	return row
}

func (system Oracle) exec(query string) (err error) {
	_, err = system.Connection.Exec(query)
	if err != nil {
		return fmt.Errorf("error running ddl/dml on %v :: %v :: %v", system.Name, query, err)
	}
	return nil
}

func (system Oracle) dropTableIfExistsOverride(schema, table string) (overridden bool, err error) {

	dropped := getSchemaPeriodTable(schema, table, system, true)

	query := fmt.Sprintf("drop table %v", dropped)
	err = system.exec(query)
	if err != nil {
		if !strings.Contains(err.Error(), "ORA-00942") {
			return true, fmt.Errorf("error dropping %v :: %v", dropped, err)
		}
	}

	return true, nil
}

func (system Oracle) createTableIfNotExistsOverride(schema, table string, columnInfos []ColumnInfo, incremental bool) (overridden bool, err error) {

	escapedSchemaPeriodTable := getSchemaPeriodTable(schema, table, system, true)

	queryBuilder := strings.Builder{}

	queryBuilder.WriteString("declare v_exists number(1); begin select count(*) into v_exists from all_tables where table_name = upper('")
	queryBuilder.WriteString(table)
	queryBuilder.WriteString("') and owner = upper('")
	queryBuilder.WriteString(schema)
	queryBuilder.WriteString("'); if v_exists = 0 then execute immediate 'create table ")
	queryBuilder.WriteString(escapedSchemaPeriodTable)
	queryBuilder.WriteString(" (")

	for i, columnInfo := range columnInfos {
		createType, err := system.pipeTypeToCreateType(columnInfo)
		if err != nil {
			return false, fmt.Errorf("error getting pipe type :: %v", err)
		}

		if i > 0 {
			queryBuilder.WriteString(", ")
		}
		queryBuilder.WriteString(columnInfo.Name)
		queryBuilder.WriteString(" ")
		queryBuilder.WriteString(createType)
	}

	queryBuilder.WriteString(")'; end if; exception when others then raise; end;")

	query := queryBuilder.String()

	err = system.exec(query)
	if err != nil {
		return false, fmt.Errorf("error creating table :: %v", err)
	}

	return true, nil
}

func (system Oracle) driverTypeToPipeType(
	columnType *sql.ColumnType,
	databaseTypeName string,
) (
	pipeType string,
	err error,
) {
	switch databaseTypeName {
	case "NCHAR":
		pipeType = "nvarchar"
	case "CHAR":
		pipeType = "nvarchar"
	case "OCIClobLocator":
		pipeType = "ntext"
	case "LONG":
		pipeType = "ntext"
	case "IBDouble":
		pipeType = "float64"
	case "IBFloat":
		pipeType = "float32"
	case "NUMBER":
		pipeType = "decimal"
	case "TimeStampDTY":
		pipeType = "datetime"
	case "TimeStampTZ_DTY":
		pipeType = "datetimetz"
	case "TimeStampLTZ_DTY":
		pipeType = "datetimetz"
	case "DATE":
		pipeType = "date"
	case "RAW":
		pipeType = "varbinary"
	case "OCIBlobLocator":
		pipeType = "blob"
	case "OCIFileLocator":
		pipeType = "blob"
	case "ROWID":
		pipeType = "nvarchar"
	case "UROWID":
		pipeType = "nvarchar"
	case "IntervalYM_DTY":
		pipeType = "nvarchar"
	case "IntervalDS_DTY":
		pipeType = "nvarchar"
	default:
		return "", fmt.Errorf(
			"unsupported database type for oracle: %v", columnType.DatabaseTypeName())
	}

	return pipeType, nil
}

func (system Oracle) pipeTypeToCreateType(columnInfo ColumnInfo) (createType string, err error) {

	length := columnInfo.Length * 4

	switch columnInfo.PipeType {
	case "nvarchar", "varchar":
		if columnInfo.LengthOk {
			if length <= 0 {
				createType = "varchar2(4000)"
			} else if length <= 4000 {
				createType = fmt.Sprintf("varchar2(%v)", length)
			} else {
				createType = "clob"
			}
		} else {
			createType = "varchar2(4000)"
		}
	case "ntext", "text":
		createType = "clob"
	case "int64":
		createType = "number(19)"
	case "int32":
		createType = "number(10)"
	case "int16":
		createType = "number(5)"
	case "float64":
		createType = "BINARY_DOUBLE"
	case "float32":
		createType = "BINARY_FLOAT"
	case "decimal":

		precisionOk := false
		scaleOk := false

		if columnInfo.DecimalOk {
			if columnInfo.Precision >= 0 && columnInfo.Precision <= 38 {
				precisionOk = true
			}
			if columnInfo.Scale >= 0 && columnInfo.Scale <= 38 {
				scaleOk = true
			}
		}

		if precisionOk && scaleOk {
			createType = fmt.Sprintf("decimal(%v, %v)", columnInfo.Precision, columnInfo.Scale)
		} else {
			createType = "BINARY_DOUBLE"
		}

	case "money":
		if columnInfo.DecimalOk {
			createType = fmt.Sprintf("decimal(%v, %v)", columnInfo.Precision, columnInfo.Scale)
		} else {
			createType = fmt.Sprintf("decimal(%v, %v)", 38, 4)
		}
	case "datetime":
		createType = "timestamp"
	case "datetimetz":
		createType = "timestamp"
	case "date":
		createType = "date"
	case "time":
		createType = "varchar2(256)"
	case "varbinary":
		if columnInfo.LengthOk {
			if columnInfo.Length <= 0 {
				createType = "raw(2000)"
			} else if columnInfo.Length <= 2000 {
				createType = fmt.Sprintf("raw(%v)", columnInfo.Length)
			} else {
				createType = "blob"
			}
		} else {
			createType = "raw(2000)"
		}
	case "blob":
		createType = "blob"
	case "uuid":
		createType = "raw(16)"
	case "bool":
		createType = "number(1)"
	case "json":
		createType = "clob"
	case "xml":
		if columnInfo.LengthOk {
			if length <= 0 {
				createType = "varchar2(4000)"
			} else if length <= 4000 {
				createType = fmt.Sprintf("varchar2(%v)", length)
			} else {
				createType = "clob"
			}
		} else {
			createType = "varchar2(4000)"
		}
	case "varbit":
		if columnInfo.LengthOk {
			if length <= 0 {
				createType = "varchar2(4000)"
			} else if length <= 4000 {
				createType = fmt.Sprintf("varchar2(%v)", length)
			} else {
				createType = "clob"
			}
		} else {
			createType = "varchar2(4000)"
		}
	default:
		return "", fmt.Errorf("unsupported pipe type for oracle: %v", columnInfo.PipeType)
	}

	return createType, nil
}

func (system Oracle) createPipeFilesOverride(pipeFileChannelIn chan PipeFileInfo, columnInfo []ColumnInfo, transfer Transfer, rows *sql.Rows,
) (pipeFileInfoChannel chan PipeFileInfo, overridden bool) {
	return pipeFileChannelIn, false
}

func (system Oracle) getPipeFileFormatters() (
	pipeFileFormatters map[string]func(interface{}) (pipeFileValue string, err error),
) {
	return map[string]func(interface{}) (pipeFileValue string, err error){
		"nvarchar": func(v interface{}) (pipeFileValue string, err error) {
			return fmt.Sprintf("%s", v), nil
		},
		"varchar": func(v interface{}) (pipeFileValue string, err error) {
			return fmt.Sprintf("%s", v), nil
		},
		"ntext": func(v interface{}) (pipeFileValue string, err error) {
			return fmt.Sprintf("%s", v), nil
		},
		"text": func(v interface{}) (pipeFileValue string, err error) {
			return fmt.Sprintf("%s", v), nil
		},
		"int64": func(v interface{}) (pipeFileValue string, err error) {
			return fmt.Sprintf("%d", v), nil
		},
		"int32": func(v interface{}) (pipeFileValue string, err error) {
			return fmt.Sprintf("%d", v), nil
		},
		"int16": func(v interface{}) (pipeFileValue string, err error) {
			return fmt.Sprintf("%d", v), nil
		},
		"float64": func(v interface{}) (pipeFileValue string, err error) {
			return fmt.Sprintf("%v", v), nil
		},
		"float32": func(v interface{}) (pipeFileValue string, err error) {
			return fmt.Sprintf("%v", v), nil
		},
		"decimal": func(v interface{}) (pipeFileValue string, err error) {
			return fmt.Sprintf("%s", v), nil
		},
		"money": func(v interface{}) (pipeFileValue string, err error) {
			return fmt.Sprintf("%s", v), nil
		},
		"datetime": func(v interface{}) (pipeFileValue string, err error) {
			valTime, ok := v.(time.Time)
			if !ok {
				return "", errors.New(
					"non time.Time value passed to datetime oraclePipeFileFormatters")
			}
			return valTime.Format(time.RFC3339Nano), nil
		},
		"datetimetz": func(v interface{}) (pipeFileValue string, err error) {
			valTime, ok := v.(time.Time)
			if !ok {
				return "", errors.New(
					"non time.Time value passed to datetimetz oraclePipeFileFormatters")
			}
			return valTime.UTC().Format(time.RFC3339Nano), nil
		},
		"date": func(v interface{}) (pipeFileValue string, err error) {
			valTime, ok := v.(time.Time)
			if !ok {
				return "", errors.New(
					"non time.Time value passed to date oraclePipeFileFormatters")
			}
			return valTime.Format(time.RFC3339Nano), nil
		},
		"time": func(v interface{}) (pipeFileValue string, err error) {
			return fmt.Sprintf("%s", v), nil
		},
		"varbinary": func(v interface{}) (pipeFileValue string, err error) {
			return fmt.Sprintf("%x", v), nil
		},
		"blob": func(v interface{}) (pipeFileValue string, err error) {
			return fmt.Sprintf("%x", v), nil
		},
		"uuid": func(v interface{}) (pipeFileValue string, err error) {
			return fmt.Sprintf("%s", v), nil
		},
		"bool": func(v interface{}) (pipeFileValue string, err error) {
			return fmt.Sprintf("%t", v), nil
		},
		"json": func(v interface{}) (pipeFileValue string, err error) {
			return fmt.Sprintf("%s", v), nil
		},
		"xml": func(v interface{}) (pipeFileValue string, err error) {
			return fmt.Sprintf("%s", v), nil
		},
	}
}

func (system Oracle) insertPipeFilesOverride(columnInfos []ColumnInfo, transfer Transfer, pipeFileInfoChannel <-chan PipeFileInfo, vacuumTable string) (overridden bool, err error) {
	finalCsvChannel := convertPipeFiles(pipeFileInfoChannel, columnInfos, transfer, system)

	table := transfer.TargetTable

	finalCsvPlusCtlFileChannel := system.createCtlFiles(finalCsvChannel, transfer, columnInfos, table)

	err = insertFinalCsvs(finalCsvPlusCtlFileChannel, transfer, system, transfer.TargetSchema, table)
	if err != nil {
		return true, fmt.Errorf("error inserting final csvs :: %v", err)
	}

	return true, nil
}
func (system Oracle) convertPipeFilesOverride(pipeFilePath <-chan PipeFileInfo, finalCsvInfoChannelIn chan FinalCsvInfo, transfer Transfer, columnInfos []ColumnInfo,
) (finalCsvInfoChannel chan FinalCsvInfo, overridden bool) {
	return finalCsvInfoChannelIn, false
}

func (system Oracle) createCtlFiles(finalCsvsIn <-chan FinalCsvInfo, transfer Transfer, columnInfos []ColumnInfo, table string) <-chan FinalCsvInfo {

	finalCsvChannelOut := make(chan FinalCsvInfo)

	go func() {
		defer close(finalCsvChannelOut)

		for finalCsvInfo := range finalCsvsIn {

			finalCsvFile, err := os.Open(finalCsvInfo.FilePath)
			if err != nil {
				transfer.Error = fmt.Sprintf("error opening final csv file :: %v", err)
				transferMap.CancelAndSetStatus(transfer.Id, transfer, StatusError)
				errorLog.Println(transfer.Error)
				return
			}

			defer func() {
				finalCsvFile.Close()
			}()

			oracleCtlFile, err := os.Create(filepath.Join(transfer.FinalCsvDir,
				fmt.Sprintf("%v.ctl", strings.TrimSuffix(filepath.Base(finalCsvInfo.FilePath), ".csv"))))
			if err != nil {
				transfer.Error = fmt.Sprintf("error creating oracle ctl file :: %v", err)
				transferMap.CancelAndSetStatus(transfer.Id, transfer, StatusError)
				errorLog.Println(transfer.Error)
				return
			}
			defer oracleCtlFile.Close()

			controlFileBuilder := strings.Builder{}

			controlFileBuilder.WriteString(`LOAD DATA CHARACTERSET 'AL32UTF8' infile '`)
			controlFileBuilder.WriteString(filepath.Join(transfer.FinalCsvDir,
				fmt.Sprintf("%v.csv", strings.TrimSuffix(filepath.Base(finalCsvInfo.FilePath), ".csv"))))
			controlFileBuilder.WriteString(`' append into table `)
			if transfer.TargetSchema != "" {
				controlFileBuilder.WriteString(transfer.TargetSchema)
				controlFileBuilder.WriteString(".")
			}

			escapedTable := escapeIfNeeded(table, system)

			controlFileBuilder.WriteString(escapedTable)
			controlFileBuilder.WriteString(
				` fields csv with embedded terminated by ',' optionally enclosed by '"' (`)

			firstCol := true

			for i, column := range columnInfos {

				if !firstCol {
					controlFileBuilder.WriteString(" ,")
				}

				controlFileBuilder.WriteString(column.Name)

				switch column.PipeType {
				case "date":
					controlFileBuilder.WriteString(" date 'YYYY-MM-DD'")
				case "datetime":
					controlFileBuilder.WriteString(" timestamp 'YYYY-MM-DD HH24:MI:SS.FF'")
				case "datetimetz":
					controlFileBuilder.WriteString(
						" timestamp with time zone 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM'")
				default:
					maxLen, err := maxColumnByteLength(finalCsvInfo.FilePath, transfer.Null, i)
					if err != nil {
						transfer.Error = fmt.Sprintf("error getting max column length :: %v", err)
						transferMap.CancelAndSetStatus(transfer.Id, transfer, StatusError)
						errorLog.Println(transfer.Error)
						return
					}
					controlFileBuilder.WriteString(" char(")
					controlFileBuilder.WriteString(fmt.Sprint(maxLen))
					controlFileBuilder.WriteString(") PRESERVE BLANKS")
				}

				controlFileBuilder.WriteString(" nullif ")
				controlFileBuilder.WriteString(column.Name)
				controlFileBuilder.WriteString("='")
				controlFileBuilder.WriteString(transfer.Null)
				controlFileBuilder.WriteString("'")

				firstCol = false
			}

			controlFileBuilder.WriteString(")")

			// write frontmatter to oracle file
			_, err = oracleCtlFile.WriteString(controlFileBuilder.String())
			if err != nil {
				transfer.Error = fmt.Sprintf("error writing to oracle ctl file :: %v", err)
				transferMap.CancelAndSetStatus(transfer.Id, transfer, StatusError)
				errorLog.Println(transfer.Error)
				return
			}

			err = oracleCtlFile.Close()
			if err != nil {
				transfer.Error = fmt.Sprintf("error closing oracle ctl file :: %v", err)
				transferMap.CancelAndSetStatus(transfer.Id, transfer, StatusError)
				errorLog.Println(transfer.Error)
				return
			}

			finalCsvChannelOut <- finalCsvInfo
		}

		infoLog.Printf("transfer %v finished creating sqllder ctl files", transfer.Id)
	}()

	return finalCsvChannelOut
}

func (system Oracle) insertFinalCsvsOverride(transfer Transfer) (overridden bool, err error) {
	return false, nil
}

func (system Oracle) getFinalCsvFormatters() map[string]func(string) (string, error) {
	return map[string]func(string) (string, error){
		"nvarchar": func(v string) (string, error) {
			return v, nil
		},
		"varchar": func(v string) (string, error) {
			return v, nil
		},
		"ntext": func(v string) (string, error) {
			return v, nil
		},
		"text": func(v string) (string, error) {
			return v, nil
		},
		"int64": func(v string) (string, error) {
			return v, nil
		},
		"int32": func(v string) (string, error) {
			return v, nil
		},
		"int16": func(v string) (string, error) {
			return v, nil
		},
		"float64": func(v string) (string, error) {
			return v, nil
		},
		"float32": func(v string) (string, error) {
			return v, nil
		},
		"decimal": func(v string) (string, error) {
			return v, nil
		},
		"money": func(v string) (string, error) {
			return v, nil
		},
		"datetime": func(v string) (string, error) {
			valTime, err := time.Parse(time.RFC3339Nano, v)
			if err != nil {
				return "", fmt.Errorf("error writing datetime value to oracle csv :: %v", err)
			}
			return valTime.Format("2006-01-02 15:04:05.999999"), nil
		},
		"datetimetz": func(v string) (string, error) {
			valTime, err := time.Parse(time.RFC3339Nano, v)
			if err != nil {
				return "", fmt.Errorf("error writing datetime value to oracle csv :: %v", err)
			}
			return valTime.UTC().Format("2006-01-02 15:04:05.999999 -07:00"), nil
		},
		"date": func(v string) (string, error) {
			valTime, err := time.Parse(time.RFC3339Nano, v)
			if err != nil {
				return "", fmt.Errorf("error writing date value to oracle csv :: %v", err)
			}
			return valTime.Format("2006-01-02"), nil
		},
		"time": func(v string) (string, error) {
			valTime, err := time.Parse(time.RFC3339Nano, v)
			if err != nil {
				return "", fmt.Errorf("error writing time value to oracle csv :: %v", err)
			}

			return valTime.Format("15:04:05.999999"), nil
		},
		"varbinary": func(v string) (string, error) {
			return v, nil
		},
		"blob": func(v string) (string, error) {
			return v, nil
		},
		"uuid": func(v string) (string, error) {
			return strings.Replace(v, "-", "", -1), nil
		},
		"bool": func(v string) (string, error) {
			if v == "1" {
				return "1", nil
			}
			return "0", nil
		},
		"json": func(v string) (string, error) {
			return v, nil
		},
		"xml": func(v string) (string, error) {
			return v, nil
		},
		"varbit": func(v string) (string, error) {
			return v, nil
		},
	}
}

// func (system Oracle) runReplicationInsertCmd(
// 	finalCsvLocation string,
// 	replication Replication,
// 	table Table,
// ) (
// 	err error,
// ) {
// 	fileNum, err := getFileNum(finalCsvLocation)
// 	if err != nil {
// 		return fmt.Errorf("error getting file number :: %v", err)
// 	}

// 	ctlFileName := filepath.Join(replication.FinalCsvDir, fmt.Sprintf("%v.ctl", fileNum))
// 	logFileName := filepath.Join(replication.FinalCsvDir, fmt.Sprintf("%v.log", fileNum))
// 	badFileName := filepath.Join(replication.FinalCsvDir, fmt.Sprintf("%v.bad", fileNum))
// 	discardFileName := filepath.Join(replication.FinalCsvDir, fmt.Sprintf("%v.discard", fileNum))

// 	if !replication.KeepFiles {
// 		defer os.Remove(logFileName)
// 		defer os.Remove(badFileName)
// 		defer os.Remove(discardFileName)
// 		defer os.Remove(ctlFileName)
// 	}

// 	cmd := exec.Command(
// 		"sqlldr",
// 		fmt.Sprintf("%s/%s@%s:%d/%s", replication.TargetUsername, replication.TargetPassword,
// 			replication.TargetHostnamename, replication.TargetPort, replication.TargetDatabase),
// 		fmt.Sprintf("control=%s", ctlFileName),
// 		fmt.Sprintf("LOG=%s", logFileName),
// 		fmt.Sprintf("BAD=%s", badFileName),
// 		fmt.Sprintf("DISCARD=%s", discardFileName),
// 	)

// 	result, err := cmd.CombinedOutput()
// 	if err != nil {
// 		return fmt.Errorf(
// 			"failed to upload csv to oracle :: stderr %v :: stdout %s", err, string(result))
// 	}

// 	return nil
// }

func (system Oracle) runInsertCmd(
	finalCsvInfo FinalCsvInfo,
	transfer Transfer,
	schema, table string,
) (
	err error,
) {

	ctlFileName := filepath.Join(transfer.FinalCsvDir, fmt.Sprintf("%v.ctl", strings.TrimSuffix(filepath.Base(finalCsvInfo.FilePath), ".csv")))
	logFileName := filepath.Join(transfer.FinalCsvDir, fmt.Sprintf("%v.log", strings.TrimSuffix(filepath.Base(finalCsvInfo.FilePath), ".csv")))
	badFileName := filepath.Join(transfer.FinalCsvDir, fmt.Sprintf("%v.bad", strings.TrimSuffix(filepath.Base(finalCsvInfo.FilePath), ".csv")))
	discardFileName := filepath.Join(transfer.FinalCsvDir, fmt.Sprintf("%v.discard", strings.TrimSuffix(filepath.Base(finalCsvInfo.FilePath), ".csv")))

	if !transfer.KeepFiles {
		defer os.Remove(logFileName)
		defer os.Remove(badFileName)
		defer os.Remove(discardFileName)
		defer os.Remove(ctlFileName)
	}

	cmd := exec.CommandContext(
		transfer.Context,
		"sqlldr",
		fmt.Sprintf("%s/%s@%s:%d/%s", transfer.TargetConnectionInfo.Username, transfer.TargetConnectionInfo.Password,
			transfer.TargetConnectionInfo.Hostname, transfer.TargetConnectionInfo.Port, transfer.TargetConnectionInfo.Database),
		fmt.Sprintf("control=%s", ctlFileName),
		fmt.Sprintf("LOG=%s", logFileName),
		fmt.Sprintf("BAD=%s", badFileName),
		fmt.Sprintf("DISCARD=%s", discardFileName),
	)

	result, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf(
			"failed to upload csv to oracle :: stderr %v :: stdout %s", err, string(result))
	}

	return nil
}

// func (system Oracle) removeTrackingSchema() (err error) {
// 	infoLog.Printf("removing tracking schema from %v", system.Name)

// 	query := `
// 		DECLARE
// 			l_sid       NUMBER;
// 			l_serial#   NUMBER;
// 			l_user_exists NUMBER;
// 		BEGIN

// 			-- Delete triggers with the prefix "reflector_tracking" (regardless of the schema)
// 			FOR t IN (SELECT owner, trigger_name
// 					FROM all_triggers
// 					WHERE lower(trigger_name) LIKE 'reflector_tracking%') LOOP
// 				EXECUTE IMMEDIATE 'DROP TRIGGER ' || t.owner || '.' || t.trigger_name;
// 			END LOOP;

// 			-- Check if the 'reflector_tracking' schema exists
// 			SELECT COUNT(*)
// 			INTO l_user_exists
// 			FROM all_users
// 			WHERE username = 'REFLECTOR_TRACKING';

// 			IF l_user_exists > 0 THEN

// 				-- Find sessions that are locking objects in the 'reflector_tracking' schema
// 				FOR s IN (SELECT DISTINCT s.sid, s.serial#
// 						FROM v$locked_object l
// 						JOIN all_objects o ON l.object_id = o.object_id
// 						JOIN v$session s ON l.session_id = s.sid
// 						WHERE o.owner = 'REFLECTOR_TRACKING') LOOP

// 					l_sid := s.sid;
// 					l_serial# := s.serial#;

// 					-- Kill the session
// 					EXECUTE IMMEDIATE 'ALTER SYSTEM KILL SESSION ''' || l_sid || ',' || l_serial# || ''' IMMEDIATE';
// 				END LOOP;

// 				-- Drop the 'reflector_tracking' schema along with all its associated objects
// 				EXECUTE IMMEDIATE 'DROP USER reflector_tracking CASCADE';

// 			END IF; -- End of check for schema existence

// 			FOR rec IN (SELECT sequence_owner, sequence_name FROM all_sequences
// 				WHERE sequence_name LIKE 'REFLECTOR_TRACKING%')
// 			LOOP
// 			EXECUTE IMMEDIATE 'DROP SEQUENCE ' || rec.sequence_owner || '.' || rec.sequence_name;
// 			END LOOP;

// 		END; -- End of anonymous block`

// 	err = system.exec(query)
// 	if err != nil {
// 		return fmt.Errorf("error dropping tracking schema :: %v", err)
// 	}

// 	infoLog.Printf("finished removing tracking schema from %v", system.Name)

// 	return nil
// }

// func (system Oracle) getTablesBySizeQuery() string {
// 	return `
// 		SELECT
// 			OWNER,
// 			TABLE_NAME
// 		FROM
// 			DBA_TABLES
// 		WHERE
// 			owner not in ('SYS', 'MDSYS', 'XDB', 'CTXSYS', 'SYSTEM', 'GSMADMIN_INTERNAL', 'OJVMSYS', 'ORDSYS', 'ORDDATA', 'LBACSYS', 'DBSNMP', 'WMSYS', 'DVSYS', 'APPQOSSYS', 'AUDSYS', 'DBSFWUSER', 'OLAPSYS', 'OUTLN', 'REFLECTOR_TRACKING')
// 		ORDER BY
// 			OWNER, TABLE_NAME`
// }

// func (system Oracle) createTrackingSchema(trackingSchema TrackingSchema) (tables []Table, err error) {
// 	return createTrackingSchemaCommon(system, trackingSchema)
// }

// func (system Oracle) getTablesBySizeDesc(target System, retentionInterval int) (tables []Table, err error) {
// 	return getTablesBySizeDescCommon(system, target, retentionInterval)
// }

// func (system Oracle) getPkQuery(table Table) (query string) {
// 	query = fmt.Sprintf(`
// 		SELECT
// 			col.column_name
// 		FROM
// 			all_constraints cons
// 		INNER JOIN
// 			all_cons_columns col ON cons.owner = col.owner
// 			AND cons.constraint_name = col.constraint_name
// 		WHERE
// 			cons.constraint_type = 'P'
// 			AND cons.owner = '%v'
// 			AND cons.table_name = '%v'
// 		ORDER BY
// 			col.position`,
// 		table.UnescapedSourceSchema,
// 		table.UnescapedName,
// 	)

// 	return query
// }

// func (system Oracle) getTablePrimaryKeys(table Table) (primaryKeys []string, err error) {
// 	return getTablePrimaryKeysCommon(table, system)
// }

// func (system Oracle) getNextCatchupVersion(table Table) (nextCatchupVersion int64, err error) {
// 	return getNextCatchupVersionCommon(table, system)
// }

// func (system Oracle) createReplicationPipeFile(table Table, replicationCycle, version int64, replication Replication) (pipeFilePath string, columnInfos []ColumnInfo, err error) {
// 	return createReplicationPipeFileCommon(table, replicationCycle, version, replication, system)
// }

func (system Oracle) schemaRequired() bool {
	return true
}

// func (system Oracle) schemaRequiredAsTarget() bool {
// 	return false
// }

func (system Oracle) escape(objectName string) (escaped string) {
	return fmt.Sprintf(`"%v"`, objectName)
}

func (system Oracle) isReservedKeyword(objectName string) bool {
	return false
}

// func (system Oracle) escapeIfNeeded(objectName string) (escaped string) {
// 	return escapeIfNeededCommon(objectName, system)
// }

// func (system Oracle) needsEscaping(objectName string) (needsEscaping bool) {
// 	return needsEscapingCommon(objectName, system)
// }

// func (system Oracle) cleanupTrackingTable(table Table) (err error) {
// 	err = system.exec(fmt.Sprintf("delete from reflector_tracking.%v where reflector_delete_after < SYSTIMESTAMP", table.EscapedSourceSchemaUnderscoreTable))
// 	if err != nil {
// 		return fmt.Errorf("error deleting rows from tracking table :: %v", err)
// 	}

// 	return nil
// }

// func (system Oracle) deleteReplicationRowsAndCreateFinalCsv(pipeFilePath string, replication Replication, columnInfos []ColumnInfo, table Table) (finalCsvPath string, err error) {
// 	return deleteReplicationRowsAndCreateFinalCsvCommon(pipeFilePath, replication, columnInfos, table, system)
// }

// func (system Oracle) checkSource() (err error) {
// 	return
// }

func (system Oracle) createSchemaIfNotExistsOverride(schema string) (overridden bool, err error) {
	var count int

	// Check if the user/schema already exists
	err = system.queryRow(fmt.Sprintf("SELECT COUNT(1) FROM dba_users WHERE username = UPPER('%v')", schema)).Scan(&count)
	if err != nil {
		return true, fmt.Errorf("error checking if user exists :: %v", err)
	}

	if count == 0 {
		randomChars, err := RandomPrintableAsciiCharacters(20)
		if err != nil {
			return true, fmt.Errorf("error generating random password :: %v", err)
		}

		// Create the user/schema
		err = system.exec(fmt.Sprintf(`CREATE USER %s identified by "%v"`, schema, randomChars))
		if err != nil {
			return true, err
		}
		infoLog.Printf("Created user %s in oracle with password %s", schema, randomChars)
	}

	return true, nil
}

// func (system Oracle) createTrackingTable(table Table) (err error) {

// 	query := fmt.Sprintf(`
// 		begin

// 		EXECUTE IMMEDIATE 'ALTER USER REFLECTOR_TRACKING QUOTA UNLIMITED ON %v';

// 		EXECUTE IMMEDIATE 'ALTER USER REFLECTOR_TRACKING DEFAULT TABLESPACE %v';

// 		EXECUTE IMMEDIATE 'create table reflector_tracking.%v TABLESPACE %v as SELECT %v from %v where 1=0';

// 		EXECUTE IMMEDIATE 'GRANT INSERT, SELECT ON reflector_tracking.%v TO %v';

// 		EXECUTE IMMEDIATE 'alter table reflector_tracking.%v add reflector_event_type char(1)';

// 		EXECUTE IMMEDIATE 'ALTER TABLE reflector_tracking.%v ADD reflector_delete_after TIMESTAMP DEFAULT (SYSTIMESTAMP + INTERVAL ''%v'' SECOND)';

// 		EXECUTE IMMEDIATE 'alter table reflector_tracking.%v add reflector_tracking_id NUMBER(19) unique';

// 		EXECUTE IMMEDIATE 'create sequence %v.%v start with 1 increment by 1 minvalue 1 maxvalue 9223372036854775807 cache 100 nocycle';

// 		EXECUTE IMMEDIATE 'GRANT SELECT ON %v.%v TO reflector_tracking';

// 		EXECUTE IMMEDIATE 'GRANT SELECT ON %v.%v TO %v';

// 		end;`,
// 		table.EscapedTablespace,
// 		table.EscapedTablespace,
// 		table.EscapedSourceSchemaUnderscoreTable,
// 		table.EscapedTablespace,
// 		strings.Join(table.EscapedColumnNames, ", "),
// 		table.EscapedSourceSchemaPeriodTable,
// 		table.EscapedSourceSchemaUnderscoreTable,
// 		table.EscapedSourceSchema,
// 		table.EscapedSourceSchemaUnderscoreTable,
// 		table.EscapedSourceSchemaUnderscoreTable,
// 		table.RetentionInterval,
// 		table.EscapedSourceSchemaUnderscoreTable,
// 		table.EscapedSourceSchema,
// 		table.EscapedTrackingSequence,
// 		table.EscapedSourceSchema,
// 		table.EscapedTrackingSequence,
// 		table.EscapedSourceSchema,
// 		table.EscapedTrackingSequence,
// 		table.EscapedSourceSchema,
// 	)

// 	err = system.exec(query)
// 	if err != nil {
// 		return fmt.Errorf("error creating tracking table :: %v", err)
// 	}

// 	return
// }

// func (system Oracle) createTrackingTriggers(table Table) (err error) {
// 	var query string

// 	if len(table.UnescapedPrimaryKeys) > 0 {
// 		query = fmt.Sprintf(`
// 			create trigger reflector_tracking.%v after insert on %v

// 				for each row declare current_reflector_tracking_id number;

// 				begin

// 					select %v.%v.nextval into current_reflector_tracking_id from dual;

// 					insert into reflector_tracking.%v (%v, reflector_event_type, reflector_tracking_id) values (
// 		`,
// 			table.EscapedInsertTrigger, table.EscapedSourceSchemaPeriodTable, table.EscapedSourceSchema, table.EscapedTrackingSequence, table.EscapedSourceSchemaUnderscoreTable,
// 			strings.Join(table.EscapedColumnNames, ", "),
// 		)

// 		for _, columnName := range table.EscapedColumnNames {
// 			query += fmt.Sprintf(":NEW.%v, ", columnName)
// 		}
// 		query += `'I', current_reflector_tracking_id); end;`

// 		err = system.exec(query)
// 		if err != nil {
// 			return fmt.Errorf("error creating insert trigger :: %v", err)
// 		}
// 	}

// 	if len(table.UnescapedPrimaryKeys) > 0 {
// 		query = fmt.Sprintf(`
// 			create trigger reflector_tracking.%v after update on %v

// 				for each row declare current_reflector_tracking_id number;

// 				begin

// 					select %v.%v.nextval into current_reflector_tracking_id from dual;

// 					insert into reflector_tracking.%v (%v, reflector_event_type, reflector_tracking_id)

// 					values (`,
// 			table.EscapedUpdateTrigger, table.EscapedSourceSchemaPeriodTable, table.EscapedSourceSchema, table.EscapedTrackingSequence, table.EscapedSourceSchemaUnderscoreTable,
// 			strings.Join(table.EscapedColumnNames, ", "),
// 		)
// 		for _, columnName := range table.EscapedColumnNames {
// 			query += fmt.Sprintf(":NEW.%v, ", columnName)
// 		}
// 		query += `'U', current_reflector_tracking_id); end;`

// 		err = system.exec(query)
// 		if err != nil {
// 			return fmt.Errorf("error creating update trigger :: %v", err)
// 		}
// 	}

// 	if len(table.UnescapedPrimaryKeys) > 0 {
// 		query = fmt.Sprintf(`
// 			create trigger reflector_tracking.%v after delete on %v

// 				for each row declare current_reflector_tracking_id number;

// 				begin

// 					select %v.%v.nextval into current_reflector_tracking_id from dual;

// 					insert into reflector_tracking.%v (%v, reflector_event_type, reflector_tracking_id)

// 					values (`,
// 			table.EscapedDeleteTrigger, table.EscapedSourceSchemaPeriodTable, table.EscapedSourceSchema, table.EscapedTrackingSequence, table.EscapedSourceSchemaUnderscoreTable,
// 			strings.Join(table.EscapedColumnNames, ", "),
// 		)
// 		for _, columnName := range table.EscapedColumnNames {
// 			query += fmt.Sprintf(":OLD.%v, ", columnName)
// 		}

// 		query += `'D', current_reflector_tracking_id); end;`

// 		err = system.exec(query)
// 		if err != nil {
// 			return fmt.Errorf("error creating delete trigger :: %v", err)
// 		}
// 	}

// 	infoLog.Printf("finished creating tracking schema in %v", system.Name)

// 	return nil
// }

// func (system Oracle) getColumnNamesQuery(table Table) string {
// 	query := fmt.Sprintf(`
// 		SELECT COLUMN_NAME
// 		FROM ALL_TAB_COLUMNS
// 		WHERE OWNER = upper('%v')
// 		AND TABLE_NAME = ('%v')
// 		ORDER BY COLUMN_ID
// 	`,
// 		table.UnescapedSourceSchema,
// 		table.UnescapedName,
// 	)

// 	return query
// }

// func (system Oracle) getMaxTrackingVersion(table Table) (maxTrackingVersion int64, err error) {
// 	query := fmt.Sprintf("select max(reflector_tracking_id) from reflector_tracking.%v", escapedSchemaUnderscoreTable)
// 	err = system.queryRow(query).Scan(&maxTrackingVersion)
// 	if err != nil {
// 		if strings.Contains(err.Error(), "converting NULL to int64 is unsupported") {
// 			maxTrackingVersion = 0
// 		} else {
// 			return 0, fmt.Errorf("error running query to get max version <- %v", err)
// 		}
// 	}

// 	return maxTrackingVersion, nil
// }

// func (system Oracle) getSchemaPeriodTable(schema, table string, escapedIfNeeded bool) (schemaPeriodTable string) {
// 	return getSchemaPeriodTableCommon(schema, table, system, escapedIfNeeded)
// }

// func (system Oracle) getSchemaUnderscoreTable(schema, table string, escapeIfNeeded bool) string {
// 	return getSchemaUnderscoreTableCommon(schema, table, system, escapeIfNeeded)
// }

// func (system Oracle) getTablespaceName(table, schema string) (tablespaceName string, err error) {
// 	query := fmt.Sprintf(`
// 		SELECT TABLESPACE_NAME
// 		FROM DBA_TABLES
// 		WHERE OWNER = upper('%v')
// 		AND TABLE_NAME = upper('%v')
// 		order by owner`,
// 		schema,
// 		table,
// 	)

// 	err = system.queryRow(query).Scan(&tablespaceName)
// 	if err != nil {
// 		return tablespaceName, fmt.Errorf("error querying oracle for tablespace name :: %v", err)
// 	}

// 	return tablespaceName, nil
// }

// func (system Oracle) isDefaultSchema(schema string) bool {
// 	return true
// }

// func (system Oracle) getDefaultSchema() string {
// 	return ""
// }

func (system Oracle) IsTableNotFoundError(err error) bool {
	return strings.Contains(err.Error(), "does not exist")
}

func (system Oracle) closeConnectionPool(printError bool) (err error) {
	err = system.Connection.Close()
	if err != nil && printError {
		errorLog.Printf("error closing %v connection pool :: %v", system.Name, err)
	}
	return err
}

func (system Oracle) dbTypeToPipeType(
	databaseTypeName string,
) (
	pipeType string,
	err error,
) {
	switch databaseTypeName {
	case "NUMBER":
		return "decimal", nil
	case "NCHAR":
		return "nvarchar", nil
	case "FLOAT":
		return "float64", nil
	case "VARCHAR2":
		return "nvarchar", nil
	case "DATE":
		return "date", nil
	case "BINARY_FLOAT":
		return "float32", nil
	case "BINARY_DOUBLE":
		return "float64", nil
	case "RAW":
		return "varbinary", nil
	case "CHAR":
		return "nvarchar", nil
	case "TIMESTAMP":
		return "datetime", nil
	case "TIMESTAMP WITH TIME ZONE":
		return "datetimetz", nil
	case "INTERVAL":
		return "nvarchar", nil
	case "UROWID":
		return "nvarchar", nil
	case "TIMESTAMP WITH LOCAL TIME ZONE":
		return "datetimetz", nil
	case "CLOB":
		return "ntext", nil
	case "BLOB":
		return "blob", nil
	case "NCLOB":
		return "ntext", nil
	default:
		return "", fmt.Errorf("unsupported database type for oracle: %v", databaseTypeName)
	}
}

func (system Oracle) getTableColumnInfosRows(schema, table string) (rows *sql.Rows, err error) {
	query := fmt.Sprintf(`
		WITH PrimaryKeys AS (
			SELECT
				cols.column_name
			FROM
				all_constraints cons
			JOIN all_cons_columns cols 
				ON cons.constraint_name = cols.constraint_name
				AND cons.owner = cols.owner
			WHERE
				cons.constraint_type = 'P'
				AND cons.owner = upper('%v')
				AND cons.table_name = upper('%v')
		)
		
		SELECT
			col.column_name AS col_name,
			CASE
				WHEN col.data_type LIKE 'TIMESTAMP(%%) WITH%%' THEN
					REPLACE(col.data_type, SUBSTR(col.data_type, INSTR(col.data_type, '('), INSTR(col.data_type, ')') - INSTR(col.data_type, '(') + 1), '')
				WHEN col.data_type LIKE 'TIMESTAMP(%%)' THEN
					REPLACE(col.data_type, SUBSTR(col.data_type, INSTR(col.data_type, '('), INSTR(col.data_type, ')') - INSTR(col.data_type, '(') + 1), '')
				WHEN col.data_type LIKE 'INTERVAL%%' THEN
					'INTERVAL'
				ELSE
					col.data_type
			END AS col_type,
			COALESCE(col.data_precision, -1) AS col_precision,
			COALESCE(col.data_scale, -1) AS col_scale,
			COALESCE(col.data_length, -1) AS col_length,
			CASE WHEN pk.column_name IS NOT NULL THEN 1 ELSE 0 END AS col_is_primary
		FROM
			all_tab_columns col
		LEFT JOIN PrimaryKeys pk ON col.column_name = pk.column_name
		WHERE
			col.owner = upper('%v')
			AND col.table_name = upper('%v')
		ORDER BY
			col.column_id`,
		schema, table, schema, table,
	)

	rows, err = system.query(query)
	if err != nil {
		return nil, fmt.Errorf("error getting table column infos rows :: %v", err)
	}

	return rows, nil
}

func (system Oracle) getIncrementalTimeOverride(schema, table, incrementalColumn string, initialLoad bool) (time.Time, bool, bool, error) {
	return time.Time{}, false, initialLoad, nil
}

func (system Oracle) getPrimaryKeysRows(schema, table string) (rows *sql.Rows, err error) {

	query := fmt.Sprintf(`
		SELECT 
			acc.column_name
		FROM 
			all_constraints ac
		JOIN 
			all_cons_columns acc ON ac.constraint_name = acc.constraint_name AND ac.owner = acc.owner
		WHERE 
			ac.constraint_type = 'P'
			AND ac.owner = upper('%v')
			AND ac.table_name = upper('%v')
		ORDER BY 
			acc.position;`,
		schema, table)

	rows, err = system.query(query)
	if err != nil {
		return nil, fmt.Errorf("error getting primary keys rows :: %v", err)
	}

	return rows, nil
}

var oracleDatetimeFormat = "2006-01-02 15:04:05.999999"
var oracleDateFormat = "2006-01-02"
var oracleTimeFormat = "15:04:05.999999"

func (system Oracle) getSqlFormatters() (
	pipeFileFormatters map[string]func(string) (pipeFileValue string, err error),
) {
	return map[string]func(string) (pipeFileValue string, err error){
		"nvarchar": func(v string) (pipeFileValue string, err error) {
			return fmt.Sprintf("'%v'", singleQuoteReplacer.Replace(v)), nil
		},
		"varchar": func(v string) (pipeFileValue string, err error) {
			return fmt.Sprintf("'%v'", singleQuoteReplacer.Replace(v)), nil
		},
		"ntext": func(v string) (pipeFileValue string, err error) {
			return fmt.Sprintf("'%v'", singleQuoteReplacer.Replace(v)), nil
		},
		"text": func(v string) (pipeFileValue string, err error) {
			return fmt.Sprintf("'%v'", singleQuoteReplacer.Replace(v)), nil
		},
		"int64": func(v string) (pipeFileValue string, err error) {
			return v, nil
		},
		"int32": func(v string) (pipeFileValue string, err error) {
			return v, nil
		},
		"int16": func(v string) (pipeFileValue string, err error) {
			return v, nil
		},
		"float64": func(v string) (pipeFileValue string, err error) {
			return v, nil
		},
		"float32": func(v string) (pipeFileValue string, err error) {
			return v, nil
		},
		"decimal": func(v string) (pipeFileValue string, err error) {
			return v, nil
		},
		"money": func(v string) (pipeFileValue string, err error) {
			return v, nil
		},
		"datetime": func(v string) (pipeFileValue string, err error) {
			valTime, err := time.Parse(time.RFC3339Nano, v)
			if err != nil {
				return "", fmt.Errorf("error writing date value to oracle csv :: %v", err)
			}
			return fmt.Sprintf("TO_TIMESTAMP('%v', 'YYYY-MM-DD HH24:MI:SS.FF6')", valTime.Format(oracleDatetimeFormat)), nil
		},
		"datetimetz": func(v string) (pipeFileValue string, err error) {
			valTime, err := time.Parse(time.RFC3339Nano, v)
			if err != nil {
				return "", fmt.Errorf("error writing date value to oracle csv :: %v", err)
			}
			return fmt.Sprintf("TO_TIMESTAMP('%v', 'YYYY-MM-DD HH24:MI:SS.FF6')", valTime.Format(oracleDatetimeFormat)), nil
		},
		"date": func(v string) (pipeFileValue string, err error) {
			valTime, err := time.Parse(time.RFC3339Nano, v)
			if err != nil {
				return "", fmt.Errorf("error writing date value to oracle csv :: %v", err)
			}
			return fmt.Sprintf("TO_DATE('%v', 'YYYY-MM-DD')", valTime.Format(oracleDateFormat)), nil
		},
		"time": func(v string) (pipeFileValue string, err error) {
			valTime, err := time.Parse(time.RFC3339Nano, v)
			if err != nil {
				return "", fmt.Errorf("error writing date value to oracle csv :: %v", err)
			}

			return fmt.Sprintf("'%v'", valTime.Format(oracleTimeFormat)), nil
		},
		"varbinary": func(v string) (pipeFileValue string, err error) {
			return fmt.Sprintf(`HEXTORAW('%s')`, v), nil
		},
		"blob": func(v string) (pipeFileValue string, err error) {
			return fmt.Sprintf(`HEXTORAW('%s')`, v), nil
		},
		"uuid": func(v string) (pipeFileValue string, err error) {
			return v, nil
		},
		"bool": func(v string) (pipeFileValue string, err error) {
			if v == "true" {
				return "1", nil
			}
			return "0", nil
		},
		"json": func(v string) (pipeFileValue string, err error) {
			return fmt.Sprintf("'%v'", singleQuoteReplacer.Replace(v)), nil
		},
		"xml": func(v string) (pipeFileValue string, err error) {
			return fmt.Sprintf("'%v'", singleQuoteReplacer.Replace(v)), nil
		},
		"varbit": func(v string) (pipeFileValue string, err error) {
			return v, nil
		},
	}
}
