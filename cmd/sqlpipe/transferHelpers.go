package main

import (
	"database/sql"
)

func getScanType(columnType *sql.ColumnType) (scanType string) {
	defer func() {
		if r := recover(); r != nil {
			infoLog.Printf("panic in getScanType :: %v", r)
			scanType = ""
		}
	}()

	scanType = columnType.ScanType().String()
	return
}
