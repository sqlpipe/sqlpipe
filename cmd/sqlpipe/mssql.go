package main

import (
	"fmt"
)

func (system *System) mssqlDropTable(schema, table string) error {
	query := fmt.Sprintf("drop table if exists %v.%v", schema, table)
	_, err := system.connection.Exec(query)
	if err != nil {
		err = fmt.Errorf("error dropping %v.%v in mssql -> %v", schema, table, err)
	}
	return err
}

// func (system *System) mssqlCreateTable(schema, table string, columnInfo ColumnInfo) error {

// }
