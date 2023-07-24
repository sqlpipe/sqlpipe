package main

import (
	"fmt"
)

func (system *System) mysqlDropTable(table string) error {
	query := fmt.Sprintf("drop table if exists %v", table)
	_, err := system.connection.Exec(query)
	if err != nil {
		err = fmt.Errorf("error dropping %v in mysql -> %v", table, err)
	}
	return err
}

func (system *System) mysqlCreateTable(table string, columnInfo []ColumnInfo) error {
	for col := range columnInfo {
		fmt.Printf("%+v\n", columnInfo[col])
	}
	return nil
}
