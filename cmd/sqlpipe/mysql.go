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
