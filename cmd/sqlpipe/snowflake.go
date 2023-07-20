package main

import (
	"fmt"
)

func (system *System) snowflakeDropTable(schema, table string) error {
	query := fmt.Sprintf("drop table %v.%v", schema, table)
	_, err := system.connection.Exec(query)
	if err != nil {
		err = fmt.Errorf("error dropping %v.%v in snowflake -> %v", schema, table, err)
	}
	return err
}
