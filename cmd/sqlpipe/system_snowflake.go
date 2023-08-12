package main

// func (system *System) snowflakeDropTable(schema, table string) error {
// 	query := fmt.Sprintf("drop table if exists %v.%v", schema, table)
// 	_, err := system.connection.Exec(query)
// 	if err != nil {
// 		err = fmt.Errorf("error dropping %v.%v in snowflake :: %v", schema, table, err)
// 	}
// 	return err
// }

// func (system *System) snowflakeCreateTable(schema, table string, columnInfo []ColumnInfo) error {
// 	for col := range columnInfo {
// 		fmt.Printf("%+v\n", columnInfo[col])
// 	}
// 	return nil
// }
