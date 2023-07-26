package main

// func (system *System) oracleDropTable(schema, table string) error {
// 	query := fmt.Sprintf("drop table %v.%v", schema, table)
// 	_, err := system.connection.Exec(query)
// 	if err != nil {
// 		err = fmt.Errorf("error dropping %v.%v in oracle :: %v", schema, table, err)
// 	}
// 	return err
// }

// func (system *System) oracleCreateTable(schema, table string, columnInfo []ColumnInfo) error {
// 	for col := range columnInfo {
// 		fmt.Printf("%+v\n", columnInfo[col])
// 	}
// 	return nil
// }
