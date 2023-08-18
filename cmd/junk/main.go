package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/sijms/go-ora/v2"
)

func main() {

	oracleDsn := "oracle://mydb_admin:Mypass123@localhost:1521/mydb"

	db, err := sql.Open("oracle", oracleDsn)
	if err != nil {
		log.Fatal(err)
	}

	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}

	// query := "select * from mydb_admin.my_table"
	// query := "select count(*) from my_table"
	query := "select count(*) from mydb_admin.my_table"
	// query := `SELECT TABLE_NAME FROM USER_TABLES`

	rows, err := db.Query(query)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		log.Fatal(err)
	}

	vals := make([]interface{}, len(columns))
	valPtrs := make([]interface{}, len(columns))

	for i := range columns {
		valPtrs[i] = &vals[i]
	}
	for rows.Next() {
		err := rows.Scan(valPtrs...)
		if err != nil {
			log.Fatal(err)
		}

		for i, v := range vals {
			fmt.Printf("%s: %s\n", columns[i], v)
		}
	}
}
