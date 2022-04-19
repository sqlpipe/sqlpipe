package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/alexbrainman/odbc"
	"github.com/shomali11/xsql"
	"github.com/sqlpipe/sqlpipe/internal/data"
)

func RunQuery(
	query data.Query,
) (
	rows *sql.Rows,
	err error,
) {
	dsn := fmt.Sprintf(
		"DRIVER=%v;SERVER=%v;PORT=%v;DATABASE=%v;UID=%v;PWD=%v;TDS_Version=8.0;",
		query.Connection.DriverName,
		query.Connection.Hostname,
		query.Connection.Port,
		query.Connection.DbName,
		query.Connection.Username,
		query.Connection.Password,
	)

	db, err := sql.Open("odbc", dsn)
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	rows, err = db.Query(query.Query)
	if err != nil {
		log.Fatal(err)
	}

	switch query.ReturnFormat {
	case "csv":
	case "json":
	case "none":
	default:
		results, err := xsql.Pretty(rows)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(results)
	}

	return rows, err
}
