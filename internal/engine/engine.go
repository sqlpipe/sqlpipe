package engine

import (
	"database/sql"
	"fmt"

	_ "github.com/alexbrainman/odbc"
	"github.com/sqlpipe/sqlpipe/internal/data"
)

func RunQuery(query data.Query) {
	dsn := fmt.Sprintf(
		"DRIVER=%v;SERVER=%v;PORT=%v;DATABASE=%v;UID=%v;PWD=%v;TDS_Version=8.0;",
		query.Connection.DriverName,
		query.Connection.Hostname,
		query.Connection.Port,
		query.Connection.DbName,
		query.Connection.Username,
		query.Connection.Password,
	)

	mssql, err := sql.Open("odbc", dsn)

	if err != nil {
		fmt.Println("err here")
		fmt.Println(err)
	}

	rows, err := mssql.Query(query.Query)
	if err != nil {
		fmt.Println("couldn't run query")
	}

	fmt.Println("HERE!")

	defer rows.Close()

	if err != nil {
		fmt.Println(err)
	}
}
