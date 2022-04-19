# xsql [![Build Status](https://travis-ci.com/shomali11/xsql.svg?branch=master)](https://travis-ci.com/shomali11/xsql) [![Go Report Card](https://goreportcard.com/badge/github.com/shomali11/xsql)](https://goreportcard.com/report/github.com/shomali11/xsql) [![GoDoc](https://godoc.org/github.com/shomali11/xsql?status.svg)](https://godoc.org/github.com/shomali11/xsql) [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

SQL Query Results Pretty Printing

## Dependencies

* `util` [github.com/shomali11/util](https://github.com/shomali11/util)

# Examples

## Example 1

```go
package main

import (
	"database/sql"
	"fmt"
	"github.com/shomali11/xsql"
	"log"
)

const (
	dataSourceFormat = "user=%s password=%s dbname=%s sslmode=disable"
)

func main() {
	dataSource := fmt.Sprintf(dataSourceFormat, "<USERNAME>", "<PASSWORD>", "<DATABASE_NAME>")
	db, err := sql.Open("<DRIVER>", dataSource)
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	rows, err := db.Query("SELECT * FROM test")
	if err != nil {
		log.Fatal(err)
	}

	results, err := xsql.Pretty(rows)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(results)
}
```

Output:

```
 id  |      name      |         title         |         created_at          | number | decimal | active
-----+----------------+-----------------------+-----------------------------+--------+---------+--------
   1 | Raed Shomali   | Sr. Software Engineer | 2017-10-24T20:59:43.37154Z  |     11 | 789.123 | true
   2 | Dwayne Johnson | The Rock              | 2017-10-24T21:00:31.530534Z |   1000 |     3.7 | true
 300 | Steve Austin   | Stone Cold            | 2017-10-26T19:42:51.993465Z |  55000 |   55.55 | false
(3 rows)
```