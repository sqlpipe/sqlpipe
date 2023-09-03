package main

import (
	"os/exec"
)

func checkDeps() {
	checkPsql()
	checkBcp()
	checkSqlLdr()
}

func checkPsql() {
	output, err := exec.Command("psql", "--version").CombinedOutput()
	if err != nil {
		warningLog.Printf("psql not found. please install psql to transfer data to postgresql :: %v :: %v\n", err, string(output))
		return
	}

	psqlAvailable = true
}

func checkBcp() {
	output, err := exec.Command("bcp", "-v").CombinedOutput()
	if err != nil {
		warningLog.Printf("bcp not found. please install bcp to transfer data to mssql :: %v :: %v\n", err, string(output))
		return
	}

	bcpAvailable = true
}

func checkSqlLdr() {
	output, err := exec.Command("sqlldr", "-help").CombinedOutput()
	if err != nil {
		warningLog.Printf("sqlldr not found. please install sqllder to transfer data to oracle :: %v :: %v\n", err, string(output))
		return
	}

	sqlldrAvailable = true
}
