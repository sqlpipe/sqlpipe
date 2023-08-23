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
		warningLog.Printf("unable to check psql version :: %v :: %v\n", err, string(output))
		return
	}

	psqlAvailable = true
}

func checkBcp() {
	output, err := exec.Command("bcp", "-v").CombinedOutput()
	if err != nil {
		warningLog.Printf("unable to check bcp version :: %v :: %v\n", err, string(output))
		return
	}

	bcpAvailable = true
}

func checkSqlLdr() {
	output, err := exec.Command("sqlldr", "-help").CombinedOutput()
	if err != nil {
		warningLog.Printf("unable to check sqlldr version :: %v :: %v\n", err, string(output))
		return
	}

	sqlLdrAvailable = true
}
