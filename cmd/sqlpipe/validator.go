package main

import (
	"fmt"
	_ "time/tzdata"
)

type validator struct {
	errors map[string]string
}

func newValidator() validator {
	return validator{errors: make(map[string]string)}
}

func (v *validator) valid() bool {
	return len(v.errors) == 0
}

func (v *validator) addError(key, message string) {
	if _, exists := v.errors[key]; !exists {
		v.errors[key] = message
	}
}

func (v *validator) check(ok bool, key, message string) {
	if !ok {
		v.addError(key, message)
	}
}

func permittedValue[T comparable](value T, permittedValues ...T) bool {
	for i := range permittedValues {
		if value == permittedValues[i] {
			return true
		}
	}
	return false
}

var permittedSources = []string{"postgresql", "mysql", "mssql", "oracle", "snowflake"}
var permittedTargets = []string{"postgresql", "mysql", "mssql", "oracle", "snowflake"}

var schemaRequired = map[string]bool{"postgresql": true, "mysql": false, "mssql": true, "oracle": true, "snowflake": true}

func validateTransfer(v validator, transfer *Transfer) {
	v.check(transfer.SourceType != "", "source-type", "must be provided")
	v.check(permittedValue(transfer.SourceType, permittedSources...), "source-type", fmt.Sprintf("must be one of %v", permittedSources))
	v.check(transfer.SourceConnectionString != "", "source-connection-string", "must be provided")

	v.check(transfer.TargetType != "", "target-type", "must be provided")
	v.check(permittedValue(transfer.TargetType, permittedTargets...), "target-type", fmt.Sprintf("must be one of %v", permittedTargets))
	v.check(transfer.TargetConnectionString != "", "target-connection-string", "must be provided")

	v.check(transfer.Query != "", "query", "must be provided")
	v.check(transfer.TargetTable != "", "target-table", "must be provided")

	if schemaRequired[transfer.TargetType] {
		v.check(transfer.TargetSchema != "", "target-schema", fmt.Sprintf("must be provided for target type %v", transfer.TargetType))
	}

	if transfer.SourceName == "" {
		panic("source name not set")
	}

	if transfer.TargetName == "" {
		panic("target name not set")
	}

	if transfer.TargetType == "mssql" {
		v.check(transfer.TargetHostname != "", "target-hostname", "must be provided for target type mssql")
		v.check(transfer.TargetUsername != "", "target-username", "must be provided for target type mssql")
		v.check(transfer.TargetPassword != "", "target-password", "must be provided for target type mssql")
		v.check(transfer.TargetDatabase != "", "target-database", "must be provided for target type mssql")
		v.check(bcpAvailable, "target-type", "bcp is not available on your os / cpu combination")
	}

	if transfer.TargetType == "postgresql" {
		v.check(psqlAvailable, "target-type", "psql is not available on your os / cpu combination")
	}

	if transfer.TargetType == "oracle" {
		v.check(sqlLdrAvailable, "target-type", "sqlldr is not available on your os / cpu combination")
	}

	if transfer.TargetType == "snowflake" {
		v.check(transfer.TargetSchema != "", "target-schema", "must be provided for target type snowflake")
	}
}
