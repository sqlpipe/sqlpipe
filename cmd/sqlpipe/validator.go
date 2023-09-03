package main

import (
	"fmt"
	"strings"
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

var schemaRequired = map[string]bool{TypePostgreSQL: true, TypeMySQL: false, TypeMSSQL: true, TypeOracle: true, TypeSnowflake: true}
var permittedSources = []string{TypePostgreSQL, TypeMySQL, TypeMSSQL, TypeOracle, TypeSnowflake}
var permittedTargets = []string{TypePostgreSQL, TypeMySQL, TypeMSSQL, TypeOracle, TypeSnowflake}

func validateTransferInput(v validator, transfer Transfer) {
	v.check(transfer.SourceType != "", "source-type", "must be provided")
	v.check(permittedValue(transfer.SourceType, permittedSources...), "source-type", fmt.Sprintf("must be one of %v", permittedSources))
	v.check(transfer.SourceConnectionString != "", "source-connection-string", "must be provided")
	v.check(transfer.TargetType != "", "target-type", "must be provided")
	v.check(permittedValue(transfer.TargetType, permittedTargets...), "target-type", fmt.Sprintf("must be one of %v", permittedTargets))
	v.check(transfer.TargetConnectionString != "", "target-connection-string", "must be provided")
	switch transfer.TargetType {
	case TypeOracle, TypeMSSQL:
		v.check(transfer.TargetHostname != "", "target-hostname", fmt.Sprintf("must be provided for target type %v", transfer.TargetType))
		v.check(transfer.TargetPort != 0, "target-port", fmt.Sprintf("must be provided for target type %v", transfer.TargetType))
		v.check(transfer.TargetUsername != "", "target-username", fmt.Sprintf("must be provided for target type %v", transfer.TargetType))
		v.check(transfer.TargetPassword != "", "target-password", fmt.Sprintf("must be provided for target type %v", transfer.TargetType))
		v.check(transfer.TargetDatabase != "", "target-database", fmt.Sprintf("must be provided for target type %v", transfer.TargetType))
	}
	if schemaRequired[transfer.TargetType] {
		v.check(transfer.TargetSchema != "", "target-schema", fmt.Sprintf("must be provided for target type %v", transfer.TargetType))
	}

	v.check(transfer.Query != "", "query", "must be provided")
	v.check(transfer.TargetTable != "", "target-table", "must be provided")

	switch transfer.SourceType {
	case TypeMySQL:
		v.check(strings.Contains(transfer.SourceConnectionString, "parseTime=true"), "source-connection-string", "must contain parseTime=true to move timestamp with time zone data from mysql")
		v.check(strings.Contains(transfer.SourceConnectionString, "loc="), "source-connection-string", `must contain loc=<URL_ENCODED_IANA_TIME_ZONE> to move timestamp with time zone data from mysql - example: loc=US%2FPacific`)
	}

	switch transfer.TargetType {
	case TypePostgreSQL:
		v.check(psqlAvailable, "target-type", "you must install psql to transfer data to postgresql")
	case TypeMSSQL:
		v.check(bcpAvailable, "target-type", "you must install bcp to transfer data to mssql")
	case TypeOracle:
		v.check(sqlldrAvailable, "target-type", "you must install SQL*Loader to transfer data to oracle")
	}

	validateTransferGeneratedFields(transfer)
}

func validateTransferGeneratedFields(transfer Transfer) {
	if transfer.Id == "" {
		panic("id not set")
	}

	if transfer.CreatedAt.IsZero() {
		panic("created at not set")
	}

	if transfer.StoppedAt != "" {
		panic("stopped at is unexpectedly set")
	}

	if transfer.Status != StatusRunning {
		panic("status not set to running")
	}

	if transfer.Err != "" {
		panic("error is unexpectedly set")
	}

	if transfer.TmpDir == "" {
		panic("tmp dir not set")
	}

	if transfer.PipeFileDir == "" {
		panic("pipe file dir not set")
	}

	if transfer.FinalCsvDir == "" {
		panic("final csv dir not set")
	}

	if transfer.Delimiter == "" {
		panic("delimiter not set")
	}

	if transfer.Newline == "" {
		panic("newline not set")
	}

	if transfer.Null == "" {
		panic("null value not set")
	}

	if transfer.SourceName == "" {
		panic("source name not set")
	}

	if transfer.TargetName == "" {
		panic("target name not set")
	}
}
