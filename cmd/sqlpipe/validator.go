package main

import (
	"regexp"
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

func matches(value string, rx *regexp.Regexp) bool {
	return rx.MatchString(value)
}

func unique[T comparable](values []T) bool {
	uniqueValues := make(map[T]bool)

	for _, value := range values {
		uniqueValues[value] = true
	}

	return len(values) == len(uniqueValues)
}

func validateTransfer(v validator, transfer transfer) {
	v.check(transfer.SourceName != "", "source-name", "must be provided")
	v.check(transfer.SourceConnectionString != "", "source-connection-string", "must be provided")

	v.check(transfer.TargetName != "", "target-name", "must be provided")
	v.check(transfer.TargetConnectionString != "", "target-connection-string", "must be provided")

	v.check(transfer.Query != "", "query", "must be provided")
	v.check(transfer.TargetTable != "", "target-table", "must be provided")
}
