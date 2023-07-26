package main

import (
	"errors"
	"fmt"
	"net/http"
	"time"
)

func transferError(transfer Transfer, err error) {

	errorLog.Printf("error running transfer %v from %v to %v :: %v", transfer.Id, transfer.SourceType, transfer.TargetType, err)

	transfer.Status = StatusError
	transfer.Err = err.Error()
	now := time.Now()
	transfer.StoppedAt = &now

	transferMap[transfer.Id] = transfer
}

func logError(r *http.Request, err error) {
	errorLog.Printf("error handling %s request to %s :: %v", r.Method, r.URL.String(), err)
}

func clientErrorResponse(w http.ResponseWriter, r *http.Request, status int, err error) {
	env := envelope{"error": err.Error()}

	err = writeJSON(w, status, env, nil)
	if err != nil {
		logError(r, err)
		w.WriteHeader(http.StatusInternalServerError)
	}
}

func serverErrorResponse(w http.ResponseWriter, r *http.Request, status int, err error) {
	logError(r, err)

	env := envelope{"error": err.Error()}

	err = writeJSON(w, status, env, nil)
	if err != nil {
		logError(r, err)
		w.WriteHeader(http.StatusInternalServerError)
	}
}

func notFoundResponse(w http.ResponseWriter, r *http.Request) {
	err := errors.New("the requested resource could not be found")
	clientErrorResponse(w, r, http.StatusNotFound, err)
}

func methodNotAllowedResponse(w http.ResponseWriter, r *http.Request) {
	err := fmt.Errorf("the %s method is not supported for this resource", r.Method)
	clientErrorResponse(w, r, http.StatusMethodNotAllowed, err)
}

func failedValidationResponse(w http.ResponseWriter, r *http.Request, errors map[string]string) {
	env := envelope{"error": errors}

	err := writeJSON(w, http.StatusUnprocessableEntity, env, nil)
	if err != nil {
		logError(r, err)
		w.WriteHeader(http.StatusInternalServerError)
	}
}
