package main

import (
	"errors"
	"fmt"
	"net/http"
)

func clientErrorResponse(w http.ResponseWriter, r *http.Request, status int, err error) {
	env := envelope{"error": err.Error()}

	err = writeJSON(w, status, env, nil)
	if err != nil {
		errorLog.Printf("error writing json :: %v", err)
		w.WriteHeader(http.StatusBadRequest)
	}
}

func serverErrorResponse(w http.ResponseWriter, r *http.Request, status int, err error) {
	errorLog.Printf("error handling %v request to %v :: %v", r.Method, r.URL.String(), err)

	env := envelope{"error": err.Error()}

	err = writeJSON(w, status, env, nil)
	if err != nil {
		errorLog.Printf("error writing json :: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
	}
}

func notFoundResponse(w http.ResponseWriter, r *http.Request) {
	err := errors.New("the requested resource could not be found")
	clientErrorResponse(w, r, http.StatusNotFound, err)
}

func methodNotAllowedResponse(w http.ResponseWriter, r *http.Request) {
	err := fmt.Errorf("the %v method is not supported for this resource", r.Method)
	clientErrorResponse(w, r, http.StatusMethodNotAllowed, err)
}

func failedValidationResponse(w http.ResponseWriter, r *http.Request, errors map[string]string) {
	env := envelope{"error": errors}

	err := writeJSON(w, http.StatusUnprocessableEntity, env, nil)
	if err != nil {
		errorLog.Printf("error writing json :: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
	}
}
