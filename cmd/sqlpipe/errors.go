package main

import (
	"errors"
	"fmt"
	"net/http"
)

func (app *application) logError(r *http.Request, err error) {
	app.errorLog.Printf("error handling %s request to %s -> %v", r.Method, r.URL.String(), err)
}

func (app *application) clientErrorResponse(w http.ResponseWriter, r *http.Request, status int, err error) {
	env := envelope{"error": err.Error()}

	err = app.writeJSON(w, status, env, nil)
	if err != nil {
		app.logError(r, err)
		w.WriteHeader(http.StatusInternalServerError)
	}
}

func (app *application) errorResponse(w http.ResponseWriter, r *http.Request, status int, err error) {
	app.logError(r, err)

	env := envelope{"error": err.Error()}

	err = app.writeJSON(w, status, env, nil)
	if err != nil {
		app.logError(r, err)
		w.WriteHeader(http.StatusInternalServerError)
	}
}

func (app *application) notFoundResponse(w http.ResponseWriter, r *http.Request) {
	err := errors.New("the requested resource could not be found")
	app.errorResponse(w, r, http.StatusNotFound, err)
}

func (app *application) methodNotAllowedResponse(w http.ResponseWriter, r *http.Request) {
	err := fmt.Errorf("the %s method is not supported for this resource", r.Method)
	app.errorResponse(w, r, http.StatusMethodNotAllowed, err)
}

func (app *application) failedValidationResponse(w http.ResponseWriter, r *http.Request, errors map[string]string) {
	env := envelope{"error": errors}

	err := app.writeJSON(w, http.StatusUnprocessableEntity, env, nil)
	if err != nil {
		app.logError(r, err)
		w.WriteHeader(http.StatusInternalServerError)
	}
}
