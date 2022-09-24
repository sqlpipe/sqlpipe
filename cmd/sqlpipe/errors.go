package main

import (
	"errors"
	"fmt"
	"net/http"
)

func (app *application) logError(r *http.Request, err error) {
	app.logger.PrintError(err, map[string]string{
		"request_method": r.Method,
		"request_url":    r.URL.String(),
	})
}

func (app *application) errorResponse(w http.ResponseWriter, r *http.Request, status int, err error) {
	app.logError(r, err)
	env := map[string]any{"error": err.Error()}

	err = app.writeJSON(w, status, env, nil)
	if err != nil {
		app.logError(r, err)
		w.WriteHeader(500)
	}
}

func (app *application) notFoundResponse(w http.ResponseWriter, r *http.Request) {
	message := errors.New("the requested resource could not be found")
	app.errorResponse(w, r, http.StatusNotFound, message)
}

func (app *application) methodNotAllowedResponse(w http.ResponseWriter, r *http.Request) {
	message := fmt.Errorf("the %s method is not supported for this resource", r.Method)
	app.errorResponse(w, r, http.StatusMethodNotAllowed, message)
}

func (app *application) badRequestResponse(w http.ResponseWriter, r *http.Request, err error) {
	app.errorResponse(w, r, http.StatusBadRequest, err)
}

func (app *application) failedValidationResponse(w http.ResponseWriter, r *http.Request, errors map[string]string) {
	env := map[string]any{"error": errors}

	err := app.writeJSON(w, http.StatusUnprocessableEntity, env, nil)
	if err != nil {
		app.logError(r, err)
		w.WriteHeader(500)
	}
}

func (app *application) rateLimitExceededResponse(w http.ResponseWriter, r *http.Request) {
	message := errors.New("rate limit exceeded")
	app.errorResponse(w, r, http.StatusTooManyRequests, message)
}

func (app *application) invalidAuthenticationTokenResponse(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("WWW-Authenticate", "Bearer")

	message := errors.New("invalid or missing authentication token")
	app.errorResponse(w, r, http.StatusUnauthorized, message)
}
