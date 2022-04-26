package main

import (
	"fmt"
	"net/http"
	"time"

	"github.com/sqlpipe/sqlpipe/internal/validator"
)

func (app *application) createAuthenticationTokenHandler(w http.ResponseWriter, r *http.Request) {
	daysValidPointer, err := app.readIntParam(r, "daysValid")
	if err != nil {
		app.badRequestResponse(w, r, err)
		return
	}

	var daysValid int64
	switch daysValidPointer {
	case nil:
		daysValid = 1
	default:
		daysValid = *daysValidPointer
	}

	v := validator.New()

	if v.Check(daysValid > 0, "integerParameter", "must be greater than 0 (denotes how many days auth token will be valid)"); !v.Valid() {
		app.failedValidationResponse(w, r, v.Errors)
		return
	}

	v.Check(daysValid <= 366, "integerParameter", "must be less than or equal to 366 (denotes how many days auth token will be valid)")

	if !v.Valid() {
		app.failedValidationResponse(w, r, v.Errors)
		return
	}

	user := app.contextGetUser(r)

	if daysValid == 0 {
		daysValid = 1
	}

	duration, err := time.ParseDuration(fmt.Sprintf("%vh", daysValid))
	if err != nil {
		panic("unable to parse time duration")
	}

	token, err := app.models.Tokens.New(user.Username, duration)
	if err != nil {
		app.serverErrorResponse(w, r, err)
		return
	}

	if err = app.writeJSON(w, http.StatusCreated, envelope{"authentication_token": token}, nil); err != nil {
		app.serverErrorResponse(w, r, err)
		return
	}
}
