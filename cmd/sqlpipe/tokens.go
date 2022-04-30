package main

import (
	"errors"
	"fmt"
	"math"
	"net/http"
	"time"

	"github.com/sqlpipe/sqlpipe/internal/data"
	"github.com/sqlpipe/sqlpipe/internal/validator"
)

func (app *application) createAuthenticationTokenHandler(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Username  string `json:"username"`
		Password  string `json:"password"`
		DaysValid *int64 `json:"daysValid"`
	}

	err := app.readJSON(w, r, &input)
	if err != nil {
		app.badRequestResponse(w, r, err)
		return
	}

	v := validator.New()

	var daysValid int64
	switch input.DaysValid {
	case nil:
		daysValid = 0
	default:
		daysValid = *input.DaysValid
	}

	if v.Check(daysValid >= 0, "daysValid", "must be 0 <= daysInfinite < 366. 0 is infinite"); !v.Valid() {
		app.failedValidationResponse(w, r, v.Errors)
		return
	}

	v.Check(daysValid <= 366, "daysValid", "must be 0 <= daysInfinite < 366. 0 is infinite")

	if !v.Valid() {
		app.failedValidationResponse(w, r, v.Errors)
		return
	}

	user, err := app.models.Users.GetUserWithPassword(input.Username)
	if err != nil {
		switch {
		case errors.Is(err, data.ErrRecordNotFound):
			app.invalidCredentialsResponse(w, r)
		default:
			app.serverErrorResponse(w, r, err)
		}
		return
	}

	match, err := user.CheckPassword(input.Password)
	if err != nil {
		app.serverErrorResponse(w, r, err)
		return
	}

	if !match {
		app.invalidCredentialsResponse(w, r)
		return
	}

	var expiry int64
	switch daysValid {
	case 0:
		expiry = math.MaxInt64
	default:
		duration, err := time.ParseDuration(fmt.Sprintf("%vh", daysValid))
		if err != nil {
			panic("unable to parse time duration")
		}
		expiry = time.Now().Add(duration).Unix()
	}

	expiryString := fmt.Sprint(expiry)
	expiryStringLen := len(expiryString)
	if expiryStringLen < 19 {
		zerosToAdd := 19 - expiryStringLen
		for i := 0; i < zerosToAdd; i++ {
			expiryString = "0" + expiryString
		}
	}

	token, err := app.models.Tokens.New(user.Username, expiryString)
	if err != nil {
		app.serverErrorResponse(w, r, err)
		return
	}

	if err = app.writeJSON(w, http.StatusCreated, envelope{"authentication_token": token}, nil); err != nil {
		app.serverErrorResponse(w, r, err)
		return
	}
}
