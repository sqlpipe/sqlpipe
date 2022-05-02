package main

import (
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

	var daysValid int64
	switch input.DaysValid {
	case nil:
		daysValid = 1
	default:
		daysValid = *input.DaysValid
	}

	v := validator.New()

	if v.Check(daysValid >= 0, "daysValid", "must be 0 <= daysInfinite < 366. 0 is infinite"); !v.Valid() {
		app.failedValidationResponse(w, r, v.Errors)
		return
	}

	v.Check(daysValid <= 366, "daysValid", "must be 0 <= daysInfinite < 366. 0 is infinite")

	if !v.Valid() {
		app.failedValidationResponse(w, r, v.Errors)
		return
	}

	user := data.User{
		Username: input.Username,
	}

	user.SetPassword(input.Password)

	var expiryUnixTime int64
	switch daysValid {
	case 0:
		expiryUnixTime = math.MaxInt64
	default:
		duration, err := time.ParseDuration(fmt.Sprintf("%vh", daysValid*24))
		if err != nil {
			panic("unable to parse time duration")
		}
		expiryUnixTime = time.Now().Add(duration).Unix()
	}

	expiryString := fmt.Sprint(expiryUnixTime)
	expiryStringLen := len(expiryString)
	if expiryStringLen < 19 {
		zerosToAdd := 19 - expiryStringLen
		for i := 0; i < zerosToAdd; i++ {
			expiryString = "0" + expiryString
		}
	}

	token, err := app.models.Tokens.New(user, expiryString)
	if err != nil {
		switch err {
		case data.ErrInvalidCredentials, data.ErrRecordNotFound:
			app.invalidAuthenticationTokenResponse(w, r)
			return
		default:
			app.serverErrorResponse(w, r, err)
			return
		}
	}

	if err = app.writeJSON(w, http.StatusCreated, envelope{"authentication_token": token}, nil); err != nil {
		app.serverErrorResponse(w, r, err)
		return
	}
}
