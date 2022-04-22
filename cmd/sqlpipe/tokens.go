package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/sqlpipe/sqlpipe/internal/data"
	"github.com/sqlpipe/sqlpipe/internal/globals"
	"github.com/sqlpipe/sqlpipe/internal/validator"
)

func (app *application) createAuthenticationTokenHandler(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Username  string `json:"username"`
		Password  string `json:"password"`
		DaysValid int    `json:"daysValid"`
	}

	err := app.readJSON(w, r, &input)
	if err != nil {
		app.badRequestResponse(w, r, err)
		return
	}

	v := validator.New()

	v.Check(input.Username != "", "username", "must be provided")
	v.Check(input.Password != "", "password", "must be provided")
	v.Check(input.DaysValid <= 366, "daysValid", "must be less than or equal to 366")

	if !v.Valid() {
		app.failedValidationResponse(w, r, v.Errors)
		return
	}

	session, err := concurrency.NewSession(app.models.Tokens.Etcd)
	if err != nil {
		app.serverErrorResponse(w, r, err)
	}
	defer session.Close()

	userKey := fmt.Sprintf("%v%v", UserPrefix, input.Username)
	mutex := concurrency.NewMutex(session, userKey)

	ctx, cancel := context.WithTimeout(context.Background(), globals.EtcdTimeout)
	defer cancel()

	err = mutex.Lock(ctx)
	if err != nil {
		app.serverErrorResponse(w, r, err)
	}

	user, err := app.models.Users.GetUserWithPasswordWithContext(input.Username, ctx)
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

	scrubbedUser := user.Scrub()

	if !match {
		app.invalidCredentialsResponse(w, r)
		return
	}

	if input.DaysValid == 0 {
		input.DaysValid = 1
	}

	duration, err := time.ParseDuration(fmt.Sprintf("%vh", 24*input.DaysValid))
	if err != nil {
		panic("unable to parse time duration")
	}

	token, err := app.models.Tokens.New(scrubbedUser.Username, duration, data.ScopeAuthentication, ctx)
	if err != nil {
		app.serverErrorResponse(w, r, err)
		return
	}

	err = app.writeJSON(w, http.StatusCreated, envelope{"authentication_token": token}, nil)
	if err != nil {
		app.serverErrorResponse(w, r, err)
	}
}
