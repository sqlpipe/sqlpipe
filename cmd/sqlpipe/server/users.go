package server

import (
	"errors"
	"net/http"
	"time"

	"github.com/calmitchell617/sqlpipe/internal/models/postgresql"
	"github.com/calmitchell617/sqlpipe/internal/validator"
)

func (app *application) createUserHandler(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Username string `json:"username"`
		Email    string `json:"email"`
		Password string `json:"password"`
		Admin    bool   `json:"admin"`
	}

	err := app.readJSON(w, r, &input)
	if err != nil {
		app.badRequestResponse(w, r, err)
		return
	}

	user := &postgresql.User{
		Username: input.Username,
		Email:    input.Email,
		Admin:    input.Admin,
	}

	err = user.Password.Set(input.Password)
	if err != nil {
		app.serverErrorResponse(w, r, err)
		return
	}

	v := validator.New()

	if postgresql.ValidateUser(v, user); !v.Valid() {
		app.failedValidationResponse(w, r, v.Errors)
		return
	}

	err = app.models.Users.Insert(user)
	if err != nil {
		switch {
		case errors.Is(err, postgresql.ErrDuplicateUsername):
			v.AddError("username", "a user with this username already exists")
			app.failedValidationResponse(w, r, v.Errors)
		default:
			app.serverErrorResponse(w, r, err)
		}
		return
	}

	err = app.writeJSON(w, http.StatusAccepted, envelope{"user": user}, nil)
	if err != nil {
		app.serverErrorResponse(w, r, err)
	}
}

func (app *application) createAdminUser(username string, password string) {
	// This function is only ever called by using the --create-admin flag when starting sqlpipe

	user := &postgresql.User{
		Username:  username,
		Activated: false,
		Admin:     true,
	}

	err := user.Password.Set(password)
	if err != nil {
		app.logger.PrintFatal(
			errors.New("there was an error setting the admin user's password"),
			map[string]string{"error": err.Error()},
		)
	}

	v := validator.New()

	if postgresql.ValidateUser(v, user); !v.Valid() {
		app.logger.PrintFatal(
			errors.New("failed to validate admin user"),
			v.Errors,
		)
	}

	err = app.models.Users.Insert(user)
	if err != nil {
		app.logger.PrintFatal(
			errors.New("failed to insert admin user"),
			map[string]string{"error": err.Error()},
		)
	}

	app.logger.PrintInfo(
		"successfully created admin user",
		map[string]string{},
	)
}

func (app *application) createAuthenticationTokenHandler(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}

	err := app.readJSON(w, r, &input)
	if err != nil {
		app.badRequestResponse(w, r, err)
		return
	}

	v := validator.New()

	postgresql.ValidateUsername(v, input.Username)
	postgresql.ValidatePasswordPlaintext(v, input.Password)

	if !v.Valid() {
		app.failedValidationResponse(w, r, v.Errors)
		return
	}

	user, err := app.models.Users.GetByUsername(input.Username)
	if err != nil {
		switch {
		case errors.Is(err, postgresql.ErrRecordNotFound):
			app.invalidCredentialsResponse(w, r)
		default:
			app.serverErrorResponse(w, r, err)
		}
		return
	}

	match, err := user.Password.Matches(input.Password)
	if err != nil {
		app.serverErrorResponse(w, r, err)
		return
	}

	if !match {
		app.invalidCredentialsResponse(w, r)
		return
	}

	token, err := app.models.Tokens.New(user.ID, 24*time.Hour, postgresql.ScopeAuthentication)
	if err != nil {
		app.serverErrorResponse(w, r, err)
		return
	}

	err = app.writeJSON(w, http.StatusCreated, envelope{"authentication_token": token}, nil)
	if err != nil {
		app.serverErrorResponse(w, r, err)
	}
}
