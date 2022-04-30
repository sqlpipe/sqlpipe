package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/sqlpipe/sqlpipe/internal/data"
	"github.com/sqlpipe/sqlpipe/internal/globals"
	"github.com/sqlpipe/sqlpipe/internal/validator"
)

func (app *application) createUserHandler(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Username string `json:"username"`
		Password string `json:"password"`
		Admin    bool   `json:"admin"`
	}

	if err := app.readJSON(w, r, &input); err != nil {
		app.badRequestResponse(w, r, err)
		return
	}

	newUser := data.User{
		Username: input.Username,
		Admin:    input.Admin,
	}

	if err = newUser.SetPassword(input.Password); err != nil {
		app.serverErrorResponse(w, r, err)
		return
	}

	v := validator.New()

	if data.ValidateUser(v, newUser); !v.Valid() {
		app.failedValidationResponse(w, r, v.Errors)
		return
	}

	callingUser := app.contextGetUser(r)

	scrubbedUser, err := app.models.Users.InsertCheckToken(newUser, *callingUser)
	if err != nil {
		switch {
		case errors.Is(err, data.ErrDuplicateUsername):
			v.AddError("username", "a user with this username already exists")
			app.failedValidationResponse(w, r, v.Errors)
		case errors.Is(err, data.ErrInvalidCredentials):
			app.invalidAuthenticationTokenResponse(w, r)
		default:
			app.serverErrorResponse(w, r, err)
		}
		return
	}

	if err = app.writeJSON(w, http.StatusAccepted, envelope{"user": scrubbedUser}, nil); err != nil {
		app.serverErrorResponse(w, r, err)
		return
	}
}

func (app *application) showUserHandler(w http.ResponseWriter, r *http.Request) {
	username, err := app.readStringParam(r, "username")
	if err != nil || username == nil {
		app.notFoundResponse(w, r)
		return
	}

	scrubbedUser, err := app.models.Users.Get(*username)
	if err != nil {
		switch {
		case errors.Is(err, data.ErrRecordNotFound):
			app.notFoundResponse(w, r)
		default:
			app.serverErrorResponse(w, r, err)
		}
		return
	}

	if err = app.writeJSON(w, http.StatusOK, envelope{"user": scrubbedUser}, nil); err != nil {
		app.serverErrorResponse(w, r, err)
		return
	}
}

func (app *application) updateUserHandler(w http.ResponseWriter, r *http.Request) {
	username, err := app.readStringParam(r, "username")
	if err != nil || username == nil {
		app.notFoundResponse(w, r)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), globals.EtcdLongTimeout)
	defer cancel()

	user, err := app.models.Users.GetUserWithPasswordWithContext(*username, &ctx)
	if err != nil {
		switch {
		case errors.Is(err, data.ErrRecordNotFound):
			app.notFoundResponse(w, r)
		default:
			app.serverErrorResponse(w, r, err)
		}
		return
	}

	var input struct {
		Username *string `json:"username"`
		Password *string `json:"password"`
		Admin    *bool   `json:"admin"`
	}

	if err = app.readJSON(w, r, &input); err != nil {
		app.badRequestResponse(w, r, err)
		return
	}

	v := validator.New()

	if input.Username != nil {
		if v.Check(input.Username == &user.Username, "username", "cannot be changed"); !v.Valid() {
			app.failedValidationResponse(w, r, v.Errors)
			return
		}
	}

	if input.Password != nil {
		if data.ValidatePassword(v, *input.Password); !v.Valid() {
			app.failedValidationResponse(w, r, v.Errors)
			return
		}

		if err = user.SetPassword(*input.Password); err != nil {
			app.serverErrorResponse(w, r, err)
			return
		}
	}

	if input.Admin != nil {
		user.Admin = *input.Admin
	}

	if data.ValidateUser(v, user); !v.Valid() {
		app.failedValidationResponse(w, r, v.Errors)
		return
	}

	if err = app.models.Users.UpdateNoLock(user, ctx); err != nil {
		app.serverErrorResponse(w, r, err)
		return
	}

	// If you changed the user's password, delete all of their outstanding authentication tokens
	if input.Password != nil {
		// TODO: SHOULD I BE PASSING A POINTER HERE?
		if err = app.models.Tokens.DeleteAllForUserWithContext(user.Username, ctx); err != nil {
			if err != data.ErrRecordNotFound {
				app.serverErrorResponse(w, r, err)
				return
			}
		}
	}

	scrubbedUser := user.Scrub()

	err = app.writeJSON(w, http.StatusOK, envelope{"user": scrubbedUser}, nil)
	if err != nil {
		app.serverErrorResponse(w, r, err)
		return
	}
}

func (app *application) deleteUserHandler(w http.ResponseWriter, r *http.Request) {
	username, err := app.readStringParam(r, "username")
	if err != nil || username == nil {
		app.notFoundResponse(w, r)
		return
	}

	if err = app.models.Users.Delete(*username); err != nil {
		switch {
		case errors.Is(err, data.ErrRecordNotFound):
			app.notFoundResponse(w, r)
		default:
			app.serverErrorResponse(w, r, err)
		}
		return
	}

	err = app.writeJSON(w, http.StatusOK, envelope{"message": fmt.Sprintf("%v deleted", username)}, nil)
	if err != nil {
		app.serverErrorResponse(w, r, err)
		return
	}
}

func (app *application) listUsersHandler(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Username     string
		Admin        *bool
		CreatedAt    time.Time
		LastModified time.Time
		data.Filters
	}

	v := validator.New()

	qs := r.URL.Query()

	input.Username = app.readString(qs, "username", "")
	input.Admin = app.readBool(qs, "admin", v)

	input.Filters.Page = app.readInt(qs, "page", 1, v)
	input.Filters.PageSize = app.readInt(qs, "page_size", 20, v)

	input.Filters.Sort = app.readString(qs, "sort", "username")
	input.Filters.SortSafelist = []string{"username", "created_at", "last_modified", "-username", "-created_at", "-last_modified"}

	if data.ValidateFilters(v, input.Filters); !v.Valid() {
		app.failedValidationResponse(w, r, v.Errors)
		return
	}

	users, metadata, err := app.models.Users.GetAll(input.Username, input.Admin, input.Filters)
	if err != nil {
		app.serverErrorResponse(w, r, err)
		return
	}

	err = app.writeJSON(w, http.StatusOK, envelope{"users": users, "metadata": metadata}, nil)
	if err != nil {
		app.serverErrorResponse(w, r, err)
		return
	}
}
