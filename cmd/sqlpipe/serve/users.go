package serve

import (
	"errors"
	"net/http"

	"github.com/calmitchell617/sqlpipe/internal/data"
	"github.com/calmitchell617/sqlpipe/internal/validator"
)

func (app *application) createAdminUser(username string, password string) {
	// This function is only ever called by using the --create-admin flag when starting sqlpipe

	user := &data.User{
		Username: username,
		Admin:    true,
	}

	err := user.Password.Set(password)
	if err != nil {
		app.logger.PrintFatal(
			errors.New("there was an error setting the admin user's password"),
			map[string]string{"error": err.Error()},
		)
	}

	v := validator.New()

	if data.ValidateUser(v, user); !v.Valid() {
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

func (app *application) createUserApiHandler(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Username string `json:"username"`
		Password string `json:"password"`
		Admin    bool   `json:"admin"`
	}

	err := app.readJSON(w, r, &input)
	if err != nil {
		app.badRequestResponse(w, r, err)
		return
	}

	user := &data.User{
		Username: input.Username,
		Admin:    input.Admin,
	}

	err = user.Password.Set(input.Password)
	if err != nil {
		app.serverErrorResponse(w, r, err)
		return
	}

	v := validator.New()

	if data.ValidateUser(v, user); !v.Valid() {
		app.failedValidationResponse(w, r, v.Errors)
		return
	}

	err = app.models.Users.Insert(user)
	if err != nil {
		switch {
		case errors.Is(err, data.ErrDuplicateUsername):
			v.AddError("email", "a user with this username already exists")
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

type listUsersInput struct {
	Username string
	data.Filters
}

func (app *application) getListUsersInput(r *http.Request) (input listUsersInput, err map[string]string) {
	v := validator.New()

	qs := r.URL.Query()

	input.Username = app.readString(qs, "username", "")

	input.Filters.Page = app.readInt(qs, "page", 1, v)
	input.Filters.PageSize = app.readInt(qs, "page_size", 20, v)

	input.Filters.Sort = app.readString(qs, "sort", "id")
	input.Filters.SortSafelist = []string{"id", "created_at", "username", "admin", "-id", "-created_at", "-username", "-admin"}

	data.ValidateFilters(v, input.Filters)

	return input, v.Errors
}

func (app *application) listUsersApiHandler(w http.ResponseWriter, r *http.Request) {
	input, validationErrors := app.getListUsersInput(r)
	if validationErrors != nil {
		app.failedValidationResponse(w, r, validationErrors)
	}

	users, metadata, err := app.models.Users.GetAll(input.Username, input.Filters)
	if err != nil {
		app.serverErrorResponse(w, r, err)
		return
	}

	err = app.writeJSON(w, http.StatusOK, envelope{"users": users, "metadata": metadata}, nil)
	if err != nil {
		app.serverErrorResponse(w, r, err)
	}
}

type updateUserInput struct {
	ID       *int64
	Username *string
	Password *string
	Admin    *bool
	data.Filters
}

func (app *application) updateUserApiHandler(w http.ResponseWriter, r *http.Request) {
	input := updateUserInput{}
	v := validator.New()

	err := app.readJSON(w, r, &input)
	if err != nil {
		app.badRequestResponse(w, r, err)
		return
	}
	if input.ID == nil || input.Username == nil || input.Password == nil || input.Admin == nil {
		err = errors.New("updating a user requires providing an existing user's ID, along with a username, password, and admin status")
		app.badRequestResponse(w, r, err)
		return
	}

	user, err := app.models.Users.GetById(*input.ID)
	if err != nil {
		switch {
		case errors.Is(err, data.ErrRecordNotFound):
			v.AddError("id", "not found")
			app.failedValidationResponse(w, r, v.Errors)
		default:
			app.serverErrorResponse(w, r, err)
		}
		return
	}

	user.Username = *input.Username
	user.Admin = *input.Admin

	err = user.Password.Set(*input.Password)
	if err != nil {
		app.serverErrorResponse(w, r, err)
		return
	}

	if data.ValidateUser(v, user); !v.Valid() {
		app.failedValidationResponse(w, r, v.Errors)
		return
	}

	app.models.Users.Update(user)
	if err != nil {
		switch {
		case errors.Is(err, data.ErrEditConflict):
			app.editConflictResponse(w, r)
		default:
			app.serverErrorResponse(w, r, err)
		}
		return
	}

	err = app.writeJSON(w, http.StatusOK, envelope{"user": user}, nil)
	if err != nil {
		app.serverErrorResponse(w, r, err)
	}
}

func (app *application) showUserApiHandler(w http.ResponseWriter, r *http.Request) {
	id, err := app.readIDParam(r)
	if err != nil {
		app.notFoundResponse(w, r)
		return
	}

	user, err := app.models.Users.GetById(id)
	if err != nil {
		switch {
		case errors.Is(err, data.ErrRecordNotFound):
			app.notFoundResponse(w, r)
		default:
			app.serverErrorResponse(w, r, err)
		}
		return
	}

	err = app.writeJSON(w, http.StatusOK, envelope{"user": user}, nil)
	if err != nil {
		app.serverErrorResponse(w, r, err)
	}
}

func (app *application) deleteUserApiHandler(w http.ResponseWriter, r *http.Request) {
	id, err := app.readIDParam(r)
	if err != nil {
		app.notFoundResponse(w, r)
		return
	}

	err = app.models.Users.Delete(id)
	if err != nil {
		switch {
		case errors.Is(err, data.ErrRecordNotFound):
			app.notFoundResponse(w, r)
		default:
			app.serverErrorResponse(w, r, err)
		}
		return
	}

	err = app.writeJSON(w, http.StatusOK, envelope{"message": "user successfully deleted"}, nil)
	if err != nil {
		app.serverErrorResponse(w, r, err)
	}
}
