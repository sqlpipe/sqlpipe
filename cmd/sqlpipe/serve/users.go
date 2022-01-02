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

type listUsersInput struct {
	Username string
	data.Filters
}

func getListUsersInput() {

}

func (app *application) listUsersApiHandler(w http.ResponseWriter, r *http.Request) {
	app.listUsers(w, r)
}

func (app *application) listUsers(w http.ResponseWriter, r *http.Request) ([]*data.User, data.Metadata, error) {

	input := listUsersInput{}

	v := validator.New()

	qs := r.URL.Query()

	input.Username = app.readString(qs, "username", "")

	input.Filters.Page = app.readInt(qs, "page", 1, v)
	input.Filters.PageSize = app.readInt(qs, "page_size", 20, v)

	input.Filters.Sort = app.readString(qs, "sort", "id")
	input.Filters.SortSafelist = []string{"id", "created_at", "username", "admin", "-id", "-created_at", "-username", "-admin"}

	if data.ValidateFilters(v, input.Filters); !v.Valid() {
		app.failedValidationResponse(w, r, v.Errors)
		return nil, nil
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
