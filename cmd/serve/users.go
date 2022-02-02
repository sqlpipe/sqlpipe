package serve

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"reflect"
	"strconv"

	"github.com/calmitchell617/sqlpipe/internal/data"
	"github.com/calmitchell617/sqlpipe/internal/forms.go"
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

	_, err = app.models.Users.Insert(user)
	if err != nil {
		switch {
		case errors.Is(err, data.ErrDuplicateUsername):
			app.logger.PrintFatal(
				errors.New("there is already a user with this username"),
				map[string]string{"error": err.Error()},
			)
		default:
			app.logger.PrintFatal(
				errors.New("failed to insert admin user"),
				map[string]string{"error": err.Error()},
			)
		}
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

	user, err = app.models.Users.Insert(user)
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

	input.Filters.Page = app.readInt(qs, "page", 1, v)
	input.Filters.PageSize = app.readInt(qs, "page_size", 100, v)

	input.Filters.Sort = app.readString(qs, "sort", "id")
	input.Filters.SortSafelist = []string{"id", "created_at", "-id", "-created_at"}

	data.ValidateFilters(v, input.Filters)

	return input, v.Errors
}

func (app *application) listUsersApiHandler(w http.ResponseWriter, r *http.Request) {
	input, validationErrors := app.getListUsersInput(r)
	if !reflect.DeepEqual(validationErrors, map[string]string{}) {
		app.failedValidationResponse(w, r, validationErrors)
	}

	users, metadata, err := app.models.Users.GetAll(input.Filters)
	if err != nil {
		app.serverErrorResponse(w, r, err)
		return
	}

	err = app.writeJSON(w, http.StatusOK, envelope{"users": users, "metadata": metadata}, nil)
	if err != nil {
		app.serverErrorResponse(w, r, err)
	}
}

func (app *application) updateUserApiHandler(w http.ResponseWriter, r *http.Request) {
	id, err := app.readIDParam(r)
	if err != nil {
		app.notFoundResponse(w, r)
		return
	}

	var input struct {
		Username string
		Password string
		Admin    bool
	}

	err = app.readJSON(w, r, &input)
	if err != nil {
		app.badRequestResponse(w, r, err)
		return
	}

	v := validator.New()
	user := &data.User{
		ID:       id,
		Username: input.Username,
		Admin:    input.Admin,
	}

	err = user.Password.Set(input.Password)
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
		app.serverErrorResponse(w, r, err)
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

func (app *application) listUsersUiHandler(w http.ResponseWriter, r *http.Request) {
	input, validationErrors := app.getListUsersInput(r)
	if !reflect.DeepEqual(validationErrors, map[string]string{}) {
		app.failedValidationResponse(w, r, validationErrors)
		return
	}

	users, metadata, err := app.models.Users.GetAll(input.Filters)
	if err != nil {
		app.serverErrorResponse(w, r, err)
		return
	}

	paginationData := getPaginationData(metadata.CurrentPage, int(metadata.TotalRecords), metadata.PageSize, "users")
	app.render(w, r, "users.page.tmpl", &templateData{Users: users, Metadata: metadata, PaginationData: &paginationData})
}

func (app *application) createUserFormUiHandler(w http.ResponseWriter, r *http.Request) {
	app.render(w, r, "create-user.page.tmpl", &templateData{Form: forms.New(nil)})
}

func (app *application) createUserUiHandler(w http.ResponseWriter, r *http.Request) {
	err := r.ParseForm()
	if err != nil {
		app.errorResponse(w, r, http.StatusBadRequest, "unable to parse create user form")
		return
	}

	user := &data.User{
		Username: r.PostForm.Get("username"),
		Admin:    r.PostForm.Get("admin") == "on",
	}

	err = user.Password.Set(r.PostForm.Get("password"))
	if err != nil {
		app.serverErrorResponse(w, r, err)
		return
	}

	form := forms.New(r.PostForm)

	if data.ValidateUser(form.Validator, user); !form.Validator.Valid() {
		app.render(w, r, "create-user.page.tmpl", &templateData{Form: form})
		return
	}

	user, err = app.models.Users.Insert(user)
	if err != nil {
		switch {
		case errors.Is(err, data.ErrDuplicateUsername):
			form.Validator.AddError("username", "a user with this username already exists")
			app.render(w, r, "create-user.page.tmpl", &templateData{Form: form})
		default:
			app.serverErrorResponse(w, r, err)
		}
		return
	}

	app.session.Put(r, "flash", fmt.Sprintf("User %d created", user.ID))
	http.Redirect(w, r, fmt.Sprintf("/ui/users/%d", user.ID), http.StatusSeeOther)
}

func (app *application) loginUserFormUiHandler(w http.ResponseWriter, r *http.Request) {
	app.render(w, r, "login.page.tmpl", &templateData{Form: forms.New(nil)})
}

func (app *application) loginUserUiHandler(w http.ResponseWriter, r *http.Request) {
	err := r.ParseForm()
	if err != nil {
		app.errorResponse(w, r, http.StatusBadRequest, "unable to parse login form")
		return
	}

	form := forms.New(r.PostForm)
	id, err := app.models.Users.Authenticate(form.Get("username"), form.Get("password"))
	if err != nil {
		if errors.Is(err, data.ErrInvalidCredentials) {
			form.Validator.AddError("generic", "Email or Password is incorrect")
			app.render(w, r, "login.page.tmpl", &templateData{Form: form})
		} else {
			app.serverErrorResponse(w, r, err)
		}
		return
	}

	app.session.Put(r, "authenticatedUserID", id)

	http.Redirect(w, r, "/", http.StatusSeeOther)
}

func (app *application) logoutUserUiHandler(w http.ResponseWriter, r *http.Request) {
	app.session.Remove(r, "authenticatedUserID")
	http.Redirect(w, r, "/", http.StatusSeeOther)
}

func (app *application) showUserUiHandler(w http.ResponseWriter, r *http.Request) {
	id, err := app.readIDParam(r)
	if err != nil || id < 1 {
		app.notFoundResponse(w, r)
		return
	}

	user, err := app.models.Users.GetById(id)
	if err != nil {
		if errors.Is(err, data.ErrRecordNotFound) {
			app.notFoundResponse(w, r)
		} else {
			app.serverErrorResponse(w, r, err)
		}
		return
	}

	app.render(w, r, "user-detail.page.tmpl", &templateData{User: user})
}

func (app *application) deleteUserUiHandler(w http.ResponseWriter, r *http.Request) {
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

	app.session.Put(r, "flash", fmt.Sprintf("User %d deleted", id))
	http.Redirect(w, r, "/ui/users", http.StatusSeeOther)
}

func (app *application) updateUserFormUiHandler(w http.ResponseWriter, r *http.Request) {
	id, err := app.readIDParam(r)
	if err != nil || id < 1 {
		app.notFoundResponse(w, r)
		return
	}

	user, err := app.models.Users.GetById(id)
	if err != nil {
		if errors.Is(err, data.ErrRecordNotFound) {
			app.notFoundResponse(w, r)
		} else {
			app.serverErrorResponse(w, r, err)
		}
		return
	}

	form := forms.New(url.Values{"username": []string{user.Username}, "admin": []string{fmt.Sprint(user.Admin)}})

	app.render(w, r, "update-user.page.tmpl", &templateData{User: user, Form: form})
}

func (app *application) updateUserUiHandler(w http.ResponseWriter, r *http.Request) {
	err := r.ParseForm()
	if err != nil {
		app.errorResponse(w, r, http.StatusBadRequest, "unable to parse update user form")
		return
	}

	id, err := strconv.ParseInt(r.PostForm.Get("id"), 10, 64)
	if err != nil {
		app.errorResponse(w, r, http.StatusBadRequest, "unable to parse user id from update form")
		return
	}

	user := &data.User{
		ID:       id,
		Username: r.PostForm.Get("username"),
		Admin:    r.PostForm.Get("admin") == "on",
	}

	err = user.Password.Set(r.PostForm.Get("password"))
	if err != nil {
		app.serverErrorResponse(w, r, err)
		return
	}

	form := forms.New(r.PostForm)

	if data.ValidateUser(form.Validator, user); !form.Validator.Valid() {
		fmt.Printf("\n\n%v\n\n", form.Validator)
		fmt.Printf("\n\n%v\n\n", form.Validator.Errors)
		app.render(w, r, "update-user.page.tmpl", &templateData{User: user, Form: form})
		return
	}

	err = app.models.Users.Update(user)
	if err != nil {
		switch {
		case errors.Is(err, data.ErrDuplicateUsername):
			form.Validator.AddError("username", "a user with this username already exists")
			app.render(w, r, "update-user.page.tmpl", &templateData{User: user, Form: form})
		default:
			app.serverErrorResponse(w, r, err)
		}
		return
	}
	app.session.Put(r, "flash", fmt.Sprintf("User %d updated", id))
	http.Redirect(w, r, fmt.Sprintf("/ui/users/%d", id), http.StatusSeeOther)
}
