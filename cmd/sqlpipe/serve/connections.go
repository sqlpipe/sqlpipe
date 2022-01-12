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

type listConnectionsInput struct {
	Name   string
	DsType string
	data.Filters
}

func (app *application) getListConnectionsInput(r *http.Request) (input listConnectionsInput, err map[string]string) {
	v := validator.New()

	qs := r.URL.Query()

	input.Name = app.readString(qs, "name", "")
	input.DsType = app.readString(qs, "dsType", "")

	input.Filters.Page = app.readInt(qs, "page", 1, v)
	input.Filters.PageSize = app.readInt(qs, "page_size", 20, v)

	input.Filters.Sort = app.readString(qs, "sort", "id")
	input.Filters.SortSafelist = []string{"id", "created_at", "name", "dsType", "-id", "-created_at", "-name", "-dsType"}

	data.ValidateFilters(v, input.Filters)

	return input, v.Errors
}

func (app *application) listConnectionsUiHandler(w http.ResponseWriter, r *http.Request) {
	input, validationErrors := app.getListConnectionsInput(r)
	if !reflect.DeepEqual(validationErrors, map[string]string{}) {
		app.failedValidationResponse(w, r, validationErrors)
		return
	}

	connections, metadata, err := app.models.Connections.GetAll(input.Name, input.Filters)
	if err != nil {
		app.serverErrorResponse(w, r, err)
		return
	}

	app.render(w, r, "connections.page.tmpl", &templateData{Connections: connections, Metadata: metadata})
}

func (app *application) createConnectionFormUiHandler(w http.ResponseWriter, r *http.Request) {
	app.render(w, r, "create-connection.page.tmpl", &templateData{Form: forms.New(nil)})
}

func (app *application) createConnectionUiHandler(w http.ResponseWriter, r *http.Request) {
	err := r.ParseForm()
	if err != nil {
		app.errorResponse(w, r, http.StatusBadRequest, "unable to parse create connection form")
		return
	}

	port := 0
	if r.PostForm.Get("port") != "" {
		port, err = strconv.Atoi(r.PostForm.Get("port"))
		if err != nil {
			app.errorResponse(w, r, http.StatusBadRequest, "non int value given to port")
			return
		}
	}

	connection := &data.Connection{
		Name:      r.PostForm.Get("name"),
		DsType:    r.PostForm.Get("dsType"),
		Hostname:  r.PostForm.Get("hostname"),
		Port:      port,
		AccountId: r.PostForm.Get("accountId"),
		DbName:    r.PostForm.Get("dbName"),
		Username:  r.PostForm.Get("username"),
		Password:  r.PostForm.Get("password"),
	}

	form := forms.New(r.PostForm)

	if data.ValidateConnection(form.Validator, connection); !form.Validator.Valid() {
		app.render(w, r, "create-connection.page.tmpl", &templateData{Form: form})
		return
	}

	canConnect := true
	if r.PostForm.Get("skipTest") != "on" {
		canConnect = testConnection(*connection)
	}

	if !canConnect {
		form.Validator.AddError("canConnect", "Unable to connect with given credentials")
		app.render(w, r, "create-connection.page.tmpl", &templateData{Form: form})
		return
	}

	connection, err = app.models.Connections.Insert(connection)
	if err != nil {
		switch {
		case errors.Is(err, data.ErrDuplicateConnectionName):
			form.Validator.AddError("name", "a connection with this name already exists")
			app.render(w, r, "create-connection.page.tmpl", &templateData{Form: form})
		default:
			app.serverErrorResponse(w, r, err)
		}
		return
	}

	app.session.Put(r, "flash", fmt.Sprintf("Connection %d created", connection.ID))
	http.Redirect(w, r, "/ui/connections", http.StatusSeeOther)
}

func (app *application) showConnectionUiHandler(w http.ResponseWriter, r *http.Request) {
	id, err := app.readIDParam(r)
	if err != nil || id < 1 {
		app.notFoundResponse(w, r)
		return
	}

	connection, err := app.models.Connections.GetById(id)
	if err != nil {
		if errors.Is(err, data.ErrRecordNotFound) {
			app.notFoundResponse(w, r)
		} else {
			app.serverErrorResponse(w, r, err)
		}
		return
	}

	app.render(w, r, "connection-detail.page.tmpl", &templateData{Connection: connection})
}

func (app *application) updateConnectionFormUiHandler(w http.ResponseWriter, r *http.Request) {
	id, err := app.readIDParam(r)
	if err != nil || id < 1 {
		app.notFoundResponse(w, r)
		return
	}

	connection, err := app.models.Connections.GetById(id)
	if err != nil {
		if errors.Is(err, data.ErrRecordNotFound) {
			app.notFoundResponse(w, r)
		} else {
			app.serverErrorResponse(w, r, err)
		}
		return
	}

	form := forms.New(
		url.Values{
			"name":      []string{connection.Name},
			"dsType":    []string{connection.DsType},
			"hostname":  []string{connection.Hostname},
			"port":      []string{fmt.Sprint(connection.Port)},
			"accountId": []string{connection.AccountId},
			"dbName":    []string{connection.DbName},
			"username":  []string{connection.Username},
		},
	)

	app.render(w, r, "update-connection.page.tmpl", &templateData{Connection: connection, Form: form})
}

func (app *application) updateConnectionUiHandler(w http.ResponseWriter, r *http.Request) {
	err := r.ParseForm()
	if err != nil {
		app.errorResponse(w, r, http.StatusBadRequest, "unable to parse update connection form")
		return
	}

	id, err := strconv.ParseInt(r.PostForm.Get("id"), 10, 64)
	if err != nil {
		app.errorResponse(w, r, http.StatusBadRequest, "unable to parse connection id from update form")
		return
	}

	version, err := strconv.Atoi(r.PostForm.Get("version"))
	if err != nil {
		app.errorResponse(w, r, http.StatusBadRequest, "unable to parse connection version from update form")
		return
	}

	port := 0
	if r.PostForm.Get("port") != "" {
		port, err = strconv.Atoi(r.PostForm.Get("port"))
		if err != nil {
			app.errorResponse(w, r, http.StatusBadRequest, "non int value given to port")
			return
		}
	}

	connection := &data.Connection{
		ID:        id,
		Name:      r.PostForm.Get("name"),
		DsType:    r.PostForm.Get("dsType"),
		Hostname:  r.PostForm.Get("hostname"),
		Port:      port,
		AccountId: r.PostForm.Get("accountId"),
		DbName:    r.PostForm.Get("dbName"),
		Username:  r.PostForm.Get("username"),
		Password:  r.PostForm.Get("password"),
		Version:   version,
	}

	form := forms.New(r.PostForm)

	if data.ValidateConnection(form.Validator, connection); !form.Validator.Valid() {
		app.render(w, r, "update-connection.page.tmpl", &templateData{Connection: connection, Form: form})
		return
	}

	canConnect := true
	if r.PostForm.Get("skipTest") != "on" {
		canConnect = testConnection(*connection)
	}

	if !canConnect {
		form.Validator.AddError("canConnect", "Unable to connect with given credentials")
		app.render(w, r, "update-connection.page.tmpl", &templateData{Connection: connection, Form: form})
		return
	}

	err = app.models.Connections.Update(connection)
	if err != nil {
		switch {
		case errors.Is(err, data.ErrDuplicateConnectionName):
			form.Validator.AddError("name", "a connection with this name already exists")
			app.render(w, r, "update-connection.page.tmpl", &templateData{Connection: connection, Form: form})
		default:
			app.serverErrorResponse(w, r, err)
		}
		return
	}
	app.session.Put(r, "flash", fmt.Sprintf("Connection %d updated", id))
	http.Redirect(w, r, fmt.Sprintf("/ui/connections/%d", id), http.StatusSeeOther)
}

func (app *application) deleteConnectionUiHandler(w http.ResponseWriter, r *http.Request) {
	id, err := app.readIDParam(r)
	if err != nil {
		app.notFoundResponse(w, r)
		return
	}

	err = app.models.Connections.Delete(id)
	if err != nil {
		switch {
		case errors.Is(err, data.ErrRecordNotFound):
			app.notFoundResponse(w, r)
		default:
			app.serverErrorResponse(w, r, err)
		}
		return
	}

	app.session.Put(r, "flash", fmt.Sprintf("Connection %d deleted", id))
	http.Redirect(w, r, "/ui/connections", http.StatusSeeOther)
}

func testConnection(connection data.Connection) bool {
	return false
}
