package serve

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"reflect"
	"strconv"

	"github.com/calmitchell617/sqlpipe/internal/data"
	"github.com/calmitchell617/sqlpipe/internal/engine"
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

	input.Filters.Page = app.readInt(qs, "page", 1, v)
	input.Filters.PageSize = app.readInt(qs, "page_size", 10, v)

	input.Filters.Sort = app.readString(qs, "sort", "id")
	input.Filters.SortSafelist = []string{"id", "created_at", "-id", "-created_at"}

	data.ValidateFilters(v, input.Filters)

	return input, v.Errors
}

func (app *application) listConnectionsUiHandler(w http.ResponseWriter, r *http.Request) {
	input, validationErrors := app.getListConnectionsInput(r)
	if !reflect.DeepEqual(validationErrors, map[string]string{}) {
		app.failedValidationResponse(w, r, validationErrors)
		return
	}

	connections, metadata, err := app.models.Connections.GetAll(input.Filters)
	if err != nil {
		app.serverErrorResponse(w, r, err)
		return
	}

	connections, errProperties, err := engine.TestConnections(connections)
	if err != nil {
		if err != nil {
			app.logger.PrintError(err, errProperties)
		}
	}

	paginationData := getPaginationData(metadata.CurrentPage, int(metadata.TotalRecords), metadata.PageSize, "connections")
	app.render(w, r, "connections.page.tmpl", &templateData{Connections: connections, Metadata: metadata, PaginationData: &paginationData})
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

	if r.PostForm.Get("skipTest") != "on" {
		connection, errProperties, err := engine.TestConnection(connection)
		if err != nil {
			app.logger.PrintError(err, errProperties)
		}
		if !connection.CanConnect {
			form.Validator.AddError("canConnect", "Unable to connect with given credentials")
			app.render(w, r, "create-connection.page.tmpl", &templateData{Connection: connection, Form: form})
			return
		}
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

	connection, errProperties, err := engine.TestConnection(connection)
	if err != nil {
		app.logger.PrintError(err, errProperties)
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

	if r.PostForm.Get("skipTest") != "on" {
		connection, errProperties, err := engine.TestConnection(connection)
		if err != nil {
			app.logger.PrintError(err, errProperties)
		}
		if !connection.CanConnect {
			form.Validator.AddError("canConnect", "Unable to connect with given credentials")
			app.render(w, r, "update-connection.page.tmpl", &templateData{Connection: connection, Form: form})
			return
		}
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

func (app *application) createConnectionApiHandler(w http.ResponseWriter, r *http.Request) {

	var input struct {
		Name      string `json:"name"`
		DsType    string `json:"dsType"`
		Hostname  string `json:"hostname"`
		Port      int    `json:"port"`
		AccountId string `json:"accountId"`
		DbName    string `json:"dbName"`
		Username  string `json:"username"`
		Password  string `json:"password"`
		SkipTest  bool   `json:"skipTest"`
	}

	err := app.readJSON(w, r, &input)
	if err != nil {
		app.badRequestResponse(w, r, err)
		return
	}

	connection := &data.Connection{
		Name:      input.Name,
		DsType:    input.DsType,
		Hostname:  input.Hostname,
		Port:      input.Port,
		AccountId: input.AccountId,
		DbName:    input.DbName,
		Username:  input.Username,
		Password:  input.Password,
	}

	v := validator.New()

	if data.ValidateConnection(v, connection); !v.Valid() {
		app.failedValidationResponse(w, r, v.Errors)
		return
	}

	if !input.SkipTest {
		connection, errProperties, err := engine.TestConnection(connection)
		if err != nil {
			app.logger.PrintError(err, errProperties)
		}
		if !connection.CanConnect {
			v.AddError("canConnect", "Unable to connect with given credentials")
			app.failedValidationResponse(w, r, v.Errors)
			return
		}
	}

	connection, err = app.models.Connections.Insert(connection)
	if err != nil {
		switch {
		case errors.Is(err, data.ErrDuplicateConnectionName):
			v.AddError("name", "a connection with this name already exists")
			app.failedValidationResponse(w, r, v.Errors)
		default:
			app.serverErrorResponse(w, r, err)
		}
		return
	}

	err = app.writeJSON(w, http.StatusAccepted, envelope{"connection": connection}, nil)
	if err != nil {
		app.serverErrorResponse(w, r, err)
	}
}

func (app *application) listConnectionsApiHandler(w http.ResponseWriter, r *http.Request) {
	input, validationErrors := app.getListConnectionsInput(r)
	if !reflect.DeepEqual(validationErrors, map[string]string{}) {
		app.failedValidationResponse(w, r, validationErrors)
	}

	connections, metadata, err := app.models.Connections.GetAll(input.Filters)
	if err != nil {
		app.serverErrorResponse(w, r, err)
		return
	}

	connections, errProperties, err := engine.TestConnections(connections)
	if err != nil {
		app.logger.PrintError(err, errProperties)
	}

	err = app.writeJSON(w, http.StatusOK, envelope{"connections": connections, "metadata": metadata}, nil)
	if err != nil {
		app.serverErrorResponse(w, r, err)
	}
}

func (app *application) showConnectionApiHandler(w http.ResponseWriter, r *http.Request) {
	id, err := app.readIDParam(r)
	if err != nil {
		app.notFoundResponse(w, r)
		return
	}

	connection, err := app.models.Connections.GetById(id)
	if err != nil {
		switch {
		case errors.Is(err, data.ErrRecordNotFound):
			app.notFoundResponse(w, r)
		default:
			app.serverErrorResponse(w, r, err)
		}
		return
	}

	err = app.writeJSON(w, http.StatusOK, envelope{"connection": connection}, nil)
	if err != nil {
		app.serverErrorResponse(w, r, err)
	}
}

func (app *application) updateConnectionApiHandler(w http.ResponseWriter, r *http.Request) {
	id, err := app.readIDParam(r)
	if err != nil {
		app.notFoundResponse(w, r)
		return
	}

	var input struct {
		Name      *string
		DsType    *string
		Hostname  *string
		Port      *int
		AccountId *string
		DbName    *string
		Username  *string
		Password  *string
	}

	err = app.readJSON(w, r, &input)
	if err != nil {
		app.badRequestResponse(w, r, err)
		return
	}

	v := validator.New()
	connection, err := app.models.Connections.GetById(id)
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

	if input.Name != nil {
		connection.Name = *input.Name
	}
	if input.DsType != nil {
		connection.DsType = *input.DsType
	}
	if input.Hostname != nil {
		connection.Hostname = *input.Hostname
	}
	if input.Port != nil {
		connection.Port = *input.Port
	}
	if input.AccountId != nil {
		connection.AccountId = *input.AccountId
	}
	if input.DbName != nil {
		connection.DbName = *input.DbName
	}
	if input.Username != nil {
		connection.Username = *input.Username
	}
	if input.Password != nil {
		connection.Password = *input.Password
	}

	if data.ValidateConnection(v, connection); !v.Valid() {
		app.failedValidationResponse(w, r, v.Errors)
		return
	}

	app.models.Connections.Update(connection)
	if err != nil {
		switch {
		case errors.Is(err, data.ErrEditConflict):
			app.editConflictResponse(w, r)
		default:
			app.serverErrorResponse(w, r, err)
		}
		return
	}

	err = app.writeJSON(w, http.StatusOK, envelope{"connection": connection}, nil)
	if err != nil {
		app.serverErrorResponse(w, r, err)
	}
}

func (app *application) deleteConnectionApiHandler(w http.ResponseWriter, r *http.Request) {
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

	err = app.writeJSON(w, http.StatusOK, envelope{"message": "connection successfully deleted"}, nil)
	if err != nil {
		app.serverErrorResponse(w, r, err)
	}
}
