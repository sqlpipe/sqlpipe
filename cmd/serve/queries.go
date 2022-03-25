package serve

import (
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"strconv"
	"time"

	"github.com/sqlpipe/sqlpipe/internal/data"
	"github.com/sqlpipe/sqlpipe/internal/forms.go"
	"github.com/sqlpipe/sqlpipe/internal/validator"
)

func (app *application) createQueryApiHandler(w http.ResponseWriter, r *http.Request) {

	var input struct {
		ConnectionID int64  `json:"connectionId"`
		Query        string `json:"query"`
	}

	err := app.readJSON(w, r, &input)
	if err != nil {
		app.badRequestResponse(w, r, err)
		return
	}

	query := &data.Query{
		ConnectionID: input.ConnectionID,
		Query:        input.Query,
	}

	v := validator.New()

	if data.ValidateQuery(v, query); !v.Valid() {
		app.failedValidationResponse(w, r, v.Errors)
		return
	}

	query, err = app.models.Queries.Insert(query)
	if err != nil {
		app.serverErrorResponse(w, r, err)
		return
	}

	err = app.writeJSON(w, http.StatusAccepted, envelope{"query": query}, nil)
	if err != nil {
		app.serverErrorResponse(w, r, err)
	}
}

type listQueriesInput struct {
	data.Filters
}

func (app *application) getListQueriesInput(r *http.Request) (input listQueriesInput, err map[string]string) {
	v := validator.New()

	qs := r.URL.Query()

	input.Filters.Page = app.readInt(qs, "page", 1, v)
	input.Filters.PageSize = app.readInt(qs, "page_size", 10, v)

	input.Filters.Sort = app.readString(qs, "sort", "id")
	input.Filters.SortSafelist = []string{"id", "created_at", "-id", "-created_at"}

	data.ValidateFilters(v, input.Filters)

	return input, v.Errors
}

func (app *application) listQueriesApiHandler(w http.ResponseWriter, r *http.Request) {
	input, validationErrors := app.getListQueriesInput(r)
	if !reflect.DeepEqual(validationErrors, map[string]string{}) {
		app.failedValidationResponse(w, r, validationErrors)
	}

	queries, metadata, err := app.models.Queries.GetAll(input.Filters)
	if err != nil {
		app.serverErrorResponse(w, r, err)
		return
	}

	err = app.writeJSON(w, http.StatusOK, envelope{"queries": queries, "metadata": metadata}, nil)
	if err != nil {
		app.serverErrorResponse(w, r, err)
	}
}

func (app *application) showQueryApiHandler(w http.ResponseWriter, r *http.Request) {
	id, err := app.readIDParam(r)
	if err != nil {
		app.notFoundResponse(w, r)
		return
	}

	query, err := app.models.Queries.GetById(id)
	if err != nil {
		switch {
		case errors.Is(err, data.ErrRecordNotFound):
			app.notFoundResponse(w, r)
		default:
			app.serverErrorResponse(w, r, err)
		}
		return
	}

	err = app.writeJSON(w, http.StatusOK, envelope{"query": query}, nil)
	if err != nil {
		app.serverErrorResponse(w, r, err)
	}
}

func (app *application) cancelQueryApiHandler(w http.ResponseWriter, r *http.Request) {
	id, err := app.readIDParam(r)
	if err != nil {
		app.notFoundResponse(w, r)
		return
	}

	v := validator.New()
	query, err := app.models.Queries.GetById(id)
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

	if query.Status != "queued" && query.Status != "active" {
		v.AddError("status", fmt.Sprintf("cannot cancel a query with status of %s", query.Status))
		app.failedValidationResponse(w, r, v.Errors)
		return
	}

	query.Status = "cancelled"
	query.StoppedAt = time.Now()

	app.models.Queries.Update(query)
	if err != nil {
		switch {
		case errors.Is(err, data.ErrEditConflict):
			app.editConflictResponse(w, r)
		default:
			app.serverErrorResponse(w, r, err)
		}
		return
	}

	err = app.writeJSON(w, http.StatusOK, envelope{"query": query}, nil)
	if err != nil {
		app.serverErrorResponse(w, r, err)
	}
}

func (app *application) deleteQueryApiHandler(w http.ResponseWriter, r *http.Request) {
	id, err := app.readIDParam(r)
	if err != nil {
		app.notFoundResponse(w, r)
		return
	}

	err = app.models.Queries.Delete(id)
	if err != nil {
		switch {
		case errors.Is(err, data.ErrRecordNotFound):
			app.notFoundResponse(w, r)
		default:
			app.serverErrorResponse(w, r, err)
		}
		return
	}

	err = app.writeJSON(w, http.StatusOK, envelope{"message": "query successfully deleted"}, nil)
	if err != nil {
		app.serverErrorResponse(w, r, err)
	}
}

func (app *application) createQueryFormUiHandler(w http.ResponseWriter, r *http.Request) {
	input, validationErrors := app.getListQueriesInput(r)
	if !reflect.DeepEqual(validationErrors, map[string]string{}) {
		app.failedValidationResponse(w, r, validationErrors)
		return
	}

	connections, _, err := app.models.Connections.GetAll(input.Filters)
	if err != nil {
		app.serverErrorResponse(w, r, err)
		return
	}

	app.render(w, r, "create-query.page.tmpl", &templateData{Connections: connections, Form: forms.New(nil)})
}

func (app *application) listQueriesUiHandler(w http.ResponseWriter, r *http.Request) {
	input, validationErrors := app.getListQueriesInput(r)
	if !reflect.DeepEqual(validationErrors, map[string]string{}) {
		app.failedValidationResponse(w, r, validationErrors)
		return
	}

	queries, metadata, err := app.models.Queries.GetAll(input.Filters)
	if err != nil {
		app.serverErrorResponse(w, r, err)
		return
	}

	paginationData := getPaginationData(metadata.CurrentPage, int(metadata.TotalRecords), metadata.PageSize, "queries")
	app.render(w, r, "queries.page.tmpl", &templateData{Queries: queries, Metadata: metadata, PaginationData: &paginationData})
}

func (app *application) createQueryUiHandler(w http.ResponseWriter, r *http.Request) {
	err := r.ParseForm()
	if err != nil {
		app.errorResponse(w, r, http.StatusBadRequest, "unable to parse run query form")
		return
	}

	var connectionId int64 = 0
	if r.PostForm.Get("connectionId") != "" {
		connectionId, err = strconv.ParseInt(r.PostForm.Get("connectionId"), 10, 64)
		if err != nil {
			app.errorResponse(w, r, http.StatusBadRequest, "non int value given to connectionId")
			return
		}
	}

	query := &data.Query{
		ConnectionID: connectionId,
		Query:        r.PostForm.Get("query"),
	}

	form := forms.New(r.PostForm)

	input, _ := app.getListQueriesInput(r)
	connections, _, err := app.models.Connections.GetAll(input.Filters)
	if err != nil {
		app.serverErrorResponse(w, r, err)
		return
	}

	if data.ValidateQuery(form.Validator, query); !form.Validator.Valid() {
		app.render(w, r, "create-query.page.tmpl", &templateData{Connections: connections, Form: form})
		return
	}

	query, err = app.models.Queries.Insert(query)
	if err != nil {
		app.serverErrorResponse(w, r, err)
		return
	}

	app.session.Put(r, "flash", fmt.Sprintf("Query %d created", query.ID))
	http.Redirect(w, r, fmt.Sprintf("/ui/queries/%d", query.ID), http.StatusSeeOther)
}

func (app *application) showQueryUiHandler(w http.ResponseWriter, r *http.Request) {
	id, err := app.readIDParam(r)
	if err != nil || id < 1 {
		app.notFoundResponse(w, r)
		return
	}

	query, err := app.models.Queries.GetById(id)
	if err != nil {
		if errors.Is(err, data.ErrRecordNotFound) {
			app.notFoundResponse(w, r)
		} else {
			app.serverErrorResponse(w, r, err)
		}
		return
	}

	app.render(w, r, "query-detail.page.tmpl", &templateData{Query: query})
}

func (app *application) cancelQueryUiHandler(w http.ResponseWriter, r *http.Request) {
	id, err := app.readIDParam(r)
	if err != nil {
		app.notFoundResponse(w, r)
		return
	}

	v := validator.New()
	query, err := app.models.Queries.GetById(id)
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

	if query.Status != "queued" && query.Status != "active" {
		v.AddError("status", fmt.Sprintf("cannot cancel a query with status of %s", query.Status))
		app.failedValidationResponse(w, r, v.Errors)
		return
	}

	query.Status = "cancelled"
	query.StoppedAt = time.Now()

	app.models.Queries.Update(query)
	if err != nil {
		switch {
		case errors.Is(err, data.ErrEditConflict):
			app.editConflictResponse(w, r)
		default:
			app.serverErrorResponse(w, r, err)
		}
		return
	}

	app.session.Put(r, "flash", "Query cancelled")
	http.Redirect(w, r, fmt.Sprintf("/ui/queries/%d", id), http.StatusSeeOther)
}

func (app *application) deleteQueryUiHandler(w http.ResponseWriter, r *http.Request) {
	id, err := app.readIDParam(r)
	if err != nil {
		app.notFoundResponse(w, r)
		return
	}

	err = app.models.Queries.Delete(id)
	if err != nil {
		switch {
		case errors.Is(err, data.ErrRecordNotFound):
			app.notFoundResponse(w, r)
		default:
			app.serverErrorResponse(w, r, err)
		}
		return
	}

	app.session.Put(r, "flash", fmt.Sprintf("Query %d deleted", id))
	http.Redirect(w, r, "/ui/queries", http.StatusSeeOther)
}
