package serve

import (
	"net/http"
	"reflect"

	"github.com/calmitchell617/sqlpipe/internal/data"
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

	connections, metadata, err := app.models.Users.GetAll(input.Username, input.Filters)
	if err != nil {
		app.serverErrorResponse(w, r, err)
		return
	}

	app.render(w, r, "users.page.tmpl", &templateData{Users: users, Metadata: metadata})
}
