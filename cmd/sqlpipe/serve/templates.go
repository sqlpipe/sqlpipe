package serve

import (
	"html/template"
	"io/fs"
	"path/filepath"
	"time"

	"github.com/calmitchell617/sqlpipe/internal/data"
	"github.com/calmitchell617/sqlpipe/internal/forms.go"
	"github.com/calmitchell617/sqlpipe/ui"
)

type templateData struct {
	CSRFToken       string
	User            *data.User
	Users           []*data.User
	Connection      *data.Connection
	Connections     []*data.Connection
	Transfer        *data.Transfer
	Transfers       []*data.Transfer
	Query           *data.Query
	Queries         []*data.Query
	Metadata        data.Metadata
	Form            *forms.Form
	PaginationData  *PaginationData
	IsAuthenticated bool
	IsAdmin         bool
	Flash           string
	ErrorMessage    string
	// Snippet         *models.Snippet
	// Snippets        []*models.Snippet
}

func newTemplateCache() (map[string]*template.Template, error) {
	cache := map[string]*template.Template{}

	pages, err := fs.Glob(ui.Files, "html/*/*.page.tmpl")
	if err != nil {
		return nil, err
	}

	for _, page := range pages {
		name := filepath.Base(page)
		ts, err := template.New(name).Funcs(functions).ParseFS(ui.Files, page)
		if err != nil {
			return nil, err
		}

		ts, err = ts.ParseFS(ui.Files, "html/*.layout.tmpl")
		if err != nil {
			return nil, err
		}

		ts, err = ts.ParseFS(ui.Files, "html/*.partial.tmpl")
		if err != nil {
			return nil, err
		}

		cache[name] = ts
	}

	return cache, nil
}

func humanDate(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format("2 Jan 2006 15:04 UTC")
}

var functions = template.FuncMap{
	"humanDate": humanDate,
}
