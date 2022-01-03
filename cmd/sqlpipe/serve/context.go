package serve

import (
	"context"
	"net/http"
)

type contextKey string

const adminContextKey = contextKey("admin")

func (app *application) contextSetAdmin(r *http.Request, admin bool) *http.Request {
	ctx := context.WithValue(r.Context(), adminContextKey, admin)
	return r.WithContext(ctx)
}

func (app *application) contextGetAdmin(r *http.Request) bool {
	admin, ok := r.Context().Value(adminContextKey).(bool)
	if !ok {
		panic("missing admin value in request context")
	}

	return admin
}
