package server

import (
	"context"
	"net/http"

	"github.com/calmitchell617/sqlpipe/internal/models/postgresql"
)

type contextKey string

const userContextKey = contextKey("user")

func (app *application) contextSetUser(r *http.Request, user *postgresql.User) *http.Request {
	ctx := context.WithValue(r.Context(), userContextKey, user)
	return r.WithContext(ctx)
}

func (app *application) contextGetUser(r *http.Request) *postgresql.User {
	user, ok := r.Context().Value(userContextKey).(*postgresql.User)
	if !ok {
		panic("missing user value in request context")
	}

	return user
}
