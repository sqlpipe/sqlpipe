package main

import (
	"context"
	"net/http"

	"github.com/sqlpipe/sqlpipe/internal/data"
)

type contextKey string

const userContextKey = contextKey("user")

func (app *application) contextSetUser(r *http.Request, scrubbedUser data.ScrubbedUser) *http.Request {
	ctx := context.WithValue(r.Context(), userContextKey, scrubbedUser)
	return r.WithContext(ctx)
}

func (app *application) contextGetUser(r *http.Request) data.ScrubbedUser {
	user, ok := r.Context().Value(userContextKey).(data.ScrubbedUser)
	if !ok {
		panic("missing user value in request context")
	}

	return user
}
