package main

import (
	"net/http"

	"github.com/sqlpipe/sqlpipe/internal/data"
)

type contextKey string

const userContextKey = contextKey("user")

func (app *application) contextGetUser(r *http.Request) *data.User {
	user, ok := r.Context().Value(userContextKey).(*data.User)
	if !ok {
		panic("missing user value in request context")
	}

	return user
}
