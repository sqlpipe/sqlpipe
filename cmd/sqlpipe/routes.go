package main

import (
	"expvar"
	"net/http"

	"github.com/julienschmidt/httprouter"
	"github.com/justinas/alice"
)

func (app *application) routes() http.Handler {
	router := httprouter.New()

	commonMiddleware := alice.New(app.metrics, app.recoverPanic, app.enableCORS, app.logRequest, app.rateLimit, app.getUserFromRequest)

	router.NotFound = http.HandlerFunc(app.notFoundResponse)
	router.MethodNotAllowed = http.HandlerFunc(app.methodNotAllowedResponse)

	router.HandlerFunc(http.MethodGet, "/v2/healthcheck", app.healthcheckHandler)

	router.HandlerFunc(http.MethodPost, "/v2/users", app.createUserHandler)
	router.HandlerFunc(http.MethodGet, "/v2/users/:username", app.showUserHandler)
	router.HandlerFunc(http.MethodGet, "/v2/users", app.listUsersHandler)
	router.HandlerFunc(http.MethodPatch, "/v2/users/:username", app.updateUserHandler)
	router.HandlerFunc(http.MethodDelete, "/v2/users/:username", app.deleteUserHandler)

	router.HandlerFunc(http.MethodPost, "/v2/tokens/authenticate", app.createAuthenticationTokenHandler)

	router.Handler(http.MethodGet, "/v2/debug/vars", expvar.Handler())

	return commonMiddleware.Then(router)
}
