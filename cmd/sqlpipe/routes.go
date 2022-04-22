package main

import (
	"expvar"
	"net/http"

	"github.com/julienschmidt/httprouter"
	"github.com/justinas/alice"
)

func (app *application) routes() http.Handler {
	router := httprouter.New()

	commonMiddleware := alice.New(app.metrics, app.recoverPanic, app.enableCORS, app.logRequest, app.rateLimit, app.authenticate)

	requireAuthenticatedUser := alice.New(app.requireAuthenticatedUser)
	requireAdmin := requireAuthenticatedUser.Append(app.requireAdmin)

	router.NotFound = http.HandlerFunc(app.notFoundResponse)
	router.MethodNotAllowed = http.HandlerFunc(app.methodNotAllowedResponse)

	router.HandlerFunc(http.MethodGet, "/v2/healthcheck", app.healthcheckHandler)

	router.Handler(http.MethodPost, "/v2/users", requireAdmin.ThenFunc(app.createUserHandler))
	router.Handler(http.MethodGet, "/v2/users/:username", requireAdmin.ThenFunc(app.showUserHandler))
	router.Handler(http.MethodGet, "/v2/users", requireAdmin.ThenFunc(app.listUsersHandler))
	router.Handler(http.MethodPatch, "/v2/users/:username", requireAdmin.ThenFunc(app.updateUserHandler))
	router.Handler(http.MethodDelete, "/v2/users/:username", requireAdmin.ThenFunc(app.deleteUserHandler))

	router.HandlerFunc(http.MethodPost, "/v2/tokens/authentication", app.createAuthenticationTokenHandler)

	router.Handler(http.MethodGet, "/v2/debug/vars", expvar.Handler())

	return commonMiddleware.Then(router)
}
