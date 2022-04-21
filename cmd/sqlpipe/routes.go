package main

import (
	"expvar"
	"net/http"

	"github.com/julienschmidt/httprouter"
	"github.com/justinas/alice"
)

func (app *application) routes() http.Handler {
	router := httprouter.New()

	commonMiddleware := alice.New(app.metrics, app.recoverPanic, app.logRequest, app.rateLimit, app.authenticate, app.requireAuthenticatedUser)
	requireAdmin := alice.New(app.requireAdmin)

	router.NotFound = http.HandlerFunc(app.notFoundResponse)
	router.MethodNotAllowed = http.HandlerFunc(app.methodNotAllowedResponse)

	router.HandlerFunc(http.MethodGet, "/v2/healthcheck", app.healthcheckHandler)

	// router.HandlerFunc(http.MethodGet, "/v2/movies", app.requirePermission("movies:read", app.listMoviesHandler))
	// router.HandlerFunc(http.MethodPost, "/v2/movies", app.requirePermission("movies:write", app.createMovieHandler))
	// router.HandlerFunc(http.MethodGet, "/v2/movies/:id", app.requirePermission("movies:read", app.showMovieHandler))
	// router.HandlerFunc(http.MethodPatch, "/v2/movies/:id", app.requirePermission("movies:write", app.updateMovieHandler))
	// router.HandlerFunc(http.MethodDelete, "/v2/movies/:id", app.requirePermission("movies:write", app.deleteMovieHandler))

	router.Handler(http.MethodPost, "/v2/users", requireAdmin.ThenFunc(app.createUserHandler))
	router.Handler(http.MethodGet, "/v2/users/:username", requireAdmin.ThenFunc(app.showUserHandler))
	router.Handler(http.MethodGet, "/v2/users", requireAdmin.ThenFunc(app.listUsersHandler))
	router.Handler(http.MethodPatch, "/v2/users/:username", requireAdmin.ThenFunc(app.updateUserHandler))
	router.Handler(http.MethodDelete, "/v2/users/:username", requireAdmin.ThenFunc(app.deleteUserHandler))
	// router.HandlerFunc(http.MethodPut, "/v2/users/activated", app.activateUserHandler)
	// router.HandlerFunc(http.MethodPut, "/v2/users/password", app.updateUserPasswordHandler)

	// router.HandlerFunc(http.MethodPost, "/v2/tokens/authentication", app.createAuthenticationTokenHandler)
	// router.HandlerFunc(http.MethodPost, "/v2/tokens/activation", app.createActivationTokenHandler)
	// router.HandlerFunc(http.MethodPost, "/v2/tokens/password-reset", app.createPasswordResetTokenHandler)

	router.Handler(http.MethodGet, "/v2/debug/vars", expvar.Handler())

	return commonMiddleware.Then(router)
}
