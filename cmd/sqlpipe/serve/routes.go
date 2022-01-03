package serve

import (
	"expvar"
	"net/http"

	"github.com/julienschmidt/httprouter"
	"github.com/justinas/alice"
)

func (app *application) routes() http.Handler {

	commonMiddleware := alice.New(app.metrics, app.recoverPanic, app.logRequest, app.rateLimit)
	apiRequireAuth := alice.New(app.basicAuth)
	apiRequireAdmin := apiRequireAuth.Append(app.requireAdmin)

	router := httprouter.New()

	router.Handler(http.MethodPost, "/api/v1/users", apiRequireAdmin.ThenFunc(app.createUserApiHandler))
	router.Handler(http.MethodGet, "/api/v1/users", apiRequireAdmin.ThenFunc(app.listUsersApiHandler))
	router.Handler(http.MethodGet, "/api/v1/users/:id", apiRequireAdmin.ThenFunc(app.showUserApiHandler))
	router.Handler(http.MethodPut, "/api/v1/users", apiRequireAdmin.ThenFunc(app.updateUserApiHandler))
	router.Handler(http.MethodDelete, "/api/v1/users/:id", apiRequireAdmin.ThenFunc(app.deleteUserApiHandler))

	router.NotFound = http.HandlerFunc(app.notFoundResponse)
	router.MethodNotAllowed = http.HandlerFunc(app.methodNotAllowedResponse)

	router.HandlerFunc(http.MethodGet, "/api/v1/healthcheck", app.healthcheckHandler)
	router.Handler(http.MethodGet, "/api/v1/debug/vars", expvar.Handler())

	return commonMiddleware.Then(router)
}
