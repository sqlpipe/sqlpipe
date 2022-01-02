package serve

import (
	"expvar"
	"net/http"

	"github.com/julienschmidt/httprouter"
	"github.com/justinas/alice"
)

func (app *application) routes() http.Handler {

	commonMiddleware := alice.New(app.metrics, app.recoverPanic, app.logRequest, app.rateLimit)

	router := httprouter.New()

	router.Handler(http.MethodGet, "/api/v1/users", http.HandlerFunc(app.listUsersApiHandler))

	router.NotFound = http.HandlerFunc(app.notFoundResponse)
	router.MethodNotAllowed = http.HandlerFunc(app.methodNotAllowedResponse)

	router.HandlerFunc(http.MethodGet, "/api/v1/healthcheck", app.healthcheckHandler)
	router.Handler(http.MethodGet, "/api/v1/debug/vars", expvar.Handler())

	return commonMiddleware.Then(router)
}
