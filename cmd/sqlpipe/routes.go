package main

import (
	"expvar"
	"net/http"

	"github.com/julienschmidt/httprouter"
)

func (app *application) routes() http.Handler {
	router := httprouter.New()

	router.NotFound = http.HandlerFunc(app.notFoundResponse)
	router.MethodNotAllowed = http.HandlerFunc(app.methodNotAllowedResponse)

	router.HandlerFunc(http.MethodGet, "/api/v3/healthcheck", app.healthcheckHandler)

	router.HandlerFunc(http.MethodPost, "/api/v3/transfers/create", app.createTransferHandler)
	router.HandlerFunc(http.MethodGet, "/api/v3/transfers/show/:id", app.showTransferHandler)
	router.HandlerFunc(http.MethodGet, "/api/v3/transfers/list", app.listTransfersHandler)
	router.HandlerFunc(http.MethodPatch, "/api/v3/transfers/cancel/:id", app.cancelTransferHandler)

	router.Handler(http.MethodGet, "/debug/vars", expvar.Handler())

	return app.metrics(app.recoverPanic(router))
}
