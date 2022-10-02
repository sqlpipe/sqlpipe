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

	router.HandlerFunc(http.MethodGet, "/v1/healthcheck", app.healthcheckHandler)

	router.HandlerFunc(http.MethodPost, "/v1/query", app.authenticate(app.runQueryHandler))
	router.HandlerFunc(http.MethodPost, "/v1/transfer", app.authenticate(app.runTransferHandler))
	router.HandlerFunc(http.MethodPost, "/v1/csv/download", app.authenticate(app.runCsvDownloadHandler))
	router.HandlerFunc(http.MethodPost, "/v1/csv/save-on-server", app.authenticate(app.runCsvSaveOnServerHandler))
	// router.HandlerFunc(http.MethodPost, "/v1/csv/s3", app.authenticate(app.runS3ExportHandler))

	router.Handler(http.MethodGet, "/debug/vars", expvar.Handler())

	return app.metrics(app.recoverPanic(app.rateLimit(router)))
}
