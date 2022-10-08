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

	router.HandlerFunc(http.MethodGet, "/v2/healthcheck", app.healthcheckHandler)

	router.HandlerFunc(http.MethodPost, "/v2/query", app.authenticate(app.runQueryHandler))
	router.HandlerFunc(http.MethodPost, "/v2/transfer", app.authenticate(app.runTransferHandler))
	router.HandlerFunc(http.MethodPost, "/v2/csv/download", app.authenticate(app.runCsvDownloadHandler))
	router.HandlerFunc(http.MethodPost, "/v2/csv/s3", app.authenticate(app.runCsvS3UploadHandler))
	router.HandlerFunc(http.MethodPost, "/v2/csv/save", app.authenticate(app.runCsvSaveOnServerHandler))

	router.Handler(http.MethodGet, "/debug/vars", expvar.Handler())

	return app.metrics(app.recoverPanic(app.rateLimit(router)))
}
