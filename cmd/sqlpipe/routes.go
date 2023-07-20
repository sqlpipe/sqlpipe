package main

import (
	"expvar"
	"net/http"

	"github.com/julienschmidt/httprouter"
)

func routes() http.Handler {
	router := httprouter.New()

	router.NotFound = http.HandlerFunc(notFoundResponse)
	router.MethodNotAllowed = http.HandlerFunc(methodNotAllowedResponse)

	router.HandlerFunc(http.MethodGet, "/api/v3/healthcheck", healthcheckHandler)

	router.HandlerFunc(http.MethodPost, "/api/v3/transfers/create", createTransferHandler)
	router.HandlerFunc(http.MethodGet, "/api/v3/transfers/show/:id", showTransferHandler)
	router.HandlerFunc(http.MethodGet, "/api/v3/transfers/list", listTransfersHandler)
	router.HandlerFunc(http.MethodPatch, "/api/v3/transfers/cancel/:id", cancelTransferHandler)

	router.Handler(http.MethodGet, "/debug/vars", expvar.Handler())

	return recoverPanic(router)
}
