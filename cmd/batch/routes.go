package main

import (
	"expvar"
	"net/http"

	"github.com/julienschmidt/httprouter"
)

func routes() http.Handler {
	router := httprouter.New()

	// SQLpipe
	router.NotFound = http.HandlerFunc(notFoundResponse)
	router.MethodNotAllowed = http.HandlerFunc(methodNotAllowedResponse)

	router.HandlerFunc(http.MethodGet, "/healthcheck", healthcheckHandler)

	router.HandlerFunc(http.MethodPost, "/transfers/create", createTransferHandler)
	router.HandlerFunc(http.MethodGet, "/transfers/show/:id", showTransferHandler)
	router.HandlerFunc(http.MethodGet, "/transfers/list", listTransfersHandler)
	router.HandlerFunc(http.MethodPatch, "/transfers/cancel/:id", cancelTransferHandler)

	router.Handler(http.MethodGet, "/debug/vars", expvar.Handler())

	return recoverPanic(router)
}
