package main

import (
	"expvar"
	"net/http"

	"github.com/julienschmidt/httprouter"
	"github.com/justinas/alice"
)

func (app *application) routes() http.Handler {

	commonMiddleware := alice.New(app.metrics, app.recoverPanic, app.logRequest, app.enableCORS, app.rateLimit)

	// uiStandardMiddleware := alice.New()
	// uiDynamicMiddleware := alice.New()

	// apiStandardMiddleware := alice.New()
	// apiDynamicMiddleware := alice.New()

	router := httprouter.New()

	router.NotFound = http.HandlerFunc(app.notFoundResponse)
	router.MethodNotAllowed = http.HandlerFunc(app.methodNotAllowedResponse)

	// **************
	// **Operations**
	// **************

	router.HandlerFunc(http.MethodGet, "/api/v1/healthcheck", app.healthcheckHandler)
	router.Handler(http.MethodGet, "/api/v1/debug/vars", expvar.Handler())

	// *************
	// **Transfers**
	// *************

	// UI
	// Create transfer post
	// Get all transfers get
	// Get one transfer get

	// API
	// Create transfer post
	// Get all transfers get
	// Get one transfer get

	// ***************
	// **Connections**
	// ***************

	// UI
	// Create Connection post
	// Get all Connections get
	// Get one Connection get
	// Update connection patch
	// Delete connection delete

	// API
	// Create Connection post
	// Get all Connections get
	// Get one Connection get
	// Update connection patch
	// Delete connection delete

	// ***********
	// **Queries**
	// ***********

	// UI
	// Create Query post
	// Get all Queries get
	// Get one Query get
	// Update Query patch
	// Delete Query delete

	// API
	// Create Query post
	// Get all Queries get
	// Get one Query get
	// Update Query patch
	// Delete Query delete

	// **********
	// **Static**
	// **********

	// UI
	// Embedded FS get

	// *********
	// **Users**
	// *********

	// UI
	// Create user get
	// Create user post
	// Login user get
	// Login user post
	// Logout user post
	// Change user password get
	// Change user password post

	// API
	// Create user post
	// Activate user put
	// Change user password put

	// **********
	// **Tokens**
	// **********

	// API
	// Create activation token post
	// Create authentication token post
	// Create password reset post

	return commonMiddleware.Then(router)
}
