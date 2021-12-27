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
	// Create transfer get
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
	// Create Connection get
	// Create Connection post
	// Get all Connections get
	// Get one Connection get
	// Update connection get
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
	// Create Query get
	// Create Query post
	// Get all Queries get
	// Get one Query get

	// API
	// Create Query post
	// Get all Queries get
	// Get one Query get

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
	// Get all users get
	// Get one user get
	// Update user get
	// Update user patch
	// Delete user delete
	// Login user get
	// Login user post
	// Logout user post

	// API
	// Create user post
	// Get all users get
	// Get one user get
	// Update user patch
	// Delete user delete
	// Activate user patch

	// **********
	// **Tokens**
	// **********

	// UI
	// Create user password reset token get
	// Activate user with token get

	// API
	// Create user activation token post
	// Create user authentication token post
	// Create user password reset token post

	return commonMiddleware.Then(router)
}
