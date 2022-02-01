package serve

import (
	"expvar"
	"net/http"

	"github.com/calmitchell617/sqlpipe/ui"
	"github.com/julienschmidt/httprouter"
	"github.com/justinas/alice"
)

func (app *application) routes() http.Handler {
	router := httprouter.New()

	// Middleware
	commonMiddleware := alice.New(app.metrics, app.recoverPanic, app.logRequest, app.rateLimit)

	apiRequireLoggedInUser := alice.New(app.authenticateApi, app.requireAuthApi)
	apiRequireAdmin := apiRequireLoggedInUser.Append(app.requireAdminApi)

	uiStandardMiddleware := alice.New(secureHeaders, app.session.Enable, noSurf, app.authenticateUi)
	uiRequireLoggedInUser := uiStandardMiddleware.Append(app.requireAuthUi)
	uiRequireAdmin := uiRequireLoggedInUser.Append(app.requireAdminUi)

	// Home page ui redirects
	router.Handler(http.MethodGet, "/", uiRequireLoggedInUser.ThenFunc(app.homeReRoute))
	router.Handler(http.MethodGet, "/ui", uiRequireLoggedInUser.ThenFunc(app.homeReRoute))

	// Users
	// API
	router.Handler(http.MethodPost, "/api/v1/users", apiRequireAdmin.ThenFunc(app.createUserApiHandler))
	router.Handler(http.MethodGet, "/api/v1/users", apiRequireAdmin.ThenFunc(app.listUsersApiHandler))
	router.Handler(http.MethodGet, "/api/v1/users/:id", apiRequireAdmin.ThenFunc(app.showUserApiHandler))
	router.Handler(http.MethodPatch, "/api/v1/users/:id", apiRequireAdmin.ThenFunc(app.updateUserApiHandler))
	router.Handler(http.MethodDelete, "/api/v1/users/:id", apiRequireAdmin.ThenFunc(app.deleteUserApiHandler))
	// UI
	router.Handler(http.MethodGet, "/ui/create-user", uiRequireAdmin.ThenFunc(app.createUserFormUiHandler))
	router.Handler(http.MethodPost, "/ui/create-user", uiRequireAdmin.ThenFunc(app.createUserUiHandler))
	router.Handler(http.MethodGet, "/ui/users", uiRequireAdmin.ThenFunc(app.listUsersUiHandler))
	router.Handler(http.MethodGet, "/ui/users/:id", uiRequireAdmin.ThenFunc(app.showUserUiHandler))
	router.Handler(http.MethodGet, "/ui/update-user/:id", uiRequireAdmin.ThenFunc(app.updateUserFormUiHandler))
	router.Handler(http.MethodPost, "/ui/update-user/:id", uiRequireAdmin.ThenFunc(app.updateUserUiHandler))
	router.Handler(http.MethodPost, "/ui/delete-user/:id", uiRequireAdmin.ThenFunc(app.deleteUserUiHandler))
	router.Handler(http.MethodGet, "/ui/login", uiStandardMiddleware.ThenFunc(app.loginUserFormUiHandler))
	router.Handler(http.MethodPost, "/ui/login", uiStandardMiddleware.ThenFunc(app.loginUserUiHandler))
	router.Handler(http.MethodGet, "/ui/logout", uiRequireLoggedInUser.ThenFunc(app.logoutUserUiHandler))

	// Connections
	// API
	router.Handler(http.MethodPost, "/api/v1/connections", apiRequireAdmin.ThenFunc(app.createConnectionApiHandler))
	router.Handler(http.MethodGet, "/api/v1/connections", apiRequireAdmin.ThenFunc(app.listConnectionsApiHandler))
	router.Handler(http.MethodGet, "/api/v1/connections/:id", apiRequireAdmin.ThenFunc(app.showConnectionApiHandler))
	router.Handler(http.MethodPatch, "/api/v1/connections/:id", apiRequireAdmin.ThenFunc(app.updateConnectionApiHandler))
	router.Handler(http.MethodDelete, "/api/v1/connections/:id", apiRequireAdmin.ThenFunc(app.deleteConnectionApiHandler))
	// UI
	router.Handler(http.MethodGet, "/ui/create-connection", uiRequireAdmin.ThenFunc(app.createConnectionFormUiHandler))
	router.Handler(http.MethodPost, "/ui/create-connection", uiRequireAdmin.ThenFunc(app.createConnectionUiHandler))
	router.Handler(http.MethodGet, "/ui/connections", uiRequireAdmin.ThenFunc(app.listConnectionsUiHandler))
	router.Handler(http.MethodGet, "/ui/connections/:id", uiRequireAdmin.ThenFunc(app.showConnectionUiHandler))
	router.Handler(http.MethodGet, "/ui/update-connection/:id", uiRequireAdmin.ThenFunc(app.updateConnectionFormUiHandler))
	router.Handler(http.MethodPost, "/ui/update-connection/:id", uiRequireAdmin.ThenFunc(app.updateConnectionUiHandler))
	router.Handler(http.MethodPost, "/ui/delete-connection/:id", uiRequireAdmin.ThenFunc(app.deleteConnectionUiHandler))

	// Transfers
	// API
	router.Handler(http.MethodPost, "/api/v1/transfers", apiRequireLoggedInUser.ThenFunc(app.createTransferApiHandler))
	router.Handler(http.MethodGet, "/api/v1/transfers", apiRequireLoggedInUser.ThenFunc(app.listTransfersApiHandler))
	router.Handler(http.MethodGet, "/api/v1/transfers/:id", apiRequireLoggedInUser.ThenFunc(app.showTransferApiHandler))
	router.Handler(http.MethodPatch, "/api/v1/cancel-transfer/:id", apiRequireLoggedInUser.ThenFunc(app.cancelTransferApiHandler))
	router.Handler(http.MethodDelete, "/api/v1/transfers/:id", apiRequireAdmin.ThenFunc(app.deleteTransferApiHandler))
	// UI
	router.Handler(http.MethodGet, "/ui/create-transfer", uiRequireLoggedInUser.ThenFunc(app.createTransferFormUiHandler))
	router.Handler(http.MethodPost, "/ui/create-transfer", uiRequireLoggedInUser.ThenFunc(app.createTransferUiHandler))
	router.Handler(http.MethodGet, "/ui/transfers", uiRequireLoggedInUser.ThenFunc(app.listTransfersUiHandler))
	router.Handler(http.MethodGet, "/ui/transfers/:id", uiRequireLoggedInUser.ThenFunc(app.showTransferUiHandler))
	router.Handler(http.MethodPost, "/ui/cancel-transfer/:id", uiRequireLoggedInUser.ThenFunc(app.cancelTransferUiHandler))
	router.Handler(http.MethodPost, "/ui/delete-transfer/:id", uiRequireAdmin.ThenFunc(app.deleteTransferUiHandler))

	// Queries
	// API
	router.Handler(http.MethodPost, "/api/v1/queries", apiRequireLoggedInUser.ThenFunc(app.createQueryApiHandler))
	router.Handler(http.MethodGet, "/api/v1/queries", apiRequireLoggedInUser.ThenFunc(app.listQueriesApiHandler))
	router.Handler(http.MethodGet, "/api/v1/queries/:id", apiRequireLoggedInUser.ThenFunc(app.showQueryApiHandler))
	router.Handler(http.MethodPatch, "/api/v1/cancel-query/:id", apiRequireLoggedInUser.ThenFunc(app.cancelQueryApiHandler))
	router.Handler(http.MethodDelete, "/api/v1/queries/:id", apiRequireAdmin.ThenFunc(app.deleteQueryApiHandler))
	// UI
	router.Handler(http.MethodGet, "/ui/create-query", uiRequireLoggedInUser.ThenFunc(app.createQueryFormUiHandler))
	router.Handler(http.MethodPost, "/ui/create-query", uiRequireLoggedInUser.ThenFunc(app.createQueryUiHandler))
	router.Handler(http.MethodGet, "/ui/queries", uiRequireLoggedInUser.ThenFunc(app.listQueriesUiHandler))
	router.Handler(http.MethodGet, "/ui/queries/:id", uiRequireLoggedInUser.ThenFunc(app.showQueryUiHandler))
	router.Handler(http.MethodPost, "/ui/cancel-query/:id", uiRequireLoggedInUser.ThenFunc(app.cancelQueryUiHandler))
	router.Handler(http.MethodPost, "/ui/delete-query/:id", uiRequireAdmin.ThenFunc(app.deleteQueryUiHandler))

	// Operations stuff
	router.HandlerFunc(http.MethodGet, "/api/v1/healthcheck", app.healthcheckHandler)
	router.Handler(http.MethodGet, "/api/v1/debug/vars", expvar.Handler())

	// Embedded file server
	router.NotFound = http.FileServer(http.FS(ui.Files))

	// Method not allowed
	router.MethodNotAllowed = http.HandlerFunc(app.methodNotAllowedResponse)

	return commonMiddleware.Then(router)
}
