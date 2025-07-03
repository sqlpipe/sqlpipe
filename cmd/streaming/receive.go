package main

import "net/http"

func (app *application) receiveHandler(w http.ResponseWriter, r *http.Request) {

	// get the route from the request
	route := r.URL.Path
	handlerFunc := app.receiveHandlers[route]
	if handlerFunc == nil {
		app.logger.Error("receiveHandler: no handler found for route", "route", route)
		return
	}

	handlerFunc(w, r)
}
