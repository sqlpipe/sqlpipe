package main

import (
	"fmt"
	"net/http"
)

func (app *application) stripeListenerHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Println("HEEEERE")
}
