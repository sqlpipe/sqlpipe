package main

import (
	"log"
	"net/http"
)

type application struct {
	errorLog *log.Logger
	infoLog  *log.Logger
}

func main() {
	mux := http.NewServeMux()
	mux.HandleFunc("/", home)
	mux.HandleFunc("/connections", showConnectionsListUi)
	mux.HandleFunc("/connections/detail", showConnectionDetailUi)

	fileServer := http.FileServer(neuteredFileSystem{http.Dir("./ui/static")})
	mux.Handle("/static", http.NotFoundHandler())
	mux.Handle("/static/", http.StripPrefix("/static", fileServer))

	// fileServer := http.FileServer(http.Dir("./ui/static/"))
	// mux.Handle("/static/", http.StripPrefix("/static", fileServer))

	log.Println("Starting server on port 9000")
	err := http.ListenAndServe(":9000", mux)
	log.Fatal(err)
}
