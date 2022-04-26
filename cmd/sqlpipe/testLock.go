package main

import (
	"context"
	"net/http"

	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/sqlpipe/sqlpipe/internal/globals"
)

func (app *application) testLock(w http.ResponseWriter, r *http.Request) {
	session, err := concurrency.NewSession(app.models.Tokens.Etcd)
	if err != nil {
		app.serverErrorResponse(w, r, err)
		return
	}
	defer session.Close()

	ctx, cancel := context.WithTimeout(context.Background(), globals.EtcdTimeout)
	defer cancel()

	mutex := concurrency.NewMutex(session, "sqlpipe/test")

	if err = mutex.Lock(ctx); err != nil {
		app.serverErrorResponse(w, r, err)
		return
	}
	defer mutex.Unlock(ctx)

	_, err = app.models.Tokens.Etcd.Put(
		ctx,
		"sqlpipe/test2/token/1",
		"Helloooo",
	)

	if err = app.writeJSON(w, http.StatusCreated, envelope{"lock": "hopefully unlocked"}, nil); err != nil {
		app.serverErrorResponse(w, r, err)
		return
	}
}
