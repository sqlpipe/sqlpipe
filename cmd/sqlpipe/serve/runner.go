package serve

import (
	"fmt"
	"time"

	"github.com/calmitchell617/sqlpipe/internal/data"
	"github.com/calmitchell617/sqlpipe/internal/engine"
)

func (app *application) toDoScanner() {
	// This loops forever, looking for transfer requests to fulfill in the db

	for {
		time.Sleep(time.Second)

		queuedTransfers, err := app.models.Transfers.GetQueued()
		if err != nil {
			app.logger.PrintError(fmt.Errorf("%s", err), nil)
		}

		for _, transfer := range queuedTransfers {
			if app.numLocalActiveQueries < 10 {
				app.background(engine.RunTransfer, *transfer)
			}
		}

		// for _, query := range queuedQueries {
		// 	if numLocalActiveQueries < 10 {
		// 		go attemptQuery(query)
		// 	}
		// }
	}
}

func (app *application) background(fn func(t *data.Transfer), transfer data.Transfer) {
	app.wg.Add(1)

	go func() {
		defer app.wg.Done()

		defer func() {
			if err := recover(); err != nil {
				app.logger.PrintError(fmt.Errorf("%s", err), nil)
			}
		}()

		fn(&transfer)
	}()
}
