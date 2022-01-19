package serve

import (
	"fmt"
	"sync"
	"time"

	"github.com/calmitchell617/sqlpipe/internal/engine"
	"github.com/calmitchell617/sqlpipe/pkg"
)

func (app *application) toDoScanner() {
	// This loops forever, looking for transfer requests to fulfill in the db
	var wg sync.WaitGroup

	for {
		wg.Wait()
		time.Sleep(time.Second)

		queuedTransfers, err := app.models.Transfers.GetQueued()
		if err != nil {
			app.logger.PrintError(fmt.Errorf("%s", err), nil)
		}

		for _, transfer := range queuedTransfers {
			if app.numLocalActiveQueries < 10 {
				wg.Add(1)
				pkg.Background(func() {
					defer wg.Done()
					transfer.Status = "active"
					err = app.models.Transfers.Update(transfer)
					if err != nil {
						app.logger.PrintError(fmt.Errorf("%s", err), nil)
					}
					err = engine.RunTransfer(transfer)
					if err != nil {
						transfer.Status = "error"
						transfer.Error = err.Error()
						transfer.StoppedAt = time.Now()
						err = app.models.Transfers.Update(transfer)
						return
					}

					transfer.Status = "complete"
					transfer.StoppedAt = time.Now()
					err = app.models.Transfers.Update(transfer)
				})
			}
		}

		// for _, query := range queuedQueries {
		// 	if numLocalActiveQueries < 10 {
		// 		go attemptQuery(query)
		// 	}
		// }
	}
}
