package serve

import (
	"errors"
	"fmt"
	"time"

	"github.com/calmitchell617/sqlpipe/internal/engine"
	"github.com/calmitchell617/sqlpipe/internal/globals"
	"github.com/calmitchell617/sqlpipe/pkg"
)

var numLocalActiveTransfers int

func (app *application) toDoScanner() {
	// This loops forever, looking for transfer requests to fulfill in the db

	for {
		time.Sleep(time.Second * 1)

		queuedTransfers, err := app.models.Transfers.GetQueued()
		if err != nil {
			app.logger.PrintError(err, nil)
		}

		queuedQueries, err := app.models.Queries.GetQueued()
		if err != nil {
			app.logger.PrintError(err, nil)
		}

		for i := 0; i < len(queuedTransfers); i++ {
			if numLocalActiveTransfers < maxConcurrentTransfers {
				numLocalActiveTransfers += 1
				transfer := queuedTransfers[i]
				pkg.Background(func() {
					transfer.Status = "active"
					err = app.models.Transfers.Update(transfer)
					if err != nil {
						transfer.Status = "error"
						transfer.Error = "unable to mark transfer as active"
						errProperties := map[string]string{
							"error:":   err.Error(),
							"transfer": fmt.Sprintf("%+v", transfer),
						}
						transfer.ErrorProperties = fmt.Sprintf("%+v", errProperties)
						transfer.StoppedAt = time.Now()

						app.logger.PrintError(
							errors.New(transfer.Error),
							errProperties,
						)
						numLocalActiveTransfers -= 1
						errProperties, err = globals.SendAnonymizedTransferAnalytics(*transfer, true)
						if err != nil {
							app.logger.PrintError(err, errProperties)
						}
						return
					}
					app.logger.PrintInfo(
						"now running a transfer",
						map[string]string{
							"ID":           fmt.Sprint(transfer.ID),
							"CreatedAt":    globals.HumanDate(transfer.CreatedAt),
							"SourceID":     fmt.Sprint(transfer.SourceID),
							"TargetID":     fmt.Sprint(transfer.TargetID),
							"Query":        transfer.Query,
							"TargetSchema": transfer.TargetSchema,
							"TargetTable":  transfer.TargetTable,
							"Overwrite":    fmt.Sprint(transfer.Overwrite),
							"Status":       transfer.Status,
						},
					)
					errProperties, err := engine.RunTransfer(transfer)
					if err != nil {
						app.logger.PrintError(err, errProperties)
						transfer.Status = "error"
						transfer.Error = err.Error()
						transfer.ErrorProperties = fmt.Sprint(errProperties)
						transfer.StoppedAt = time.Now()

						err = app.models.Transfers.Update(transfer)
						if err != nil {
							errProperties := map[string]string{
								"error:":   err.Error(),
								"transfer": fmt.Sprintf("%+v", transfer),
							}
							app.logger.PrintError(
								errors.New("unable to update transfer"),
								errProperties,
							)
						}
						numLocalActiveTransfers -= 1
						errProperties, err = globals.SendAnonymizedTransferAnalytics(*transfer, true)
						if err != nil {
							app.logger.PrintError(err, errProperties)
						}
						return
					}

					transfer.Status = "complete"
					transfer.StoppedAt = time.Now()
					err = app.models.Transfers.Update(transfer)
					if err != nil {
						errProperties := map[string]string{
							"error:":   err.Error(),
							"transfer": fmt.Sprintf("%+v", transfer),
						}
						app.logger.PrintError(
							errors.New("unable to update transfer"),
							errProperties,
						)
					}
					numLocalActiveTransfers -= 1
					errProperties, err = globals.SendAnonymizedTransferAnalytics(*transfer, true)
					if err != nil {
						app.logger.PrintError(err, errProperties)
					}
				})
			}
		}

		for i := 0; i < len(queuedQueries); i++ {
			query := queuedQueries[i]
			pkg.Background(func() {
				query.Status = "active"
				err = app.models.Queries.Update(query)
				if err != nil {
					query.Status = "error"
					query.Error = "unable to mark query as active"
					errProperties := map[string]string{
						"error:": err.Error(),
						"query":  fmt.Sprintf("%+v", query),
					}
					query.ErrorProperties = fmt.Sprintf("%+v", errProperties)

					app.logger.PrintError(
						errors.New(query.Error),
						errProperties,
					)
					errProperties, err = globals.SendAnonymizedQueryAnalytics(*query, true)
					if err != nil {
						app.logger.PrintError(err, errProperties)
					}
					return
				}
				app.logger.PrintInfo(
					"now running a query",
					map[string]string{
						"ID":           fmt.Sprint(query.ID),
						"CreatedAt":    globals.HumanDate(query.CreatedAt),
						"ConnectionID": fmt.Sprint(query.ConnectionID),
						"Query":        query.Query,
						"Status":       query.Status,
					},
				)
				errProperties, err := engine.RunQuery(query)
				if err != nil {
					app.logger.PrintError(err, errProperties)
					query.Status = "error"
					query.Error = err.Error()
					query.ErrorProperties = fmt.Sprint(errProperties)
					query.StoppedAt = time.Now()
					err = app.models.Queries.Update(query)
					if err != nil {
						errProperties := map[string]string{
							"error:": err.Error(),
							"query":  fmt.Sprintf("%+v", query),
						}
						app.logger.PrintError(
							errors.New("unable to update query"),
							errProperties,
						)
					}
					errProperties, err = globals.SendAnonymizedQueryAnalytics(*query, true)
					if err != nil {
						app.logger.PrintError(err, errProperties)
					}
					return
				}

				query.Status = "complete"
				query.StoppedAt = time.Now()
				err = app.models.Queries.Update(query)
				if err != nil {
					errProperties := map[string]string{
						"error:": err.Error(),
						"query":  fmt.Sprintf("%+v", query),
					}
					app.logger.PrintError(
						errors.New("unable to update query"),
						errProperties,
					)
				}
				errProperties, err = globals.SendAnonymizedQueryAnalytics(*query, true)
				if err != nil {
					app.logger.PrintError(err, errProperties)
				}
			})
		}
	}
}
