package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"golang.org/x/sync/errgroup"
)

type ColumnInfo struct {
	name       string
	pipeType   string
	scanType   string
	decimalOk  bool
	precision  int64
	scale      int64
	lengthOk   bool
	length     int64
	nullableOk bool
	nullable   bool
}

func handleStops(cancel context.CancelFunc, transfer Transfer) {
	for {
		select {
		case ip := <-transfer.CancelChannel:
			transfer.Status = StatusCancelled
			infoLog.Printf("transfer %v cancelled by ip %v", transfer.Id, ip)
		case err := <-transfer.ErrorChannel:
			transfer.Err = err.Error()
			transfer.Status = StatusError
			errorLog.Printf("error running transfer %v :: %v", transfer.Id, err)
		case <-transfer.CompleteChannel:
			transfer.Status = StatusComplete
			infoLog.Printf("transfer %v complete", transfer.Id)
		}
		cancel()
		transfer.StoppedAt = fmt.Sprint(time.Now())
		transferMap.Set(transfer.Id, transfer)
		return
	}
}

func runTransfer(transfer Transfer) {
	defer func() {
		if !transfer.KeepFiles {
			os.RemoveAll(transfer.TmpDir)
			infoLog.Printf("temp dir %v removed", transfer.TmpDir)
		}
		close(transfer.CancelChannel)
		close(transfer.ErrorChannel)
		close(transfer.CompleteChannel)
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go handleStops(cancel, transfer)

	source, err := newSystem(transfer.SourceName, transfer.SourceType, transfer.SourceConnectionString)
	if err != nil {
		transfer.ErrorChannel <- fmt.Errorf("error creating source system :: %v", err)
		return
	}

	target, err := newSystem(transfer.TargetName, transfer.TargetType, transfer.TargetConnectionString)
	if err != nil {
		transfer.ErrorChannel <- fmt.Errorf("error creating target system :: %v", err)
		return
	}

	if transfer.DropTargetTableIfExists {
		dropped, err := target.dropTable(transfer.TargetSchema, transfer.TargetTable)
		if err != nil {
			transfer.ErrorChannel <- fmt.Errorf("error dropping target table :: %v", err)
			return
		}
		infoLog.Printf("transfer %v dropped %v if exists", transfer.Id, dropped)
	}

	rows, err := source.query(transfer.Query)
	if err != nil {
		transfer.ErrorChannel <- fmt.Errorf("error querying source :: %v", err)
		return
	}
	defer rows.Close()

	columnInfo, err := source.getColumnInfo(rows, source)
	if err != nil {
		transfer.ErrorChannel <- fmt.Errorf("error getting column info :: %v", err)
		return
	}

	if transfer.CreateTargetTable {
		created, err := createTableCommon(transfer.Id, transfer.TargetTable, columnInfo, target)
		if err != nil {
			transfer.ErrorChannel <- fmt.Errorf("error creating target table :: %v", err)
			return
		}
		infoLog.Printf("transfer %v created %v", transfer.Id, created)
	}

	transferErrGroup := &errgroup.Group{}

	pipeFiles, err := source.createPipeFiles(transfer.Id, transferErrGroup)
	if err != nil {
		transfer.ErrorChannel <- fmt.Errorf("error creating pipe files :: %v", err)
		return
	}

	err = target.insertPipeFiles(transfer, pipeFiles, transferErrGroup)
	if err != nil {
		transfer.ErrorChannel <- fmt.Errorf("error inserting pipe files :: %v", err)
		return
	}

	err = transferErrGroup.Wait()
	if err != nil {
		transfer.ErrorChannel <- fmt.Errorf("error waiting for transfer err group :: %v", err)
		return
	}

	transfer.CompleteChannel <- true
}
