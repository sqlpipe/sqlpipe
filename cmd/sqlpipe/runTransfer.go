package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

func handleUpdates(
	cancel context.CancelFunc,
	statusChannel <-chan string,
	errorChannel <-chan error,
	transfer Transfer,
) {
	// all transfer updates are sent via channel - this function handles those updates

	exit := false

	for {
		select {
		case ip := <-transfer.CancelChannel:
			cancel()
			transfer.Status = StatusCancelled
			transfer.StoppedAt = fmt.Sprint(time.Now())
			infoLog.Printf("transfer %v cancelled by ip %v", transfer.Id, ip)
			exit = true
			defer func() {
				transfer.CancelledChannel <- true
			}()
		case err := <-errorChannel:
			cancel()
			transfer.Status = StatusError
			transfer.StoppedAt = fmt.Sprint(time.Now())
			transfer.Err = err.Error()
			errorLog.Printf("error running transfer %v :: %v", transfer.Id, err)
			exit = true
		case status := <-statusChannel:
			transfer.Status = status
			infoLog.Printf("transfer %v is now %v", transfer.Id, status)
		}

		transferMap.Set(transfer.Id, transfer)

		if exit {
			return
		}
	}
}

func runTransfer(transfer Transfer) {
	statusChannel := make(chan string)
	errorChannel := make(chan error)

	ctx, cancel := context.WithCancel(context.Background())

	go handleUpdates(cancel, statusChannel, errorChannel, transfer)

	statusChannel <- StatusRunning

	// create temp directories
	err := os.MkdirAll(transfer.TmpDir, 0600)
	if err != nil {
		errorChannel <- fmt.Errorf("error creating temp dir :: %v", err)
	}

	defer func() {
		if !transfer.KeepFiles {
			os.RemoveAll(transfer.TmpDir)
			infoLog.Printf("temp dir %v removed", transfer.TmpDir)
		}
	}()

	infoLog.Printf("temp dir %v created", transfer.TmpDir)

	transfer.PipeFileDir = filepath.Join(transfer.TmpDir, "pipe-files")
	err = os.MkdirAll(transfer.PipeFileDir, 0600)
	if err != nil {
		errorChannel <- fmt.Errorf("error creating pipe file dir :: %v", err)
	}
	infoLog.Printf("pipe file dir %v created", transfer.PipeFileDir)

	transfer.FinalCsvDir = filepath.Join(transfer.TmpDir, "final-csv")
	err = os.MkdirAll(transfer.FinalCsvDir, 0600)
	if err != nil {
		errorChannel <- fmt.Errorf("error creating final csv dir :: %v", err)
	}
	infoLog.Printf("final csv dir %v created", transfer.FinalCsvDir)

	source, err := newSystem(
		transfer.SourceName, transfer.SourceType, transfer.SourceConnectionString,
	)
	if err != nil {
		errorChannel <- fmt.Errorf("error creating source system :: %v", err)
		return
	}

	target, err := newSystem(
		transfer.TargetName, transfer.TargetType, transfer.TargetConnectionString,
	)
	if err != nil {
		errorChannel <- fmt.Errorf("error creating target system :: %v", err)
		return
	}

	if transfer.DropTargetTableIfExists {
		dropped, err := target.dropTableIfExists(transfer)
		if err != nil {
			errorChannel <- fmt.Errorf("error dropping target table :: %v", err)
			return
		}
		infoLog.Printf("transfer %v dropped %v if exists", transfer.Id, dropped)
	}

	rows, err := source.query(transfer.Query)
	if err != nil {
		errorChannel <- fmt.Errorf("error querying source :: %v", err)
		return
	}
	defer rows.Close()

	columnInfo, err := source.getColumnInfo(rows)
	if err != nil {
		errorChannel <- fmt.Errorf("error getting column info :: %v", err)
		return
	}

	if transfer.CreateTargetTable {
		created, err := target.createTable(columnInfo, transfer)
		if err != nil {
			errorChannel <- fmt.Errorf("error creating target table :: %v", err)
			return
		}
		infoLog.Printf("transfer %v created %v", transfer.Id, created)
	}

	pipeFileChannel := source.createPipeFiles(ctx, errorChannel, columnInfo, transfer, rows)

	err = target.insertPipeFiles(ctx, pipeFileChannel, errorChannel, columnInfo, transfer)
	if err != nil {
		errorChannel <- fmt.Errorf("error inserting pipe files :: %v", err)
		return
	}

	statusChannel <- StatusComplete
}
