package main

import (
	"fmt"
	"os"
	"path/filepath"

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

func runTransfer(transfer *Transfer) {
	if transfer.DropTargetTable {
		err := transfer.Target.dropTable(transfer.TargetSchema, transfer.TargetTable)
		if err != nil {
			transferError(transfer, fmt.Errorf("error dropping target table :: %v", err))
			return
		}
	}

	var err error
	transfer.Rows, err = transfer.Source.query(transfer.Query)
	if err != nil {
		transferError(transfer, fmt.Errorf("error querying source :: %v", err))
		return
	}
	defer transfer.Rows.Close()

	transfer.ColumnInfo, err = transfer.Source.getColumnInfo(transfer)
	if err != nil {
		transferError(transfer, fmt.Errorf("error getting source column info :: %v", err))
		return
	}

	if transfer.CreateTargetTable {
		err = transfer.Target.createTable(transfer)
		if err != nil {
			transferError(transfer, fmt.Errorf("error creating target table :: %v", err))
			return
		}
	}

	transfer.TmpDir = filepath.Join(os.TempDir(), fmt.Sprintf("sqlpipe-transfer-%v", transfer.Id))
	err = os.Mkdir(transfer.TmpDir, os.ModePerm)
	if err != nil {
		transferError(transfer, fmt.Errorf("error creating transfer dir :: %v", err))
		return
	}
	// defer os.RemoveAll(transfer.TmpDir)

	transferErrGroup := &errgroup.Group{}

	pipeFiles, err := transfer.Source.createPipeFiles(transfer, transferErrGroup)
	if err != nil {
		transferError(transfer, fmt.Errorf("error writing pipe file :: %v", err))
		return
	}

	err = transfer.Target.insertPipeFiles(transfer, pipeFiles, transferErrGroup)
	if err != nil {
		transferError(transfer, fmt.Errorf("error inserting data :: %v", err))
		return
	}

	err = transferErrGroup.Wait()
	if err != nil {
		transferError(transfer, fmt.Errorf("error inserting data :: %v", err))
		return
	}

	infoLog.Printf("transfer %v complete\n", transfer.Id)
}
