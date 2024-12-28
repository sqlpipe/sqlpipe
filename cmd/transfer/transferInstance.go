package main

import (
	"fmt"

	"github.com/sqlpipe/sqlpipe/internal/data"
)

func transferInstance(transferInfo data.TransferInfo) error {
	sourceConnectionInfo := ConnectionInfo{
		Name:     transferInfo.SourceName,
		Type:     transferInfo.SourceType,
		Hostname: transferInfo.SourceHostname,
		Port:     transferInfo.SourcePort,
		Database: transferInfo.SourceDatabase,
		Username: transferInfo.SourceUsername,
		Password: transferInfo.SourcePassword,
	}

	source, err := newSystem(sourceConnectionInfo)
	if err != nil {
		return fmt.Errorf("error creating source system :: %v", err)
	}
	defer source.closeConnectionPool(true)

	instanceRootNode, err := source.discoverStructure()
	if err != nil {
		return fmt.Errorf("error getting instance structure :: %v", err)
	}

	instanceRootNode.PrintIndented(0)

	return nil
}
