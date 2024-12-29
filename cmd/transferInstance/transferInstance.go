package main

import (
	"fmt"

	"golang.org/x/sync/errgroup"
)

func transferInstance() error {
	sourceConnectionInfo := ConnectionInfo{
		Name:     instanceTransfer.SourceName,
		Type:     instanceTransfer.SourceType,
		Hostname: instanceTransfer.SourceHostname,
		Port:     instanceTransfer.SourcePort,
		Username: instanceTransfer.SourceUsername,
		Password: instanceTransfer.SourcePassword,
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

	fmt.Println("Instance structure:")
	instanceRootNode.PrettyPrint("")

	transferErrG := errgroup.Group{}

	for _, transferInfo := range instanceTransfer.TransferInfos {
		transferInfo := transferInfo

		logger.Info(fmt.Sprintf("running transfer :: %v", transferInfo))

		transferErrG.Go(func() error {
			err = runTransfer(transferInfo)
			if err != nil {
				logger.Error(fmt.Sprintf("error running transfer :: %v", err))
				return err
			}

			return nil
		})
	}

	err = transferErrG.Wait()
	if err != nil {
		return fmt.Errorf("error running at least one transfer :: %v", err)
	}

	return nil
}
