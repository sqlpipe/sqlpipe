package main

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	"golang.org/x/sync/errgroup"
)

func transferInstance() error {

	var err error

	instanceTransfer.SourcePassword, err = generateRandomString(20)
	if err != nil {
		return fmt.Errorf("error generating random password :: %v", err)
	}

	changePasswordInput := &rds.ModifyDBInstanceInput{
		DBInstanceIdentifier: aws.String(instanceTransfer.BackupId),
		MasterUserPassword:   aws.String(instanceTransfer.SourcePassword),
		ApplyImmediately:     aws.Bool(true),
	}

	awsConfig := aws.Config{
		Credentials: credentials.NewStaticCredentialsProvider(instanceTransfer.AccountUsername, instanceTransfer.AccountPassword, ""),
		Region:      instanceTransfer.Region,
	}

	rdsClient := rds.NewFromConfig(awsConfig)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	logger.Info("changing instances password")

	_, err = rdsClient.ModifyDBInstance(ctx, changePasswordInput)
	if err != nil {
		return fmt.Errorf("error changing source password :: %v", err)
	}

	time.Sleep(10 * time.Second)

	// wait for the source instance to be available, check every second

	for {
		time.Sleep(1 * time.Second)

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		dbInstances, err := rdsClient.DescribeDBInstances(ctx, &rds.DescribeDBInstancesInput{
			DBInstanceIdentifier: aws.String(instanceTransfer.BackupId),
		})
		if err != nil {
			return fmt.Errorf("error describing source instance :: %v", err)
		}

		if len(dbInstances.DBInstances) == 0 {
			return fmt.Errorf("source instance not found")
		}

		if *dbInstances.DBInstances[0].DBInstanceStatus == "available" {
			break
		}
	}

	logger.Info("source instance password has changed, now available")

	time.Sleep(5 * time.Second)

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

	_, err = source.discoverStructure()
	if err != nil {
		return fmt.Errorf("error getting instance structure :: %v", err)
	}

	transferErrG := errgroup.Group{}
	// set limit on number of concurrent transfers

	transferErrG.SetLimit(5)

	for _, transferInfo := range instanceTransfer.TransferInfos {
		transferInfo := transferInfo

		logger.Info("starting transfer", "transfer-info", fmt.Sprintf("%+v", transferInfo))

		transferErrG.Go(func() error {
			err = runTransfer(transferInfo)
			if err != nil {
				logger.Error("error running transfer", "error", err)
				return err
			}

			return nil
		})
	}

	err = transferErrG.Wait()
	if err != nil {
		return fmt.Errorf("error running at least one transfer :: %v", err)
	}

	// drop staging database in snowflake
	targetConnectionInfo := ConnectionInfo{
		Name:     instanceTransfer.TargetName,
		Type:     instanceTransfer.TargetType,
		Hostname: instanceTransfer.TargetHostname,
		Port:     instanceTransfer.TargetPort,
		Username: instanceTransfer.TargetUsername,
		Password: instanceTransfer.TargetPassword,
	}

	target, err := newSystem(targetConnectionInfo)
	if err != nil {
		return fmt.Errorf("error creating target system :: %v", err)
	}
	defer target.closeConnectionPool(true)

	err = target.exec(fmt.Sprintf("DROP DATABASE IF EXISTS %v", instanceTransfer.BackupId))
	if err != nil {
		return fmt.Errorf("error dropping staging database :: %v", err)
	}

	return nil
}
