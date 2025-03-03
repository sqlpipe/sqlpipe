package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	"golang.org/x/sync/errgroup"
)

func transferInstance() error {

	var err error

	if instanceTransfer.SourceInstance.Password == "" {
		awsConfig := aws.Config{
			Credentials: credentials.NewStaticCredentialsProvider(instanceTransfer.CloudUsername, instanceTransfer.CloudPassword, ""),
			Region:      instanceTransfer.SourceInstance.Region,
		}

		rdsClient := rds.NewFromConfig(awsConfig)
		instanceTransfer.SourceInstance.Password, err = generateRandomString(20)
		if err != nil {
			return fmt.Errorf("error generating random password :: %v", err)
		}

		changePasswordInput := &rds.ModifyDBInstanceInput{
			DBInstanceIdentifier: aws.String(instanceTransfer.RestoredInstanceID),
			MasterUserPassword:   aws.String(instanceTransfer.SourceInstance.Password),
			ApplyImmediately:     aws.Bool(true),
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		logger.Info("changing backup instances password", BackupInstanceId, instanceTransfer.RestoredInstanceID)

		_, err = rdsClient.ModifyDBInstance(ctx, changePasswordInput)
		if err != nil {
			return fmt.Errorf("error changing source password :: %v", err)
		}

		// it takes a few seconds for the password change process to start
		time.Sleep(30 * time.Second)

		ctx, cancel = context.WithTimeout(context.Background(), 10*time.Minute)
		defer cancel()

		for {

			if ctx.Err() != nil {
				return fmt.Errorf("timeout waiting for instance to be available")
			}

			time.Sleep(5 * time.Second)

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			dbInstances, err := rdsClient.DescribeDBInstances(ctx, &rds.DescribeDBInstancesInput{
				DBInstanceIdentifier: aws.String(instanceTransfer.RestoredInstanceID),
			})
			if err != nil {
				return fmt.Errorf("error describing backup instance :: %v", err)
			}

			if len(dbInstances.DBInstances) == 0 {
				return fmt.Errorf("backup instance id %v not found", instanceTransfer.RestoredInstanceID)
			}

			logger.Info("now checking instance", "instance status", fmt.Sprintf("%v", *dbInstances.DBInstances[0].DBInstanceStatus))

			if *dbInstances.DBInstances[0].DBInstanceStatus == "available" {
				break
			}
		}

		logger.Info("source instance password has changed, starting transfer", BackupInstanceId, instanceTransfer.RestoredInstanceID)
	}
	var dbName string

	if instanceTransfer.SourceInstance.Type == "oracle" {

		awsConfig := aws.Config{
			Credentials: credentials.NewStaticCredentialsProvider(instanceTransfer.CloudUsername, instanceTransfer.CloudPassword, ""),
			Region:      instanceTransfer.SourceInstance.Region,
		}

		rdsClient := rds.NewFromConfig(awsConfig)

		describeDBInstancesInput := &rds.DescribeDBInstancesInput{
			DBInstanceIdentifier: aws.String(instanceTransfer.RestoredInstanceID),
		}

		dbInstances, err := rdsClient.DescribeDBInstances(context.Background(), describeDBInstancesInput)
		if err != nil {
			return fmt.Errorf("error describing DB instances: %v", err)
		}

		if len(dbInstances.DBInstances) == 0 {
			return fmt.Errorf("no DB instances found with identifier: %v", instanceTransfer.RestoredInstanceID)
		}

		dbInstance := dbInstances.DBInstances[0]
		if dbInstance.Engine != nil && *dbInstance.Engine == "oracle-se2" {
			dbName = *dbInstance.DBName
		} else {
			return fmt.Errorf("DB instance is not a single tenant Oracle DB")
		}

		if dbName == "" {
			return fmt.Errorf("no database name found for instance %v", instanceTransfer.RestoredInstanceID)
		}
	}

	sourceConnectionInfo := ConnectionInfo{
		Type:     instanceTransfer.SourceInstance.Type,
		Hostname: instanceTransfer.SourceInstance.Host,
		Port:     instanceTransfer.SourceInstance.Port,
		Username: instanceTransfer.SourceInstance.Username,
		Password: instanceTransfer.SourceInstance.Password,
		Database: dbName,
	}

	source, err := newSystem(sourceConnectionInfo)
	if err != nil {
		return fmt.Errorf("error creating source system :: %v", err)
	}
	defer source.closeConnectionPool(true)

	instanceTransfer, err = source.discoverStructure(instanceTransfer)
	if err != nil {
		return fmt.Errorf("error getting instance structure :: %v", err)
	}

	transferErrG := errgroup.Group{}

	transferErrG.SetLimit(5)

	for _, transferInfo := range instanceTransfer.TransferInfos {
		transferInfo := transferInfo

		logger.Info("starting transfer", OriginalDatabaseName, transferInfo.SourceDatabase, "schema", transferInfo.SourceSchema, "table", transferInfo.SourceTable)

		transferErrG.Go(func() error {
			err = runTransfer(transferInfo)
			if err != nil {
				logger.Error("error running transfer", "error", err, "database", transferInfo.SourceDatabase, "schema", transferInfo.SourceSchema, "table", transferInfo.SourceTable)
				return err
			}

			return nil
		})
	}

	err = transferErrG.Wait()
	if err != nil {
		return errors.New("error running one or more transfers. please check logs for more information")
	}

	targetConnectionInfo := ConnectionInfo{
		Type:     instanceTransfer.TargetType,
		Hostname: instanceTransfer.TargetHost,
		Username: instanceTransfer.TargetUsername,
		Password: instanceTransfer.TargetPassword,
	}

	target, err := newSystem(targetConnectionInfo)
	if err != nil {
		return fmt.Errorf("error creating target system :: %v", err)
	}

	for _, dbName := range instanceTransfer.StagingDbNames {
		dropStagingDbQuery := fmt.Sprintf(`DROP DATABASE IF EXISTS %v`, dbName)
		err := target.exec(dropStagingDbQuery)
		if err != nil {
			logger.Error("error dropping staging database", "error", err)
		}
	}

	instanceTransfer.SchemaTree.RecursivelySearchForPII()

	schemaTreeJson, err := instanceTransfer.SchemaTree.ToJSON()
	if err != nil {
		return fmt.Errorf("error converting schema tree to json :: %v", err)
	}

	fmt.Printf("SCHEMATREERAWJSON%v\n", schemaTreeJson)

	return nil
}
