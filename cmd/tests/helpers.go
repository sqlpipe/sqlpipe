package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

type ConnectionInfo struct {
	SystemType       string
	Host             string
	Port             int
	User             string
	Password         string
	DBName           string
	Account          string
	ConnectionString string
	Schema           string
	Table            string
}

func makeSqlpipeTransfer(source, target ConnectionInfo) error {
	payload := map[string]interface{}{
		"source-name":                        source.SystemType,
		"source-type":                        source.SystemType,
		"source-connection-string":           source.ConnectionString,
		"source-table":                       source.Table,
		"target-name":                        target.SystemType,
		"target-type":                        target.SystemType,
		"target-hostname":                    target.Host,
		"target-port":                        target.Port,
		"target-username":                    target.User,
		"target-password":                    target.Password,
		"target-connection-string":           target.ConnectionString,
		"target-table":                       fmt.Sprintf("%v_%v", target.Table, source.SystemType),
		"drop-target-table-if-exists":        true,
		"create-target-table-if-not-exists":  true,
		"create-target-schema-if-not-exists": true,
		"target-database":                    target.DBName,
	}

	if source.Schema != "" {
		payload["source-schema"] = source.Schema
	}

	if target.Schema != "" {
		payload["target-schema"] = target.Schema
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("error marshalling json :: %v", err)
	}

	transferRoute := fmt.Sprintf("%s/transfers/create", serverAddress)

	req, err := http.NewRequest("POST", transferRoute, bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("error creating request :: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("error sending request :: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("error reading response body :: %v", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		fmt.Println(string(body))
		return fmt.Errorf("error response status :: %v", resp.Status)
	}

	return nil
}
