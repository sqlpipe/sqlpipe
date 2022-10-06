package main

import (
	"database/sql"
	"fmt"
	"io"
	"net/http"
	"os"

	"github.com/sqlpipe/sqlpipe/internal/data"
	"github.com/sqlpipe/sqlpipe/internal/engine/csvs"
	"github.com/sqlpipe/sqlpipe/internal/validator"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func (app *application) runCsvSaveOnServerHandler(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Source        data.Source `json:"source"`
		WriteLocation string      `json:"write_location"`
		Query         string      `json:"query"`
	}

	err := app.readJSON(w, r, &input)
	if err != nil {
		app.badRequestResponse(w, r, err)
		return
	}

	export := &data.CsvSave{
		Source:        input.Source,
		WriteLocation: input.WriteLocation,
		Query:         input.Query,
	}

	v := validator.New()
	if data.ValidateCsvSave(v, export); !v.Valid() {
		app.failedValidationResponse(w, r, v.Errors)
		return
	}

	var sourceDb *sql.DB
	sourceDb, err = sql.Open(
		"odbc",
		export.Source.OdbcDsn,
	)
	if err != nil {
		app.errorResponse(w, r, http.StatusBadRequest, err)
		return
	}

	export.Source.Db = sourceDb
	err = export.Source.Db.Ping()
	if err != nil {
		app.errorResponse(w, r, http.StatusBadRequest, err)
		return
	}

	_, err = os.Stat(export.WriteLocation)
	if err == nil {
		err = os.Remove(export.WriteLocation)
		if err != nil {
			app.errorResponse(w, r, http.StatusBadRequest, err)
			return
		}
	}

	file, err := os.OpenFile(export.WriteLocation, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		app.errorResponse(w, r, http.StatusBadRequest, err)
		return
	}

	err = csvs.WriteCsvToFile(r.Context(), *export, file)
	if err != nil {
		app.errorResponse(w, r, http.StatusBadRequest, err)
		return
	}

	err = app.respondWithJSON(w, http.StatusOK, map[string]any{"message": fmt.Sprintf("csv file written to %v", file.Name())}, make(http.Header))
	if err != nil {
		app.errorResponse(w, r, http.StatusInternalServerError, err)
	}
}

func (app *application) runCsvDownloadHandler(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Source data.Source `json:"source"`
		Query  string      `json:"query"`
	}

	err := app.readJSON(w, r, &input)
	if err != nil {
		app.badRequestResponse(w, r, err)
		return
	}

	export := &data.CsvSave{
		Source: input.Source,
		Query:  input.Query,
	}

	v := validator.New()
	if data.ValidateCsvSaveNoWriteLocation(v, export); !v.Valid() {
		app.failedValidationResponse(w, r, v.Errors)
		return
	}

	var sourceDb *sql.DB
	sourceDb, err = sql.Open(
		"odbc",
		export.Source.OdbcDsn,
	)
	if err != nil {
		app.errorResponse(w, r, http.StatusBadRequest, err)
		return
	}

	export.Source.Db = sourceDb
	err = export.Source.Db.Ping()
	if err != nil {
		app.errorResponse(w, r, http.StatusBadRequest, err)
		return
	}

	file, err := os.CreateTemp("", "")
	if err != nil {
		app.errorResponse(w, r, http.StatusInternalServerError, err)
	}

	err = csvs.WriteCsvToFile(r.Context(), *export, file)
	if err != nil {
		app.errorResponse(w, r, http.StatusBadRequest, err)
		return
	}

	err = app.respondWithCSV(w, http.StatusOK, file, make(http.Header))
	if err != nil {
		app.errorResponse(w, r, http.StatusInternalServerError, err)
	}
}

func (app *application) runCsvS3UploadHandler(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Source data.Source   `json:"source"`
		Target data.S3Upload `json:"target"`
		Query  string        `json:"query"`
	}

	err := app.readJSON(w, r, &input)
	if err != nil {
		app.badRequestResponse(w, r, err)
		return
	}

	s3Upload := &data.S3Upload{
		Source:    input.Source,
		Query:     input.Query,
		AwsRegion: input.Target.AwsRegion,
		AwsKey:    input.Target.AwsKey,
		AwsSecret: input.Target.AwsSecret,
		AwsToken:  input.Target.AwsToken,
		S3Bucket:  input.Target.S3Bucket,
		S3Dir:     input.Target.S3Dir,
		FileName:  input.Target.FileName,
	}

	v := validator.New()
	if data.ValidateS3Upload(v, s3Upload); !v.Valid() {
		app.failedValidationResponse(w, r, v.Errors)
		return
	}

	export := &data.CsvSave{
		Source: input.Source,
		Query:  input.Query,
	}

	if data.ValidateCsvSaveNoWriteLocation(v, export); !v.Valid() {
		app.failedValidationResponse(w, r, v.Errors)
		return
	}

	var sourceDb *sql.DB
	sourceDb, err = sql.Open(
		"odbc",
		export.Source.OdbcDsn,
	)
	if err != nil {
		app.errorResponse(w, r, http.StatusBadRequest, err)
		return
	}

	export.Source.Db = sourceDb
	err = export.Source.Db.Ping()
	if err != nil {
		app.errorResponse(w, r, http.StatusBadRequest, err)
		return
	}

	file, err := os.CreateTemp("", "")
	if err != nil {
		app.errorResponse(w, r, http.StatusInternalServerError, err)
	}
	defer file.Close()

	err = csvs.WriteCsvToFile(r.Context(), *export, file)
	if err != nil {
		app.errorResponse(w, r, http.StatusBadRequest, err)
		return
	}

	_, err = file.Seek(0, io.SeekStart)
	if err != nil {
		app.errorResponse(w, r, http.StatusInternalServerError, err)
	}

	creds := credentials.NewStaticCredentialsProvider(
		s3Upload.AwsKey,
		s3Upload.AwsSecret,
		s3Upload.AwsToken,
	)

	awsClientCfg, err := config.LoadDefaultConfig(
		r.Context(),
		config.WithRegion(s3Upload.AwsRegion),
		config.WithCredentialsProvider(creds),
	)
	if err != nil {
		app.errorResponse(w, r, http.StatusBadRequest, err)
		return
	}

	s3Client := s3.NewFromConfig(awsClientCfg)
	uploader := manager.NewUploader(s3Client)
	fileKey := fmt.Sprintf("%v/%v", s3Upload.S3Dir, s3Upload.FileName)

	_, err = uploader.Upload(r.Context(), &s3.PutObjectInput{
		Bucket: &input.Target.S3Bucket,
		Key:    aws.String(fileKey),
		Body:   file,
	})
	if err != nil {
		app.errorResponse(w, r, http.StatusBadRequest, err)
		return
	}

	err = app.respondWithJSON(w, http.StatusOK, map[string]any{"message": fmt.Sprintf("csv file written to %v/%v/%v", s3Upload.S3Bucket, s3Upload.S3Dir, s3Upload.FileName)}, make(http.Header))
	if err != nil {
		app.errorResponse(w, r, http.StatusInternalServerError, err)
	}
}
