package data

import (
	"github.com/sqlpipe/sqlpipe/internal/validator"
	"github.com/sqlpipe/sqlpipe/pkg"
)

type CsvSave struct {
	Source        Source `json:"source"`
	WriteLocation string `json:"write_location"`
	Query         string `json:"query"`
}

type S3Upload struct {
	Source    Source `json:"source"`
	Query     string `json:"query"`
	AwsRegion string `json:"aws_region"`
	AwsKey    string `json:"aws_key"`
	AwsSecret string `json:"aws_secret"`
	AwsToken  string `json:"aws_token"`
	S3Bucket  string `json:"s3_bucket"`
	S3Dir     string `json:"s3_dir"`
	FileName  string `json:"file_name"`
}

func ValidateCsvSave(v *validator.Validator, csvSave *CsvSave) {
	ValidateSource(v, csvSave.Source)
	v.Check(pkg.IsValidPath(csvSave.WriteLocation), "write_location", "must be a valid file path")
	v.Check(csvSave.Query != "", "query", "must be provided")
}

func ValidateCsvSaveNoWriteLocation(v *validator.Validator, csvSave *CsvSave) {
	ValidateSource(v, csvSave.Source)
	v.Check(csvSave.Query != "", "query", "must be provided")
}

func ValidateS3Upload(v *validator.Validator, s3Upload *S3Upload) {
	ValidateSource(v, s3Upload.Source)
	v.Check(s3Upload.Query != "", "query", "must be provided")
	v.Check(s3Upload.AwsRegion != "", "aws_region", "must be provided")
	v.Check(s3Upload.AwsKey != "", "aws_key", "must be provided")
	v.Check(s3Upload.AwsSecret != "", "aws_secret", "must be provided")
	v.Check(s3Upload.S3Bucket != "", "s3_bucket", "must be provided")
	v.Check(s3Upload.S3Dir != "", "s3_dir", "must be provided")
	v.Check(s3Upload.FileName != "", "file_name", "must be provided")
}
