package main

import (
	"crypto/rand"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
	"unicode"
)

type envelope map[string]any

var schemaRequired = map[string]bool{TypePostgreSQL: true, TypeMySQL: false, TypeMSSQL: true, TypeOracle: true, TypeSnowflake: true}
var permittedTransferSources = []string{TypePostgreSQL, TypeMySQL, TypeMSSQL, TypeOracle, TypeSnowflake}
var permittedTransferTargets = []string{TypePostgreSQL, TypeMySQL, TypeMSSQL, TypeOracle, TypeSnowflake}

var (
	Statuses = []string{StatusQueued, StatusRunning, StatusCancelled, StatusError, StatusComplete, ""}

	StatusQueued    = "queued"
	StatusRunning   = "running"
	StatusCancelled = "cancelled"
	StatusError     = "error"
	StatusComplete  = "complete"

	TypePostgreSQL = "postgresql"
	TypeMySQL      = "mysql"
	TypeMSSQL      = "mssql"
	TypeOracle     = "oracle"
	TypeSnowflake  = "snowflake"

	DriverPostgreSQL = "pgx"
	DriverMySQL      = "mysql"
	DriverMSSQL      = "sqlserver"
	DriverOracle     = "oracle"
	DriverSnowflake  = "snowflake"
)

func getFileNum(fileName string) (fileNum int64, err error) {
	fileNameClean := filepath.Base(fileName)
	fileNumString := strings.Split(fileNameClean, ".")[0]
	fileNum, err = strconv.ParseInt(fileNumString, 2, 64)
	if err != nil {
		return 0, fmt.Errorf("error converting file number to int :: %v", err)
	}

	return fileNum, nil
}

func readJSON(w http.ResponseWriter, r *http.Request, dst any) error {
	maxBytes := 1_048_576
	r.Body = http.MaxBytesReader(w, r.Body, int64(maxBytes))

	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()

	err := dec.Decode(dst)
	if err != nil {
		var syntaxError *json.SyntaxError
		var unmarshalTypeError *json.UnmarshalTypeError
		var invalidUnmarshalError *json.InvalidUnmarshalError
		var maxBytesError *http.MaxBytesError

		switch {
		case errors.As(err, &syntaxError):
			return fmt.Errorf("body contains badly-formed JSON (at character %d)", syntaxError.Offset)

		case errors.Is(err, io.ErrUnexpectedEOF):
			return errors.New("body contains badly-formed JSON")

		case errors.As(err, &unmarshalTypeError):
			if unmarshalTypeError.Field != "" {
				return fmt.Errorf("body contains incorrect JSON type for field %q", unmarshalTypeError.Field)
			}
			return fmt.Errorf("body contains incorrect JSON type (at character %d)", unmarshalTypeError.Offset)

		case errors.Is(err, io.EOF):
			return errors.New("body must not be empty")

		case strings.HasPrefix(err.Error(), "json: unknown field "):
			fieldName := strings.TrimPrefix(err.Error(), "json: unknown field ")
			return fmt.Errorf("body contains unknown key %s", fieldName)

		case errors.As(err, &maxBytesError):
			return fmt.Errorf("body must not be larger than %d bytes", maxBytesError.Limit)

		case errors.As(err, &invalidUnmarshalError):
			panic(err)

		default:
			return err
		}
	}

	err = dec.Decode(&struct{}{})
	if err != io.EOF {
		return errors.New("body must only contain a single JSON value")
	}

	return nil
}

func writeJSON(w http.ResponseWriter, status int, data envelope, headers http.Header) error {
	js, err := json.MarshalIndent(data, "", "\t")
	if err != nil {
		return err
	}

	js = append(js, '\n')

	for key, value := range headers {
		w.Header()[key] = value
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	w.Write(js)

	return nil
}

func ProgramVersion() string {
	var revision string
	var modified bool

	bi, ok := debug.ReadBuildInfo()
	if ok {
		for _, s := range bi.Settings {
			switch s.Key {
			case "vcs.revision":
				revision = s.Value
			case "vcs.modified":
				if s.Value == "true" {
					modified = true
				}
			}
		}
	}

	if modified {
		return fmt.Sprintf("%s-dirty", revision)
	}

	return revision
}

func healthcheckHandler(w http.ResponseWriter, r *http.Request) {
	env := map[string]any{
		"status": "available",
		"system_info": map[string]string{
			"version": programVersion,
		},
	}

	err := writeJSON(w, http.StatusOK, env, nil)
	if err != nil {
		serverErrorResponse(w, r, http.StatusInternalServerError, err)
	}
}

func recoverPanic(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				w.Header().Set("Connection", "close")
				serverErrorResponse(w, r, http.StatusInternalServerError, fmt.Errorf("%s", err))
			}
		}()

		next.ServeHTTP(w, r)
	})
}

func maxColumnByteLength(filename, null string, columnIndex int) (int, error) {
	file, err := os.Open(filename)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	r := csv.NewReader(file)
	maxLength := 0

	for {
		record, err := r.Read()
		if err != nil {
			break
		}

		if columnIndex < 0 || columnIndex >= len(record) {
			return 0, fmt.Errorf("invalid column index %d", columnIndex)
		}

		length := len(record[columnIndex])
		if length > maxLength {
			maxLength = length
		}
	}

	return maxLength + len(null), nil
}

func checkDeps() {
	checkPsql()
	checkBcp()
	checkSqlLdr()
}

func checkPsql() {
	_, err := exec.Command("psql", "--version").CombinedOutput()
	if err != nil {
		// warningLog.Printf("psql not found. please install psql to transfer data to postgresql :: %v :: %v\n", err, string(output))
		return
	}

	psqlAvailable = true
}

func checkBcp() {
	_, err := exec.Command("bcp", "-v").CombinedOutput()
	if err != nil {
		// warningLog.Printf("bcp not found. please install bcp to transfer data to mssql :: %v :: %v\n", err, string(output))
		return
	}

	bcpAvailable = true
}

func checkSqlLdr() {
	_, err := exec.Command("sqlldr", "-help").CombinedOutput()
	if err != nil {
		// warningLog.Printf("sqlldr not found. please install sqllder to transfer data to oracle :: %v :: %v\n", err, string(output))
		return
	}

	sqlldrAvailable = true
}

func containsSpaces(s string) bool {
	for _, char := range s {
		if unicode.IsSpace(char) {
			return true
		}
	}
	return false
}

func RandomPrintableAsciiCharacters(length int) (string, error) {
	randomString := ""

	possibleCharacters := []string{
		`0`,
		`1`,
		`2`,
		`3`,
		`4`,
		`5`,
		`6`,
		`7`,
		`8`,
		`9`,
		`A`,
		`B`,
		`C`,
		`D`,
		`E`,
		`F`,
		`G`,
		`H`,
		`I`,
		`J`,
		`K`,
		`L`,
		`M`,
		`N`,
		`O`,
		`P`,
		`Q`,
		`R`,
		`S`,
		`T`,
		`U`,
		`V`,
		`W`,
		`X`,
		`Y`,
		`Z`,
		`a`,
		`b`,
		`c`,
		`d`,
		`e`,
		`f`,
		`g`,
		`h`,
		`i`,
		`j`,
		`k`,
		`l`,
		`m`,
		`n`,
		`o`,
		`p`,
		`q`,
		`r`,
		`s`,
		`t`,
		`u`,
		`v`,
		`w`,
		`x`,
		`y`,
		`z`,
	}

	numChars := int64(len(possibleCharacters))

	for i := 0; i < length; i++ {
		nBig, err := rand.Int(rand.Reader, big.NewInt(numChars))
		if err != nil {
			err = fmt.Errorf("error generating random number:: %v", err)
			return "", err
		}
		randomInt := int(nBig.Int64())

		randomString = randomString + possibleCharacters[randomInt]
	}

	return randomString, nil
}

func RandomLetters(length int) (string, error) {
	randomString := ""

	possibleCharacters := []string{
		`A`,
		`B`,
		`C`,
		`D`,
		`E`,
		`F`,
		`G`,
		`H`,
		`I`,
		`J`,
		`K`,
		`L`,
		`M`,
		`N`,
		`O`,
		`P`,
		`Q`,
		`R`,
		`S`,
		`T`,
		`U`,
		`V`,
		`W`,
		`X`,
		`Y`,
		`Z`,
		`a`,
		`b`,
		`c`,
		`d`,
		`e`,
		`f`,
		`g`,
		`h`,
		`i`,
		`j`,
		`k`,
		`l`,
		`m`,
		`n`,
		`o`,
		`p`,
		`q`,
		`r`,
		`s`,
		`t`,
		`u`,
		`v`,
		`w`,
		`x`,
		`y`,
		`z`,
	}

	numChars := int64(len(possibleCharacters))

	for i := 0; i < length; i++ {
		nBig, err := rand.Int(rand.Reader, big.NewInt(numChars))
		if err != nil {
			err = fmt.Errorf("error generating random number:: %v", err)
			return "", err
		}
		randomInt := int(nBig.Int64())

		randomString = randomString + possibleCharacters[randomInt]
	}

	return randomString, nil
}

type validator struct {
	errors map[string]string
}

func newValidator() validator {
	return validator{errors: make(map[string]string)}
}

func (v *validator) valid() bool {
	return len(v.errors) == 0
}

func (v *validator) addError(key, message string) {
	if _, exists := v.errors[key]; !exists {
		v.errors[key] = message
	}
}

func (v *validator) check(ok bool, key, message string) {
	if !ok {
		v.addError(key, message)
	}
}

func permittedValue[T comparable](value T, permittedValues ...T) bool {
	for i := range permittedValues {
		if value == permittedValues[i] {
			return true
		}
	}
	return false
}

type ColumnInfo struct {
	Name         string `json:"name"`
	PipeType     string `json:"pipe-type"`
	ScanType     string `json:"scan-type"`
	DecimalOk    bool   `json:"decimal-ok"`
	Precision    int64  `json:"precision"`
	Scale        int64  `json:"scale"`
	LengthOk     bool   `json:"length-ok"`
	Length       int64  `json:"length"`
	NullableOk   bool   `json:"nullable-ok"`
	Nullable     bool   `json:"nullable"`
	IsPrimaryKey bool   `json:"is-primary-key"`
}

type FinalCsvInfo struct {
	FilePath   string
	InsertInfo string
}
