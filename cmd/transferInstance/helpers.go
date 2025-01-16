package main

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"os/exec"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
	"unicode"

	"github.com/sqlpipe/sqlpipe/internal/data"
)

// misc log keys
const (
	OriginalInstanceId   = "instance-id"
	OriginalDatabaseName = "database"
	OriginalSchemaName   = "schema"
	OriginalTableName    = "table"
	BackupInstanceId     = "backup-instance-id"
	StagingDatabaseName  = "staging-database"
	TargetDatabaseName   = "target-database"
	TargetSchemaName     = "target-schema"
	TargetTableName      = "target-table"
)

var (
	DriverPostgreSQL = "pgx"
	DriverMySQL      = "mysql"
	DriverMSSQL      = "sqlserver"
	DriverOracle     = "oracle"
	DriverSnowflake  = "snowflake"
)

var (
	StatusPending  = "pending"
	StatusRunning  = "running"
	StatusError    = "error"
	StatusComplete = "complete"
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

// func maxColumnByteLength(filename, null string, columnIndex int) (int, error) {
// 	file, err := os.Open(filename)
// 	if err != nil {
// 		return 0, err
// 	}
// 	defer file.Close()

// 	r := csv.NewReader(file)
// 	maxLength := 0

// 	for {
// 		record, err := r.Read()
// 		if err != nil {
// 			break
// 		}

// 		if columnIndex < 0 || columnIndex >= len(record) {
// 			return 0, fmt.Errorf("invalid column index %d", columnIndex)
// 		}

// 		length := len(record[columnIndex])
// 		if length > maxLength {
// 			maxLength = length
// 		}
// 	}

// 	return maxLength + len(null), nil
// }

func checkDeps(instanceTransfer *data.InstanceTransfer) {
	checkPsql(instanceTransfer)
	checkBcp(instanceTransfer)
	checkSqlLdr(instanceTransfer)
}

func checkPsql(instanceTransfer *data.InstanceTransfer) {
	output, err := exec.Command("psql", "--version").CombinedOutput()
	if err != nil {
		logger.Warn(fmt.Sprintf("psql not found. please install psql to transfer data to postgresql :: %v :: %v\n", err, string(output)))
		return
	}

	instanceTransfer.PsqlAvailable = true
}

func checkBcp(instanceTransfer *data.InstanceTransfer) {
	output, err := exec.Command("bcp", "-v").CombinedOutput()
	if err != nil {
		logger.Warn(fmt.Sprintf("bcp not found. please install bcp to transfer data to mssql :: %v :: %v\n", err, string(output)))
		return
	}

	instanceTransfer.BcpAvailable = true
}

func checkSqlLdr(instanceTransfer *data.InstanceTransfer) {
	output, err := exec.Command("sqlldr", "-help").CombinedOutput()
	if err != nil {
		logger.Warn(fmt.Sprintf("sqlldr not found. please install sqllder to transfer data to oracle :: %v :: %v\n", err, string(output)))
		return
	}

	instanceTransfer.SqlLdrAvailable = true
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

func permittedValue[T comparable](value T, permittedValues ...T) bool {
	for i := range permittedValues {
		if value == permittedValues[i] {
			return true
		}
	}
	return false
}

const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
const charsetLength = int64(len(charset))

func generateRandomString(length int) (string, error) {

	randomChars := make([]byte, length)

	for i := 0; i < length; i++ {
		nBig, err := rand.Int(rand.Reader, big.NewInt(charsetLength))
		if err != nil {
			err = fmt.Errorf("error generating random number:: %v", err)
			return "", err
		}
		randomInt := int(nBig.Int64())

		randomChars[i] = charset[randomInt]
	}

	randomString := string(randomChars)

	return randomString, nil

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

// func getAdminDbName(dbType string) (string, error) {

// 	var adminDbName string

// 	switch dbType {
// 	case data.TypePostgreSQL:
// 		adminDbName = "postgres"
// 	default:
// 		return "", fmt.Errorf("unsupported db type: %s", dbType)
// 	}

// 	return adminDbName, nil

// }
