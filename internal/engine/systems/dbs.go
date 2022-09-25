package systems

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

var dbValReplacer = strings.NewReplacer(`'`, `''`, "{", "(", "}", ")")

var DropTableCommands = map[string]string{
	"postgresql": "drop table if exists",
}

var NullStrings = map[string]string{
	"postgresql": "null",
}

var CreateWriters = map[string]map[string]func(column *sql.ColumnType, terminator string) (string, error){
	"postgresql": {
		"SQL_UNKNOWN_TYPE":    textCreateWriter,
		"SQL_CHAR":            textCreateWriter,
		"SQL_NUMERIC":         numericCreateWriter,
		"SQL_DECIMAL":         numericCreateWriter,
		"SQL_INTEGER":         intCreateWriter,
		"SQL_SMALLINT":        smallIntCreateWriter,
		"SQL_FLOAT":           doubleCreateWriter,
		"SQL_REAL":            doubleCreateWriter,
		"SQL_DOUBLE":          doubleCreateWriter,
		"SQL_DATETIME":        timestampCreateWriter,
		"SQL_TIME":            timeCreateWriter,
		"SQL_VARCHAR":         textCreateWriter,
		"SQL_TYPE_DATE":       dateCreateWriter,
		"SQL_TYPE_TIME":       timeCreateWriter,
		"SQL_TYPE_TIMESTAMP":  timestampCreateWriter,
		"SQL_TIMESTAMP":       timestampCreateWriter,
		"SQL_LONGVARCHAR":     textCreateWriter,
		"SQL_BINARY":          byteaCreateWriter,
		"SQL_VARBINARY":       byteaCreateWriter,
		"SQL_LONGVARBINARY":   byteaCreateWriter,
		"SQL_BIGINT":          bigIntCreateWriter,
		"SQL_TINYINT":         smallIntCreateWriter,
		"SQL_BIT":             boolCreateWriter,
		"SQL_WCHAR":           textCreateWriter,
		"SQL_WVARCHAR":        textCreateWriter,
		"SQL_WLONGVARCHAR":    textCreateWriter,
		"SQL_GUID":            uuidCreateWriter,
		"SQL_SIGNED_OFFSET":   textCreateWriter,
		"SQL_UNSIGNED_OFFSET": textCreateWriter,
		"SQL_SS_XML":          xmlCreateWriter,
		"SQL_SS_TIME2":        timeCreateWriter,
	},
}

func textCreateWriter(column *sql.ColumnType, terminator string) (string, error) {
	return fmt.Sprintf("%v text%v", column.Name(), terminator), nil
}

func numericCreateWriter(column *sql.ColumnType, terminator string) (string, error) {
	precision, scale, _ := column.DecimalSize()
	return fmt.Sprintf("%v numeric(%v,%v)%v", column.Name(), precision, scale, terminator), nil
}

func smallIntCreateWriter(column *sql.ColumnType, terminator string) (string, error) {
	return fmt.Sprintf("%v smallint%v", column.Name(), terminator), nil
}

func intCreateWriter(column *sql.ColumnType, terminator string) (string, error) {
	return fmt.Sprintf("%v int%v", column.Name(), terminator), nil
}

func bigIntCreateWriter(column *sql.ColumnType, terminator string) (string, error) {
	return fmt.Sprintf("%v bigint%v", column.Name(), terminator), nil
}

func doubleCreateWriter(column *sql.ColumnType, terminator string) (string, error) {
	return fmt.Sprintf("%v double precision%v", column.Name(), terminator), nil
}

func timestampCreateWriter(column *sql.ColumnType, terminator string) (string, error) {
	return fmt.Sprintf("%v timestamp%v", column.Name(), terminator), nil
}

func timeCreateWriter(column *sql.ColumnType, terminator string) (string, error) {
	return fmt.Sprintf("%v time%v", column.Name(), terminator), nil
}

func dateCreateWriter(column *sql.ColumnType, terminator string) (string, error) {
	return fmt.Sprintf("%v date%v", column.Name(), terminator), nil
}

func byteaCreateWriter(column *sql.ColumnType, terminator string) (string, error) {
	return fmt.Sprintf("%v bytea%v", column.Name(), terminator), nil
}

func boolCreateWriter(column *sql.ColumnType, terminator string) (string, error) {
	return fmt.Sprintf("%v bool%v", column.Name(), terminator), nil
}

func uuidCreateWriter(column *sql.ColumnType, terminator string) (string, error) {
	return fmt.Sprintf("%v uuid%v", column.Name(), terminator), nil
}

func xmlCreateWriter(column *sql.ColumnType, terminator string) (string, error) {
	return fmt.Sprintf("%v xml%v", column.Name(), terminator), nil
}

var ValWriters = map[string]map[string]func(value interface{}, terminator string, nullString string) (string, error){
	"postgresql": {
		"SQL_UNKNOWN_TYPE":    printRaw,
		"SQL_CHAR":            printRaw,
		"SQL_NUMERIC":         printRaw,
		"SQL_DECIMAL":         printRaw,
		"SQL_INTEGER":         printRaw,
		"SQL_SMALLINT":        printRaw,
		"SQL_FLOAT":           printRaw,
		"SQL_REAL":            printRaw,
		"SQL_DOUBLE":          printRaw,
		"SQL_DATETIME":        printRaw,
		"SQL_TIME":            printRaw,
		"SQL_VARCHAR":         castToBytesCastToStringPrintQuoted,
		"SQL_TYPE_DATE":       castToTimeFormatToDateString,
		"SQL_TYPE_TIME":       castToTimeFormatToTimeString,
		"SQL_TYPE_TIMESTAMP":  castToTimeFormatToTimetampString,
		"SQL_TIMESTAMP":       printRaw,
		"SQL_LONGVARCHAR":     printRaw,
		"SQL_BINARY":          printRaw,
		"SQL_VARBINARY":       printRaw,
		"SQL_LONGVARBINARY":   castToBytesCastToStringPrintQuotedHex,
		"SQL_BIGINT":          printRaw,
		"SQL_TINYINT":         printRaw,
		"SQL_BIT":             castToBoolWriteBinaryEquivalent,
		"SQL_WCHAR":           castToBytesCastToStringPrintQuoted,
		"SQL_WVARCHAR":        castToBytesCastToStringPrintQuoted,
		"SQL_WLONGVARCHAR":    castToBytesCastToStringPrintQuoted,
		"SQL_GUID":            printRawQuoted,
		"SQL_SIGNED_OFFSET":   printRaw,
		"SQL_UNSIGNED_OFFSET": printRaw,
		"SQL_SS_XML":          printRaw,
		"SQL_SS_TIME2":        printRaw,
	},
}

func printRaw(value interface{}, terminator string, nullString string) (string, error) {
	if value == nil {
		return fmt.Sprintf("%v%v", nullString, terminator), nil
	}
	return fmt.Sprintf("%v%v", value, terminator), nil
}

func printRawQuoted(value interface{}, terminator string, nullString string) (string, error) {
	if value == nil {
		return fmt.Sprintf("%v%v", nullString, terminator), nil
	}
	return fmt.Sprintf("'%v'%v", value, terminator), nil
}

func castToBoolWriteBinaryEquivalent(value interface{}, terminator string, nullString string) (string, error) {
	if value == nil {
		return fmt.Sprintf("%v%v", nullString, terminator), nil
	}
	valBool, ok := value.(bool)
	if !ok {
		return "", errors.New("castToBool unable to cast value to bool")
	}

	return fmt.Sprintf("%v%v", valBool, terminator), nil
}

func castToBytesCastToStringPrintQuoted(value interface{}, terminator string, nullString string) (string, error) {
	if value == nil {
		return fmt.Sprintf("%v%v", nullString, terminator), nil
	}
	valBytes, ok := value.([]byte)
	if !ok {
		return "", errors.New("castToBytesCastToStringPrintQuoted unable to cast value to bytes")
	}
	valString := string(valBytes)
	escaped := dbValReplacer.Replace(valString)
	return fmt.Sprintf("'%v'%v", escaped, terminator), nil
}

func castToTimeFormatToDateString(value interface{}, terminator string, nullString string) (string, error) {
	if value == nil {
		return fmt.Sprintf("%v%v", nullString, terminator), nil
	}
	valTime, ok := value.(time.Time)
	if !ok {
		return "", errors.New("castToTimeFormatToDateString unable to cast value to bytes")
	}
	return fmt.Sprintf("'%v'%v", valTime.Format("2006/01/02"), terminator), nil
}

func castToTimeFormatToTimeString(value interface{}, terminator string, nullString string) (string, error) {
	if value == nil {
		return fmt.Sprintf("%v%v", nullString, terminator), nil
	}
	valTime, ok := value.(time.Time)
	if !ok {
		return "", errors.New("castToTimeFormatToTimeString unable to cast value to bytes")
	}
	return fmt.Sprintf("'%v'%v", valTime.Format("15:04:05.999999999"), terminator), nil
}

func castToTimeFormatToTimetampString(value interface{}, terminator string, nullString string) (string, error) {
	if value == nil {
		return fmt.Sprintf("%v%v", nullString, terminator), nil
	}
	valTime, ok := value.(time.Time)
	if !ok {
		return "", errors.New("castToTimeFormatToTimetampString unable to cast value to bytes")
	}
	return fmt.Sprintf("'%v'%v", valTime.Format(time.RFC3339Nano), terminator), nil
}

func castToBytesCastToStringPrintQuotedHex(value interface{}, terminator string, nullString string) (string, error) {
	if value == nil {
		return fmt.Sprintf("%v%v", nullString, terminator), nil
	}
	valBytes, ok := value.([]byte)
	if !ok {
		return "", errors.New("castToBytesCastToStringPrintQuotedHex unable to cast value to bytes")
	}
	return fmt.Sprintf("'%x'%v", string(valBytes), terminator), nil
}
