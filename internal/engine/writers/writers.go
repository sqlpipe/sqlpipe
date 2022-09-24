package writers

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

var CreateWriters = map[string]map[string]func(value interface{}, terminator string) (string, error){
	"default": {
		"SQL_UNKNOWN_TYPE":    quotedTextCreateWriter,
		"SQL_CHAR":            quotedTextCreateWriter,
		"SQL_NUMERIC":         quotedTextCreateWriter,
		"SQL_DECIMAL":         quotedTextCreateWriter,
		"SQL_INTEGER":         quotedTextCreateWriter,
		"SQL_SMALLINT":        quotedTextCreateWriter,
		"SQL_FLOAT":           quotedTextCreateWriter,
		"SQL_REAL":            quotedTextCreateWriter,
		"SQL_DOUBLE":          quotedTextCreateWriter,
		"SQL_DATETIME":        quotedTextCreateWriter,
		"SQL_TIME":            quotedTextCreateWriter,
		"SQL_VARCHAR":         quotedTextCreateWriter,
		"SQL_TYPE_DATE":       quotedTextCreateWriter,
		"SQL_TYPE_TIME":       quotedTextCreateWriter,
		"SQL_TYPE_TIMESTAMP":  quotedTextCreateWriter,
		"SQL_TIMESTAMP":       quotedTextCreateWriter,
		"SQL_LONGVARCHAR":     quotedTextCreateWriter,
		"SQL_BINARY":          quotedTextCreateWriter,
		"SQL_VARBINARY":       quotedTextCreateWriter,
		"SQL_LONGVARBINARY":   quotedTextCreateWriter,
		"SQL_BIGINT":          quotedTextCreateWriter,
		"SQL_TINYINT":         quotedTextCreateWriter,
		"SQL_BIT":             quotedTextCreateWriter,
		"SQL_WCHAR":           quotedTextCreateWriter,
		"SQL_WVARCHAR":        quotedTextCreateWriter,
		"SQL_WLONGVARCHAR":    quotedTextCreateWriter,
		"SQL_GUID":            quotedTextCreateWriter,
		"SQL_SIGNED_OFFSET":   quotedTextCreateWriter,
		"SQL_UNSIGNED_OFFSET": quotedTextCreateWriter,
		"SQL_SS_XML":          quotedTextCreateWriter,
		"SQL_SS_TIME2":        quotedTextCreateWriter,
	},
}

func quotedTextCreateWriter(value interface{}, terminator string) (string, error) {
	return fmt.Sprintf("%v text%v", value, terminator), nil
}

var ValWriters = map[string]map[string]func(value interface{}, terminator string, nullString string) (string, error){
	"default": {
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

	var valToReturn string

	switch valBool {
	case true:
		valToReturn = fmt.Sprintf("1%v", terminator)
	case false:
		valToReturn = fmt.Sprintf("0%v", terminator)
	}

	return valToReturn, nil
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
	quotesEscaped := strings.ReplaceAll(valString, "'", "''")
	quotesEscaped = strings.ReplaceAll(quotesEscaped, "{", "(")
	quotesEscaped = strings.ReplaceAll(quotesEscaped, "}", ")")
	return fmt.Sprintf("'%v'%v", quotesEscaped, terminator), nil
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
