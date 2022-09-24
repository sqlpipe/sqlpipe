package writers

import (
	"errors"
	"fmt"
	"strings"
)

var GeneralCreateWriters = map[string]func(value interface{}, terminator string) (string, error){
	"SQL_UNKNOWN_TYPE":    generalQuotedTextCreateWriter,
	"SQL_CHAR":            generalQuotedTextCreateWriter,
	"SQL_NUMERIC":         generalQuotedTextCreateWriter,
	"SQL_DECIMAL":         generalQuotedTextCreateWriter,
	"SQL_INTEGER":         generalQuotedTextCreateWriter,
	"SQL_SMALLINT":        generalQuotedTextCreateWriter,
	"SQL_FLOAT":           generalQuotedTextCreateWriter,
	"SQL_REAL":            generalQuotedTextCreateWriter,
	"SQL_DOUBLE":          generalQuotedTextCreateWriter,
	"SQL_DATETIME":        generalQuotedTextCreateWriter,
	"SQL_TIME":            generalQuotedTextCreateWriter,
	"SQL_VARCHAR":         generalQuotedTextCreateWriter,
	"SQL_TYPE_DATE":       generalQuotedTextCreateWriter,
	"SQL_TYPE_TIME":       generalQuotedTextCreateWriter,
	"SQL_TYPE_TIMESTAMP":  generalQuotedTextCreateWriter,
	"SQL_TIMESTAMP":       generalQuotedTextCreateWriter,
	"SQL_LONGVARCHAR":     generalQuotedTextCreateWriter,
	"SQL_BINARY":          generalQuotedTextCreateWriter,
	"SQL_VARBINARY":       generalQuotedTextCreateWriter,
	"SQL_LONGVARBINARY":   generalQuotedTextCreateWriter,
	"SQL_BIGINT":          generalQuotedTextCreateWriter,
	"SQL_TINYINT":         generalQuotedTextCreateWriter,
	"SQL_BIT":             generalQuotedTextCreateWriter,
	"SQL_WCHAR":           generalQuotedTextCreateWriter,
	"SQL_WVARCHAR":        generalQuotedTextCreateWriter,
	"SQL_WLONGVARCHAR":    generalQuotedTextCreateWriter,
	"SQL_GUID":            generalQuotedTextCreateWriter,
	"SQL_SIGNED_OFFSET":   generalQuotedTextCreateWriter,
	"SQL_UNSIGNED_OFFSET": generalQuotedTextCreateWriter,
	"SQL_SS_XML":          generalQuotedTextCreateWriter,
	"SQL_SS_TIME2":        generalQuotedTextCreateWriter,
}

func generalQuotedTextCreateWriter(value interface{}, terminator string) (string, error) {
	return fmt.Sprintf(`%v text%v`, value, terminator), nil
}

var GeneralValWriters = map[string]func(value interface{}, terminator string) (string, error){
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
	"SQL_TYPE_DATE":       printRaw,
	"SQL_TYPE_TIME":       printRaw,
	"SQL_TYPE_TIMESTAMP":  printRaw,
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
	"SQL_GUID":            printRaw,
	"SQL_SIGNED_OFFSET":   printRaw,
	"SQL_UNSIGNED_OFFSET": printRaw,
	"SQL_SS_XML":          printRaw,
	"SQL_SS_TIME2":        printRaw,
}

func printRaw(value interface{}, terminator string) (string, error) {
	if value == nil {
		return fmt.Sprintf("%v", terminator), nil
	}
	return fmt.Sprintf("%v%v", value, terminator), nil
}

func castToBoolWriteBinaryEquivalent(value interface{}, terminator string) (string, error) {
	if value == nil {
		return fmt.Sprintf("%v", terminator), nil
	}
	valBool, ok := value.(bool)
	if !ok {
		return "", errors.New("castToBool unable to cast value to bool")
	}

	var valToReturn string

	switch valBool {
	case true:
		valToReturn = fmt.Sprintf(`1%v`, terminator)
	case false:
		valToReturn = fmt.Sprintf(`0%v`, terminator)
	}

	return valToReturn, nil
}

func castToBytesCastToStringPrintQuoted(value interface{}, terminator string) (string, error) {
	if value == nil {
		return fmt.Sprintf("%v", terminator), nil
	}
	valBytes, ok := value.([]byte)
	if !ok {
		return "", errors.New("castToBytesCastToStringPrintQuoted unable to cast value to bytes")
	}
	valString := string(valBytes)
	quotesEscaped := strings.ReplaceAll(valString, `"`, `""`)
	return fmt.Sprintf(`"%v"%v`, quotesEscaped, terminator), nil
}

func castToBytesCastToStringPrintQuotedHex(value interface{}, terminator string) (string, error) {
	if value == nil {
		return fmt.Sprintf("%v", terminator), nil
	}
	valBytes, ok := value.([]byte)
	if !ok {
		return "", errors.New("castToBytesCastToStringPrintQuotedHex unable to cast value to bytes")
	}
	return fmt.Sprintf(`"%x"%v`, string(valBytes), terminator), nil
}
