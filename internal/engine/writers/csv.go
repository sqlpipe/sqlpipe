package writers

import "fmt"

var CsvValWriters = map[string]func(value interface{}, terminator string) (string, error){
	"SQL_UNKNOWN_TYPE":    csvQuotedTextValWriter,
	"SQL_CHAR":            csvQuotedTextValWriter,
	"SQL_NUMERIC":         csvQuotedTextValWriter,
	"SQL_DECIMAL":         csvQuotedTextValWriter,
	"SQL_INTEGER":         csvQuotedTextValWriter,
	"SQL_SMALLINT":        csvQuotedTextValWriter,
	"SQL_FLOAT":           csvQuotedTextValWriter,
	"SQL_REAL":            csvQuotedTextValWriter,
	"SQL_DOUBLE":          csvQuotedTextValWriter,
	"SQL_DATETIME":        csvQuotedTextValWriter,
	"SQL_TIME":            csvQuotedTextValWriter,
	"SQL_VARCHAR":         csvQuotedTextValWriter,
	"SQL_TYPE_DATE":       csvQuotedTextValWriter,
	"SQL_TYPE_TIME":       csvQuotedTextValWriter,
	"SQL_TYPE_TIMESTAMP":  csvQuotedTextValWriter,
	"SQL_TIMESTAMP":       csvQuotedTextValWriter,
	"SQL_LONGVARCHAR":     csvQuotedTextValWriter,
	"SQL_BINARY":          csvQuotedTextValWriter,
	"SQL_VARBINARY":       csvQuotedTextValWriter,
	"SQL_LONGVARBINARY":   csvQuotedTextValWriter,
	"SQL_BIGINT":          csvQuotedTextValWriter,
	"SQL_TINYINT":         csvQuotedTextValWriter,
	"SQL_BIT":             csvQuotedTextValWriter,
	"SQL_WCHAR":           csvQuotedTextValWriter,
	"SQL_WVARCHAR":        csvQuotedTextValWriter,
	"SQL_WLONGVARCHAR":    csvQuotedTextValWriter,
	"SQL_GUID":            csvQuotedTextValWriter,
	"SQL_SIGNED_OFFSET":   csvQuotedTextValWriter,
	"SQL_UNSIGNED_OFFSET": csvQuotedTextValWriter,
	"SQL_SS_XML":          csvQuotedTextValWriter,
	"SQL_SS_TIME2":        csvQuotedTextValWriter,
}

func csvQuotedTextValWriter(value interface{}, terminator string) (string, error) {
	if value == nil {
		return fmt.Sprintf("%v", terminator), nil
	}
	return fmt.Sprintf(`"%v"%v`, value, terminator), nil
}
