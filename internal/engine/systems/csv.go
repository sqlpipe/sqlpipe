package systems

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

var csvValReplacer = strings.NewReplacer(`"`, `""`)

var CsvValWriters = map[string]func(value interface{}, terminator string, nullString string) (string, error){
	`SQL_UNKNOWN_TYPE`:    csvPrintRaw,
	`SQL_CHAR`:            csvPrintRaw,
	`SQL_NUMERIC`:         csvPrintRaw,
	`SQL_DECIMAL`:         csvPrintRaw,
	`SQL_INTEGER`:         csvPrintRaw,
	`SQL_SMALLINT`:        csvPrintRaw,
	`SQL_FLOAT`:           csvPrintRaw,
	`SQL_REAL`:            csvPrintRaw,
	`SQL_DOUBLE`:          csvPrintRaw,
	`SQL_DATETIME`:        csvPrintRaw,
	`SQL_TIME`:            csvPrintRaw,
	`SQL_VARCHAR`:         csvCastToBytesCastToStringPrintQuoted,
	`SQL_TYPE_DATE`:       csvCastToTimeFormatToDateString,
	`SQL_TYPE_TIME`:       csvCastToTimeFormatToTimeString,
	`SQL_TYPE_TIMESTAMP`:  csvCastToTimeFormatToTimetampString,
	`SQL_TIMESTAMP`:       csvPrintRaw,
	`SQL_LONGVARCHAR`:     csvPrintRaw,
	`SQL_BINARY`:          csvPrintRaw,
	`SQL_VARBINARY`:       csvPrintRaw,
	`SQL_LONGVARBINARY`:   csvCastToBytesCastToStringPrintQuotedHex,
	`SQL_BIGINT`:          csvPrintRaw,
	`SQL_TINYINT`:         csvPrintRaw,
	`SQL_BIT`:             csvCastToBoolWriteBinaryEquivalent,
	`SQL_WCHAR`:           csvCastToBytesCastToStringPrintQuoted,
	`SQL_WVARCHAR`:        csvCastToBytesCastToStringPrintQuoted,
	`SQL_WLONGVARCHAR`:    csvCastToBytesCastToStringPrintQuoted,
	`SQL_GUID`:            csvPrintRawQuoted,
	`SQL_SIGNED_OFFSET`:   csvPrintRaw,
	`SQL_UNSIGNED_OFFSET`: csvPrintRaw,
	`SQL_SS_XML`:          csvPrintRaw,
	`SQL_SS_TIME2`:        csvPrintRaw,
}

func csvPrintRaw(value interface{}, terminator string, nullString string) (string, error) {
	if value == nil {
		return fmt.Sprintf(`%v%v`, nullString, terminator), nil
	}
	return fmt.Sprintf(`%v%v`, value, terminator), nil
}

func csvPrintRawQuoted(value interface{}, terminator string, nullString string) (string, error) {
	if value == nil {
		return fmt.Sprintf(`%v%v`, nullString, terminator), nil
	}
	return fmt.Sprintf(`"%v"%v`, value, terminator), nil
}

func csvCastToBoolWriteBinaryEquivalent(value interface{}, terminator string, nullString string) (string, error) {
	if value == nil {
		return fmt.Sprintf(`%v%v`, nullString, terminator), nil
	}
	valBool, ok := value.(bool)
	if !ok {
		return ``, errors.New(`castToBool unable to cast value to bool`)
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

func csvCastToBytesCastToStringPrintQuoted(value interface{}, terminator string, nullString string) (string, error) {
	if value == nil {
		return fmt.Sprintf(`%v%v`, nullString, terminator), nil
	}
	valBytes, ok := value.([]byte)
	if !ok {
		return ``, errors.New(`castToBytesCastToStringPrintQuoted unable to cast value to bytes`)
	}
	valString := string(valBytes)
	escaped := csvValReplacer.Replace(valString)
	return fmt.Sprintf(`"%v"%v`, escaped, terminator), nil
}

func csvCastToTimeFormatToDateString(value interface{}, terminator string, nullString string) (string, error) {
	if value == nil {
		return fmt.Sprintf(`%v%v`, nullString, terminator), nil
	}
	valTime, ok := value.(time.Time)
	if !ok {
		return ``, errors.New(`castToTimeFormatToDateString unable to cast value to bytes`)
	}
	return fmt.Sprintf(`"%v"%v`, valTime.Format(`2006/01/02`), terminator), nil
}

func csvCastToTimeFormatToTimeString(value interface{}, terminator string, nullString string) (string, error) {
	if value == nil {
		return fmt.Sprintf(`%v%v`, nullString, terminator), nil
	}
	valTime, ok := value.(time.Time)
	if !ok {
		return ``, errors.New(`castToTimeFormatToTimeString unable to cast value to bytes`)
	}
	return fmt.Sprintf(`"%v"%v`, valTime.Format(`15:04:05.999999999`), terminator), nil
}

func csvCastToTimeFormatToTimetampString(value interface{}, terminator string, nullString string) (string, error) {
	if value == nil {
		return fmt.Sprintf(`%v%v`, nullString, terminator), nil
	}
	valTime, ok := value.(time.Time)
	if !ok {
		return ``, errors.New(`castToTimeFormatToTimetampString unable to cast value to bytes`)
	}
	return fmt.Sprintf(`"%v"%v`, valTime.Format(time.RFC3339Nano), terminator), nil
}

func csvCastToBytesCastToStringPrintQuotedHex(value interface{}, terminator string, nullString string) (string, error) {
	if value == nil {
		return fmt.Sprintf(`%v%v`, nullString, terminator), nil
	}
	valBytes, ok := value.([]byte)
	if !ok {
		return ``, errors.New(`castToBytesCastToStringPrintQuotedHex unable to cast value to bytes`)
	}
	return fmt.Sprintf(`"%x"%v`, string(valBytes), terminator), nil
}
