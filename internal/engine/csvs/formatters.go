package csvs

import (
	"errors"
	"fmt"
	"time"
)

var formatters = map[string]func(value interface{}) (string, error){
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
	`SQL_VARCHAR`:         csvPrintRaw,
	`SQL_TYPE_DATE`:       csvCastToTimeFormatToDateString,
	`SQL_TYPE_TIME`:       csvCastToTimeFormatToTimeString,
	`SQL_TYPE_TIMESTAMP`:  csvCastToTimeFormatToTimetampString,
	`SQL_TIMESTAMP`:       csvPrintRaw,
	`SQL_LONGVARCHAR`:     csvPrintRaw,
	`SQL_BINARY`:          csvPrintRaw,
	`SQL_VARBINARY`:       csvPrintRaw,
	`SQL_LONGVARBINARY`:   csvCastToBytesCastToString,
	`SQL_BIGINT`:          csvPrintRaw,
	`SQL_TINYINT`:         csvPrintRaw,
	`SQL_BIT`:             csvCastToBoolWriteBinaryEquivalent,
	`SQL_WCHAR`:           csvCastToBytesCastToStringEscape,
	`SQL_WVARCHAR`:        csvCastToBytesCastToStringEscape,
	`SQL_WLONGVARCHAR`:    csvCastToBytesCastToStringEscape,
	`SQL_GUID`:            csvPrintRaw,
	`SQL_SIGNED_OFFSET`:   csvPrintRaw,
	`SQL_UNSIGNED_OFFSET`: csvPrintRaw,
	`SQL_SS_XML`:          csvPrintRaw,
	`SQL_SS_TIME2`:        csvPrintRaw,
}

func csvPrintRaw(value interface{}) (string, error) {
	if value == nil {
		return "", nil
	}
	return fmt.Sprintf(`%v`, value), nil
}

func csvCastToBoolWriteBinaryEquivalent(value interface{}) (string, error) {
	if value == nil {
		return "", nil
	}
	valBool, ok := value.(bool)
	if !ok {
		return ``, errors.New(`castToBool unable to cast value to bool`)
	}

	var valToReturn string

	switch valBool {
	case true:
		valToReturn = "1"
	case false:
		valToReturn = "0"
	}

	return valToReturn, nil
}

func csvCastToBytesCastToStringEscape(value interface{}) (string, error) {
	if value == nil {
		return "", nil
	}
	valBytes, ok := value.([]byte)
	if !ok {
		return ``, errors.New(`csvCastToBytesCastToStringEscape unable to cast value to bytes`)
	}
	return fmt.Sprintf(`%v`, string(valBytes)), nil
}

func csvCastToTimeFormatToDateString(value interface{}) (string, error) {
	if value == nil {
		return "", nil
	}
	valTime, ok := value.(time.Time)
	if !ok {
		return ``, errors.New(`castToTimeFormatToDateString unable to cast value to bytes`)
	}
	return fmt.Sprintf(`%v`, valTime.Format(`2006/01/02`)), nil
}

func csvCastToTimeFormatToTimeString(value interface{}) (string, error) {
	if value == nil {
		return "", nil
	}
	valTime, ok := value.(time.Time)
	if !ok {
		return ``, errors.New(`castToTimeFormatToTimeString unable to cast value to bytes`)
	}
	return fmt.Sprintf(`"%v"`, valTime.Format(`15:04:05.999999999`)), nil
}

func csvCastToTimeFormatToTimetampString(value interface{}) (string, error) {
	if value == nil {
		return "", nil
	}
	valTime, ok := value.(time.Time)
	if !ok {
		return ``, errors.New(`castToTimeFormatToTimetampString unable to cast value to bytes`)
	}
	return fmt.Sprintf(`%v`, valTime.Format(time.RFC3339Nano)), nil
}

func csvCastToBytesCastToString(value interface{}) (string, error) {
	if value == nil {
		return "", nil
	}
	valBytes, ok := value.([]byte)
	if !ok {
		return ``, errors.New(`csvCastToBytesCastToString unable to cast value to bytes`)
	}
	return fmt.Sprintf(`%x`, string(valBytes)), nil
}
