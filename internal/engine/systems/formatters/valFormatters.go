package formatters

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

var dbValReplacer = strings.NewReplacer(`'`, `''`, "{", "(", "}", ")")

func Raw(value interface{}, terminator string, nullString string) (string, error) {
	if value == nil {
		return fmt.Sprintf("%v%v", nullString, terminator), nil
	}
	return fmt.Sprintf("%v%v", value, terminator), nil
}

func Quoted(value interface{}, terminator string, nullString string) (string, error) {
	if value == nil {
		return fmt.Sprintf("%v%v", nullString, terminator), nil
	}
	return fmt.Sprintf("'%v'%v", value, terminator), nil
}

func CastToBoolWriteBinaryEquivalent(value interface{}, terminator string, nullString string) (string, error) {
	if value == nil {
		return fmt.Sprintf("%v%v", nullString, terminator), nil
	}
	valBool, ok := value.(bool)
	if !ok {
		return "", errors.New("castToBool unable to cast value to bool")
	}

	return fmt.Sprintf("%v%v", valBool, terminator), nil
}

func CastToBytesCastToStringPrintQuoted(value interface{}, terminator string, nullString string) (string, error) {
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

func CastToTimeFormatToDateString(value interface{}, terminator string, nullString string) (string, error) {
	if value == nil {
		return fmt.Sprintf("%v%v", nullString, terminator), nil
	}
	valTime, ok := value.(time.Time)
	if !ok {
		return "", errors.New("castToTimeFormatToDateString unable to cast value to bytes")
	}
	return fmt.Sprintf("'%v'%v", valTime.Format("2006/01/02"), terminator), nil
}

func CastToTimeFormatToTimeString(value interface{}, terminator string, nullString string) (string, error) {
	if value == nil {
		return fmt.Sprintf("%v%v", nullString, terminator), nil
	}
	valTime, ok := value.(time.Time)
	if !ok {
		return "", errors.New("castToTimeFormatToTimeString unable to cast value to bytes")
	}
	return fmt.Sprintf("'%v'%v", valTime.Format("15:04:05.999999999"), terminator), nil
}

func CastToTimeFormatToTimetampString(value interface{}, terminator string, nullString string) (string, error) {
	if value == nil {
		return fmt.Sprintf("%v%v", nullString, terminator), nil
	}
	valTime, ok := value.(time.Time)
	if !ok {
		return "", errors.New("castToTimeFormatToTimetampString unable to cast value to bytes")
	}
	return fmt.Sprintf("'%v'%v", valTime.Format(time.RFC3339Nano), terminator), nil
}

func CastToBytesCastToStringPrintQuotedHex(value interface{}, terminator string, nullString string) (string, error) {
	if value == nil {
		return fmt.Sprintf("%v%v", nullString, terminator), nil
	}
	valBytes, ok := value.([]byte)
	if !ok {
		return "", errors.New("castToBytesCastToStringPrintQuotedHex unable to cast value to bytes")
	}
	return fmt.Sprintf("'%x'%v", string(valBytes), terminator), nil
}
