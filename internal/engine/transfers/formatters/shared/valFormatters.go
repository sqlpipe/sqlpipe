package shared

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

var dbValReplacer = strings.NewReplacer(`'`, `''`, "{", "(", "}", ")")

func RawXnull(value interface{}, terminator string) (formattedValue string, err error) {
	if value == nil {
		return fmt.Sprintf("null%v", terminator), nil
	}
	return fmt.Sprintf("%v%v", value, terminator), nil
}

func QuotedXnull(value interface{}, terminator string) (formattedValue string, err error) {
	if value == nil {
		return fmt.Sprintf("null%v", terminator), nil
	}
	return fmt.Sprintf("'%v'%v", value, terminator), nil
}

func CastToBoolWriteTextEquivalentXnull(value interface{}, terminator string) (formattedValue string, err error) {
	if value == nil {
		return fmt.Sprintf("null%v", terminator), nil
	}
	valBool, ok := value.(bool)
	if !ok {
		return "", errors.New("CastToBoolWriteTextEquivalentXnull unable to cast value to bool")
	}

	return fmt.Sprintf("%v%v", valBool, terminator), nil
}

func CastToBoolWriteBinaryEquivalentXnull(value interface{}, terminator string) (formattedValue string, err error) {
	if value == nil {
		return fmt.Sprintf("null%v", terminator), nil
	}
	valBool, ok := value.(bool)
	if !ok {
		return "", errors.New("CastToBoolWriteBinaryEquivalentXnull unable to cast value to bool")
	}

	if valBool {
		return fmt.Sprintf("1%v", terminator), nil
	} else {
		return fmt.Sprintf("0%v", terminator), nil
	}
}

func CastToBytesCastToStringPrintQuotedXnull(value interface{}, terminator string) (formattedValue string, err error) {
	if value == nil {
		return fmt.Sprintf("null%v", terminator), nil
	}
	valBytes, ok := value.([]byte)
	if !ok {
		return "", errors.New("CastToBytesCastToStringPrintQuotedXnull unable to cast value to bytes")
	}
	escaped := dbValReplacer.Replace(string(valBytes))
	return fmt.Sprintf("'%v'%v", escaped, terminator), nil
}

func CastToTimeFormatToDateStringXnull(value interface{}, terminator string) (formattedValue string, err error) {
	if value == nil {
		return fmt.Sprintf("null%v", terminator), nil
	}
	valTime, ok := value.(time.Time)
	if !ok {
		return "", errors.New("CastToTimeFormatToDateStringXnull unable to cast value to bytes")
	}
	return fmt.Sprintf("'%v'%v", valTime.Format("2006/01/02"), terminator), nil
}

func CastToTimeFormatToTimeStringXnull(value interface{}, terminator string) (formattedValue string, err error) {
	if value == nil {
		return fmt.Sprintf("null%v", terminator), nil
	}
	valTime, ok := value.(time.Time)
	if !ok {
		return "", errors.New("CastToTimeFormatToTimeStringXnull unable to cast value to bytes")
	}
	return fmt.Sprintf("'%v'%v", valTime.Format("15:04:05.999999999"), terminator), nil
}

func CastToTimeFormatToTimetampStringXnull(value interface{}, terminator string) (formattedValue string, err error) {
	if value == nil {
		return fmt.Sprintf("null%v", terminator), nil
	}
	valTime, ok := value.(time.Time)
	if !ok {
		return "", errors.New("CastToTimeFormatToTimetampStringXnull unable to cast value to bytes")
	}
	return fmt.Sprintf("'%v'%v", valTime.Format(time.RFC3339Nano), terminator), nil
}

func CastToBytesCastToStringPrintQuotedHexXnull(value interface{}, terminator string) (formattedValue string, err error) {
	if value == nil {
		return fmt.Sprintf("null%v", terminator), nil
	}
	valBytes, ok := value.([]byte)
	if !ok {
		return "", errors.New("CastToBytesCastToStringPrintQuotedHexXnull unable to cast value to bytes")
	}
	return fmt.Sprintf("'%x'%v", string(valBytes), terminator), nil
}
