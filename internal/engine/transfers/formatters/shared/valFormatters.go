package shared

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

var dbValReplacer = strings.NewReplacer(`'`, `''`, "{", "(", "}", ")")

func RawXcommaXnull(value interface{}) (string, error) {
	if value == nil {
		return "null,", nil
	}
	return fmt.Sprintf("%v,", value), nil
}

func RawXparenthesisXnull(value interface{}) (string, error) {
	if value == nil {
		return "null)", nil
	}
	return fmt.Sprintf("%v)", value), nil
}

func QuotedXcommaXnull(value interface{}) (string, error) {
	if value == nil {
		return "null,", nil
	}
	return fmt.Sprintf("'%v',", value), nil
}

func QuotedXparenthesisXnull(value interface{}) (string, error) {
	if value == nil {
		return "null)", nil
	}
	return fmt.Sprintf("'%v')", value), nil
}

func CastToBoolWriteBinaryEquivalentXcommaXnull(value interface{}) (string, error) {
	if value == nil {
		return "null,", nil
	}
	valBool, ok := value.(bool)
	if !ok {
		return "", errors.New("castToBool unable to cast value to bool")
	}

	return fmt.Sprintf("%v,", valBool), nil
}

func CastToBoolWriteBinaryEquivalentXparenthesisXnull(value interface{}) (string, error) {
	if value == nil {
		return "null)", nil
	}
	valBool, ok := value.(bool)
	if !ok {
		return "", errors.New("castToBool unable to cast value to bool")
	}

	return fmt.Sprintf("%v)", valBool), nil
}

func CastToBytesCastToStringPrintQuotedXcommaXnull(value interface{}) (string, error) {
	if value == nil {
		return "null,", nil
	}
	valBytes, ok := value.([]byte)
	if !ok {
		return "", errors.New("castToBytesCastToStringPrintQuoted unable to cast value to bytes")
	}
	valString := string(valBytes)
	escaped := dbValReplacer.Replace(valString)
	return fmt.Sprintf("'%v',", escaped), nil
}

func CastToBytesCastToStringPrintQuotedXparenthesisXnull(value interface{}) (string, error) {
	if value == nil {
		return "null)", nil
	}
	valBytes, ok := value.([]byte)
	if !ok {
		return "", errors.New("castToBytesCastToStringPrintQuoted unable to cast value to bytes")
	}
	valString := string(valBytes)
	escaped := dbValReplacer.Replace(valString)
	return fmt.Sprintf("'%v')", escaped), nil
}

func CastToTimeFormatToDateStringXcommaXnull(value interface{}) (string, error) {
	if value == nil {
		return "null,", nil
	}
	valTime, ok := value.(time.Time)
	if !ok {
		return "", errors.New("castToTimeFormatToDateString unable to cast value to bytes")
	}
	return fmt.Sprintf("'%v',", valTime.Format("2006/01/02")), nil
}

func CastToTimeFormatToDateStringXparenthesisXnull(value interface{}) (string, error) {
	if value == nil {
		return "null)", nil
	}
	valTime, ok := value.(time.Time)
	if !ok {
		return "", errors.New("castToTimeFormatToDateString unable to cast value to bytes")
	}
	return fmt.Sprintf("'%v')", valTime.Format("2006/01/02")), nil
}

func CastToTimeFormatToTimeStringXcommaXnull(value interface{}) (string, error) {
	if value == nil {
		return "null,", nil
	}
	valTime, ok := value.(time.Time)
	if !ok {
		return "", errors.New("castToTimeFormatToTimeString unable to cast value to bytes")
	}
	return fmt.Sprintf("'%v',", valTime.Format("15:04:05.999999999")), nil
}

func CastToTimeFormatToTimeStringXparenthesisXnull(value interface{}) (string, error) {
	if value == nil {
		return "null)", nil
	}
	valTime, ok := value.(time.Time)
	if !ok {
		return "", errors.New("castToTimeFormatToTimeString unable to cast value to bytes")
	}
	return fmt.Sprintf("'%v')", valTime.Format("15:04:05.999999999")), nil
}

func CastToTimeFormatToTimetampStringXcommaXnull(value interface{}) (string, error) {
	if value == nil {
		return "null,", nil
	}
	valTime, ok := value.(time.Time)
	if !ok {
		return "", errors.New("castToTimeFormatToTimetampString unable to cast value to bytes")
	}
	return fmt.Sprintf("'%v',", valTime.Format(time.RFC3339Nano)), nil
}

func CastToTimeFormatToTimetampStringXparenthesisXnull(value interface{}) (string, error) {
	if value == nil {
		return "null)", nil
	}
	valTime, ok := value.(time.Time)
	if !ok {
		return "", errors.New("castToTimeFormatToTimetampString unable to cast value to bytes")
	}
	return fmt.Sprintf("'%v')", valTime.Format(time.RFC3339Nano)), nil
}

func CastToBytesCastToStringPrintQuotedHexXcommaXnull(value interface{}) (string, error) {
	if value == nil {
		return "null,", nil
	}
	valBytes, ok := value.([]byte)
	if !ok {
		return "", errors.New("castToBytesCastToStringPrintQuotedHex unable to cast value to bytes")
	}
	return fmt.Sprintf("'%x',", string(valBytes)), nil
}

func CastToBytesCastToStringPrintQuotedHexXparenthesisXnull(value interface{}) (string, error) {
	if value == nil {
		return "null)", nil
	}
	valBytes, ok := value.([]byte)
	if !ok {
		return "", errors.New("castToBytesCastToStringPrintQuotedHex unable to cast value to bytes")
	}
	return fmt.Sprintf("'%x')", string(valBytes)), nil
}
