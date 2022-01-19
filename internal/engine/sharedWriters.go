package engine

import (
	"fmt"
	"strings"
)

var generalReplacer = strings.NewReplacer(`"`, `""`, "\n", "")
var turboEndStringNilReplacer = strings.NewReplacer(
	`"%!s(<nil>)"`, "",
	`%!s(<nil>)`, "",
	`%!x(<nil>)`, "",
	`%!d(<nil>)`, "",
	`%!t(<nil>)`, "",
	`%!v(<nil>)`, "",
	`%!g(<nil>)`, "",
)

var sqlEndStringNilReplacer = strings.NewReplacer(
	`'%!s(<nil>)'`, "null",
	`%!s(<nil>)`, "null",
	`%!d(<nil>)`, "null",
	`%!t(<nil>)`, "null",
	`'%!v(<nil>)'`, "null",
	`%!g(<nil>)`, "null",
	`'\x%!x(<nil>)'`, "null",
	`x'%!x(<nil>)'`, "null",
	`CONVERT(VARBINARY(8000), '0x%!x(<nil>)', 1)`, "null",
	`hextoraw('%!x(<nil>)')`, "null",
	`to_binary('%!x(<nil>)')`, "null",
	`'%!x(<nil>)'`, "null",
	`'%!b(<nil>)'`, "null",
	"'<nil>'", "null",
	"<nil>", "null",
)

// *******************
// ***TURBO WRITERS***
// *******************

func writeIntMidTurbo(value interface{}, builder *strings.Builder) {
	fmt.Fprintf(builder, `%d,`, value)
}

func writeIntEndTurbo(value interface{}, builder *strings.Builder) {
	fmt.Fprintf(builder, "%d\n", value)
}

func writeBoolMidTurbo(value interface{}, builder *strings.Builder) {
	fmt.Fprintf(builder, `%t,`, value)
}
func writeBoolEndTurbo(value interface{}, builder *strings.Builder) {
	fmt.Fprintf(builder, "%t\n", value)
}

func writeFloatMidTurbo(value interface{}, builder *strings.Builder) {
	fmt.Fprintf(builder, `%g,`, value)
}
func writeFloatEndTurbo(value interface{}, builder *strings.Builder) {
	fmt.Fprintf(builder, "%g\n", value)
}

func writeStringNoQuotesMidTurbo(value interface{}, builder *strings.Builder) {
	fmt.Fprintf(builder, `%s,`, value)
}

func writeStringNoQuotesEndTurbo(value interface{}, builder *strings.Builder) {
	fmt.Fprintf(builder, "%s\n", value)
}

func writeQuotedStringMidTurbo(value interface{}, builder *strings.Builder) {
	fmt.Fprintf(builder, `"%s",`, value)
}

func writeQuotedStringEndTurbo(value interface{}, builder *strings.Builder) {
	fmt.Fprintf(builder, "\"%s\"\n", value)
}

func writeEscapedQuotedStringMidTurbo(value interface{}, builder *strings.Builder) {
	fmt.Fprintf(builder, `"%s",`, generalReplacer.Replace(fmt.Sprintf("%s", value)))
}

func writeEscapedQuotedStringEndTurbo(value interface{}, builder *strings.Builder) {
	fmt.Fprintf(builder, "\"%s\"\n", generalReplacer.Replace(fmt.Sprintf("%s", value)))
}

func writeJSONMidTurbo(value interface{}, builder *strings.Builder) {
	fmt.Fprintf(builder, `"%s",`, generalReplacer.Replace(fmt.Sprintf("%s", value)))
}

func writeJSONEndTurbo(value interface{}, builder *strings.Builder) {
	fmt.Fprintf(builder, "\"%s\"\n", generalReplacer.Replace(fmt.Sprintf("%s", value)))
}

func writeMSSQLUniqueIdentifierMidTurbo(value interface{}, builder *strings.Builder) {
	// This is a really stupid fix but it works
	// https://github.com/denisenkom/go-mssqldb/issues/56
	// I guess the bits get shifted around in the first half of these bytes... whatever
	switch value := value.(type) {
	case []uint8:
		fmt.Fprintf(
			builder,
			"%X%X%X%X%X%X%X%X%X%X%X,",
			value[3],
			value[2],
			value[1],
			value[0],
			value[5],
			value[4],
			value[7],
			value[6],
			value[8],
			value[9],
			value[10:],
		)
	default:
		builder.WriteString(",")
	}
}

func writeMSSQLUniqueIdentifierEndTurbo(value interface{}, builder *strings.Builder) {
	// This is a really stupid fix but it works
	// https://github.com/denisenkom/go-mssqldb/issues/56
	// I guess the bits get shifted around in the first half of these bytes... whatever
	switch value := value.(type) {
	case []uint8:
		fmt.Fprintf(
			builder,
			"%X%X%X%X%X%X%X%X%X%X%X\n",
			value[3],
			value[2],
			value[1],
			value[0],
			value[5],
			value[4],
			value[7],
			value[6],
			value[8],
			value[9],
			value[10:],
		)
	default:
		builder.WriteString("\n")
	}
}

func oracleWriteNumberMidTurbo(value interface{}, builder *strings.Builder) {
	printedVal := fmt.Sprint(value)
	if printedVal != "<nil>" {
		fmt.Fprintf(builder, "%v,", value)
	} else {
		builder.WriteString(",")
	}
}

func oracleWriteNumberEndTurbo(value interface{}, builder *strings.Builder) {
	printedVal := fmt.Sprint(value)
	if printedVal != "<nil>" {
		fmt.Fprintf(builder, "%v\n", value)
	} else {
		builder.WriteString("\n")
	}
}

// **********************
// **********************
// **********************
// ***STANDARD WRITERS***
// **********************
// **********************
// **********************

func oracleWriteNumber(value interface{}, terminator string) string {
	return fmt.Sprintf("%v%s", value, terminator)
}

func writeInsertInt(value interface{}, terminator string) string {
	return fmt.Sprintf("%d%s", value, terminator)
}

func writeInsertStringNoEscape(value interface{}, terminator string) string {
	return fmt.Sprintf("'%s'%s", value, terminator)
}

func writeInsertBool(value interface{}, terminator string) string {
	return fmt.Sprintf("%t%s", value, terminator)
}

func writeInsertFloat(value interface{}, terminator string) string {
	return fmt.Sprintf("%g%s", value, terminator)
}

func writeInsertRawStringNoQuotes(value interface{}, terminator string) string {
	return fmt.Sprintf("%s%s", value, terminator)
}

func writeInsertEscapedString(value interface{}, terminator string) string {
	return fmt.Sprintf("'%s'%s", strings.ReplaceAll(fmt.Sprintf("%s", value), "'", "''"), terminator)
}

func writeInsertBinaryString(value interface{}, terminator string) string {
	return fmt.Sprintf("'%b'%s", value, terminator)
}

func writeInsertEscapedStringRemoveNewines(value interface{}, terminator string) string {
	quotesEscaped := strings.ReplaceAll(fmt.Sprintf("%s", value), "'", "''")
	newlinesEscaped := strings.ReplaceAll(quotesEscaped, "\n", "")
	return fmt.Sprintf("'%s'%s", newlinesEscaped, terminator)
}

func writeMSSQLUniqueIdentifier(value interface{}, terminator string) string {
	// This is a really stupid fix but it works
	// https://github.com/denisenkom/go-mssqldb/issues/56
	// I guess the bits get shifted around in the first half of these bytes... whatever
	var returnVal string

	switch v := value.(type) {
	case []uint8:
		returnVal = fmt.Sprintf("'%X%X%X%X%X%X%X%X%X%X%X'%s",
			v[3],
			v[2],
			v[1],
			v[0],
			v[5],
			v[4],
			v[7],
			v[6],
			v[8],
			v[9],
			v[10:],
			terminator,
		)
	default:
		return fmt.Sprintf("null%s", terminator)
	}
	return returnVal
}

var backslashJSONReplacer = strings.NewReplacer(
	"'", "''",
	`\"`, `\\"`,
)

func writeBackslashJSON(value interface{}, terminator string) string {
	return fmt.Sprintf("'%s'%s", backslashJSONReplacer.Replace(fmt.Sprintf("%s", value)), terminator)
}
