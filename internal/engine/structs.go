package engine

import "reflect"

type QueryResult struct {
	ColumnTypes map[string]string
	Rows        []interface{}
}

type ResultSetColumnInfo struct {
	ColumnNames             []string
	ColumnDbTypes           []string
	ColumnIntermediateTypes []string
	ColumnScanTypes         []reflect.Type
	ColumnLengths           []int64
	LengthOks               []bool
	ColumnPrecisions        []int64
	ColumnScales            []int64
	PrecisionScaleOks       []bool
	ColumnNullables         []bool
	NullableOks             []bool
	NumCols                 int
	ColumnNamesToTypes      map[string]string
}
