package shared

import (
	"database/sql"
	"fmt"
)

func TextCreateFormatter(column *sql.ColumnType, terminator string) (string, error) {
	return fmt.Sprintf("%v text%v", column.Name(), terminator), nil
}

func NTextCreateFormatter(column *sql.ColumnType, terminator string) (string, error) {
	return fmt.Sprintf("%v ntext%v", column.Name(), terminator), nil
}

func CharCreateFormatter(column *sql.ColumnType, terminator string) (string, error) {
	length, _ := column.Length()
	return fmt.Sprintf("%v char(%v)%v", column.Name(), length, terminator), nil
}
func VarcharCreateFormatter(column *sql.ColumnType, terminator string) (string, error) {
	length, _ := column.Length()
	return fmt.Sprintf("%v varchar(%v)%v", column.Name(), length, terminator), nil
}

func NumericCreateFormatter(column *sql.ColumnType, terminator string) (string, error) {
	precision, scale, _ := column.DecimalSize()
	return fmt.Sprintf("%v numeric(%v,%v)%v", column.Name(), precision, scale, terminator), nil
}

func NumberCreateFormatter(column *sql.ColumnType, terminator string) (string, error) {
	precision, scale, _ := column.DecimalSize()
	return fmt.Sprintf("%v number(%v,%v)%v", column.Name(), precision, scale, terminator), nil
}

func DecimalCreateFormatter(column *sql.ColumnType, terminator string) (string, error) {
	precision, scale, _ := column.DecimalSize()
	return fmt.Sprintf("%v decimal(%v,%v)%v", column.Name(), precision, scale, terminator), nil
}

func SmallIntCreateFormatter(column *sql.ColumnType, terminator string) (string, error) {
	return fmt.Sprintf("%v smallint%v", column.Name(), terminator), nil
}

func IntCreateFormatter(column *sql.ColumnType, terminator string) (string, error) {
	return fmt.Sprintf("%v int%v", column.Name(), terminator), nil
}

func BigIntCreateFormatter(column *sql.ColumnType, terminator string) (string, error) {
	return fmt.Sprintf("%v bigint%v", column.Name(), terminator), nil
}

func DoublePrecisionCreateFormatter(column *sql.ColumnType, terminator string) (string, error) {
	return fmt.Sprintf("%v double precision%v", column.Name(), terminator), nil
}

func DoubleCreateFormatter(column *sql.ColumnType, terminator string) (string, error) {
	return fmt.Sprintf("%v double %v", column.Name(), terminator), nil
}

func FloatCreateFormatter(column *sql.ColumnType, terminator string) (string, error) {
	return fmt.Sprintf("%v float%v", column.Name(), terminator), nil
}

func TimestampCreateFormatter(column *sql.ColumnType, terminator string) (string, error) {
	return fmt.Sprintf("%v timestamp%v", column.Name(), terminator), nil
}

func DatetimeCreateFormatter(column *sql.ColumnType, terminator string) (string, error) {
	return fmt.Sprintf("%v datetime%v", column.Name(), terminator), nil
}

func Datetime2CreateFormatter(column *sql.ColumnType, terminator string) (string, error) {
	return fmt.Sprintf("%v datetime2%v", column.Name(), terminator), nil
}

func TimeCreateFormatter(column *sql.ColumnType, terminator string) (string, error) {
	return fmt.Sprintf("%v time%v", column.Name(), terminator), nil
}

func DateCreateFormatter(column *sql.ColumnType, terminator string) (string, error) {
	return fmt.Sprintf("%v date%v", column.Name(), terminator), nil
}

func ByteaCreateFormatter(column *sql.ColumnType, terminator string) (string, error) {
	return fmt.Sprintf("%v bytea%v", column.Name(), terminator), nil
}

func BinaryCreateFormatter(column *sql.ColumnType, terminator string) (string, error) {
	return fmt.Sprintf("%v binary%v", column.Name(), terminator), nil
}

func LongBlobCreateFormatter(column *sql.ColumnType, terminator string) (string, error) {
	return fmt.Sprintf("%v longblob%v", column.Name(), terminator), nil
}

func VarbinaryCreateFormatter(column *sql.ColumnType, terminator string) (string, error) {
	length, _ := column.Length()
	return fmt.Sprintf("%v varbinary(%v)%v", column.Name(), length, terminator), nil
}

func BoolCreateFormatter(column *sql.ColumnType, terminator string) (string, error) {
	return fmt.Sprintf("%v bool%v", column.Name(), terminator), nil
}

func BooleanCreateFormatter(column *sql.ColumnType, terminator string) (string, error) {
	return fmt.Sprintf("%v boolean%v", column.Name(), terminator), nil
}

func BitCreateFormatter(column *sql.ColumnType, terminator string) (string, error) {
	return fmt.Sprintf("%v bit%v", column.Name(), terminator), nil
}

func UuidCreateFormatter(column *sql.ColumnType, terminator string) (string, error) {
	return fmt.Sprintf("%v uuid%v", column.Name(), terminator), nil
}

func UniqueIdentifierCreateFormatter(column *sql.ColumnType, terminator string) (string, error) {
	return fmt.Sprintf("%v uniqueidentifier%v", column.Name(), terminator), nil
}

func XmlCreateFormatter(column *sql.ColumnType, terminator string) (string, error) {
	return fmt.Sprintf("%v xml%v", column.Name(), terminator), nil
}
