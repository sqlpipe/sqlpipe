package shared

import (
	"database/sql"
	"fmt"
)

func TextCreateFormatter(column *sql.ColumnType, terminator string) (string, error) {
	return fmt.Sprintf("%v text%v", column.Name(), terminator), nil
}

func NumericCreateFormatter(column *sql.ColumnType, terminator string) (string, error) {
	precision, scale, _ := column.DecimalSize()
	return fmt.Sprintf("%v numeric(%v,%v)%v", column.Name(), precision, scale, terminator), nil
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

func DoubleCreateFormatter(column *sql.ColumnType, terminator string) (string, error) {
	return fmt.Sprintf("%v double precision%v", column.Name(), terminator), nil
}

func TimestampCreateFormatter(column *sql.ColumnType, terminator string) (string, error) {
	return fmt.Sprintf("%v timestamp%v", column.Name(), terminator), nil
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

func BoolCreateFormatter(column *sql.ColumnType, terminator string) (string, error) {
	return fmt.Sprintf("%v bool%v", column.Name(), terminator), nil
}

func UuidCreateFormatter(column *sql.ColumnType, terminator string) (string, error) {
	return fmt.Sprintf("%v uuid%v", column.Name(), terminator), nil
}

func XmlCreateFormatter(column *sql.ColumnType, terminator string) (string, error) {
	return fmt.Sprintf("%v xml%v", column.Name(), terminator), nil
}
