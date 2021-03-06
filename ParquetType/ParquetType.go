package ParquetType

import (
	"fmt"
	"log"
)

//base type
type BOOLEAN bool
type INT32 int32
type INT64 int64
type INT96 string // length=96
type FLOAT float32
type DOUBLE float64
type BYTE_ARRAY string
type FIXED_LEN_BYTE_ARRAY string

//logical type
type UTF8 string
type INT_8 int32
type INT_16 int32
type INT_32 int32
type INT_64 int64
type UINT_8 uint32
type UINT_16 uint32
type UINT_32 uint32
type UINT_64 uint64
type DATE int32
type TIME_MILLIS int32
type TIME_MICROS int64
type TIMESTAMP_MILLIS int64
type TIMESTAMP_MICROS int64
type INTERVAL string // length=12
type DECIMAL string

//Scan a string to parquet value
func StrToParquetType(s string, typeName string) interface{} {
	if typeName == "BOOLEAN" {
		var v BOOLEAN
		fmt.Sscanf(s, "%t", &v)
		return v
	} else if typeName == "INT32" {
		var v INT32
		fmt.Sscanf(s, "%d", &v)
		return v
	} else if typeName == "INT64" {
		var v INT64
		fmt.Sscanf(s, "%d", &v)
		return v
	} else if typeName == "INT96" {
		var v INT96
		v = INT96(s)
		return v
	} else if typeName == "FLOAT" {
		var v FLOAT
		fmt.Sscanf(s, "%f", &v)
		return v
	} else if typeName == "DOUBLE" {
		var v DOUBLE
		fmt.Sscanf(s, "%f", &v)
		return v
	} else if typeName == "BYTE_ARRAY" {
		var v BYTE_ARRAY
		v = BYTE_ARRAY(s)
		return v
	} else if typeName == "FIXED_LEN_BYTE_ARRAY" {
		var v FIXED_LEN_BYTE_ARRAY
		v = FIXED_LEN_BYTE_ARRAY(s)
		return v
	} else if typeName == "UTF8" {
		var v UTF8
		v = UTF8(s)
		return v
	} else if typeName == "INT_8" {
		var v INT_8
		fmt.Sscanf(s, "%d", &v)
		return v
	} else if typeName == "INT_16" {
		var v INT_16
		fmt.Sscanf(s, "%d", &v)
		return v
	} else if typeName == "INT_32" {
		var v INT_32
		fmt.Sscanf(s, "%d", &v)
		return v
	} else if typeName == "INT_64" {
		var v INT_64
		fmt.Sscanf(s, "%d", &v)
		return v
	} else if typeName == "UINT_8" {
		var v UINT_8
		fmt.Sscanf(s, "%d", &v)
		return v
	} else if typeName == "UINT_16" {
		var v UINT_16
		fmt.Sscanf(s, "%d", &v)
		return v
	} else if typeName == "UINT_32" {
		var v UINT_32
		fmt.Sscanf(s, "%d", &v)
		return v
	} else if typeName == "UINT_64" {
		var v UINT_64
		fmt.Sscanf(s, "%d", &v)
		return v
	} else if typeName == "DATE" {
		var v DATE
		fmt.Sscanf(s, "%d", &v)
		return v
	} else if typeName == "TIME_MILLIS" {
		var v TIME_MILLIS
		fmt.Sscanf(s, "%d", &v)
		return v
	} else if typeName == "TIME_MICROS" {
		var v TIME_MICROS
		fmt.Sscanf(s, "%d", &v)
		return v
	} else if typeName == "TIMESTAMP_MILLIS" {
		var v TIMESTAMP_MILLIS
		fmt.Sscanf(s, "%d", &v)
		return v
	} else if typeName == "TIMESTAMP_MICROS" {
		var v TIMESTAMP_MICROS
		fmt.Sscanf(s, "%d", &v)
		return v
	} else if typeName == "INTERVAL" {
		var v INTERVAL
		v = INTERVAL(s)
		return v
	} else if typeName == "DECIMAL" {
		var v DECIMAL
		v = DECIMAL(s)
		return v
	} else {
		log.Printf("Type Error: %v ", typeName)
		return nil
	}

}
