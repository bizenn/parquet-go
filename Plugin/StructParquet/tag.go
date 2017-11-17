// Parquet encoder for golang struct
//
// This is intended to behave similarly to golang encoding/json fashion.

package StructParquet

import (
	"fmt"
	"regexp"
	"strconv"
)

const (
	tagName = "parquet"
)

type ColumnType struct {
	Name      string
	Type      string
	Precision int32
	Scale     int32
}

var (
	nameMatcher      = regexp.MustCompile(`^[_[:alpha:]][_[:alnum:]]*`)
	delimiterMatcher = regexp.MustCompile(`^\s*,\s*`)
	typeMatcher      = regexp.MustCompile(`^[_[:alpha:]][_[:alnum:]]*$`)
	typeParamMatcher = regexp.MustCompile(`^([_[:alpha:]][_[:alnum:]]*)\((\d+)(?:\s*,\s*(\d+))?\)$`)
)

// tag := parquet:"<columnName>,<type_descriptor>"
// columnName := valid parquet column name
// type_descriptor :=  BOOLEAN | INT32 | INT64 | FLOAT | DOUBLE | BYTE_ARRAY \
//                     | UTF8 | INT_8 | INT_16 | INT_32 | INT_64 | UINT_8 | UINT_16 | UINT_32 | UINT_64 \
//                     | DECIMAL(precision,scale) \
//                     | DATE | TIME_MILLIS | TIME_MICROS | TIMESTAMP_MILLIS | TIMESTAMP_MICROS | INTERVAL
// Currently unsupported: JSON, BSON, Nested Types
func parseTag(tag string) (ct *ColumnType, err error) {
	ct = &ColumnType{}
	var is []int
	if is = nameMatcher.FindStringIndex(tag); is == nil {
		err = fmt.Errorf("Failed to parse \"Name\" on %s", tag)
	} else {
		ct.Name = tag[is[0]:is[1]]
		tag = tag[is[1]:]
		if is = delimiterMatcher.FindStringIndex(tag); is == nil {
			err = fmt.Errorf("Failed to parse delimiter on %s", tag)
		} else {
			tag = tag[is[1]:]
			if is = typeMatcher.FindStringIndex(tag); is != nil {
				ct.Type = tag[is[0]:is[1]]
			} else if ss := typeParamMatcher.FindStringSubmatch(tag); ss != nil {
				ct.Type = ss[1]
				var p int64
				if p, err = strconv.ParseInt(ss[2], 10, 32); err == nil {
					ct.Precision = int32(p)
					if ss[3] != "" {
						var s int64
						if s, err = strconv.ParseInt(ss[3], 10, 32); err == nil {
							ct.Scale = int32(s)
						}
					}
				}
			} else {
				err = fmt.Errorf("Failed to parse \"Type\" on %s", tag)
			}
		}
	}
	return ct, err
}
