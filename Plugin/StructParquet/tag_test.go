package StructParquet

import (
	"testing"
)

func TestParseTag(t *testing.T) {
	for _, tag := range []string{
		"",
		"nameonly",
		"parameters(10,2)",
	} {
		ct, err := parseTag(tag)
		if err == nil {
			t.Errorf("%s should be raise error but got %#v", tag, ct)
		}
	}

	dataSet := []struct {
		tag string
		ct  ColumnType
	}{
		// Primitive Types
		{"boolean,BOOLEAN", ColumnType{"boolean", "BOOLEAN", 0, 0}},
		{"int32,INT32", ColumnType{"int32", "INT32", 0, 0}},
		{"int64,INT64", ColumnType{"int64", "INT64", 0, 0}},
		{"int96,INT96", ColumnType{"int96", "INT96", 0, 0}},
		{"float,FLOAT", ColumnType{"float", "FLOAT", 0, 0}},
		{"double,DOUBLE", ColumnType{"double", "DOUBLE", 0, 0}},
		{"byte_array,BYTE_ARRAY", ColumnType{"byte_array", "BYTE_ARRAY", 0, 0}},
		// Logical Types
		{"utf8,UTF8", ColumnType{"utf8", "UTF8", 0, 0}},
		{"int_8,INT_8", ColumnType{"int_8", "INT_8", 0, 0}},
		{"int_16,INT_16", ColumnType{"int_16", "INT_16", 0, 0}},
		{"int_32,INT_32", ColumnType{"int_32", "INT_32", 0, 0}},
		{"int_64,INT_64", ColumnType{"int_64", "INT_64", 0, 0}},
		{"uint_8,UINT_8", ColumnType{"uint_8", "UINT_8", 0, 0}},
		{"uint_16,UINT_16", ColumnType{"uint_16", "UINT_16", 0, 0}},
		{"uint_32,UINT_32", ColumnType{"uint_32", "UINT_32", 0, 0}},
		{"uint_64,UINT_64", ColumnType{"uint_64", "UINT_64", 0, 0}},
		{"decimal,DECIMAL(9)", ColumnType{"decimal", "DECIMAL", 9, 0}},
		{"decimal,DECIMAL(10,4)", ColumnType{"decimal", "DECIMAL", 10, 4}},
	}
	for _, data := range dataSet {
		ct, err := parseTag(data.tag)
		if err != nil {
			t.Errorf("Unexpected error: %s: tag: %s", err.Error(), data.tag)
		}
		if ct.Name != data.ct.Name {
			t.Errorf("Name expected %s, but got %s", data.ct.Name, ct.Name)
		}
		if ct.Type != data.ct.Type {
			t.Errorf("Type expected %s, but got %s", data.ct.Type, ct.Type)
		}
		if ct.Precision != data.ct.Precision {
			t.Errorf("Precision expected %d, but got %d", data.ct.Precision, ct.Precision)
		}
		if ct.Scale != data.ct.Scale {
			t.Errorf("Scale expected %d, but got %d", data.ct.Scale, ct.Scale)
		}
	}
}
