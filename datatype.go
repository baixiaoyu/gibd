package main

import (
	"fmt"
	"reflect"
)

type IntegerType struct {
	name  string
	width int
}

func newIntegerType(base_type string, modifiers string, properties string) *IntegerType {
	width := base_type_width_map[base_type]
	//unsigned := strings.Contains(properties, "UNSIGNED")
	name := make_name(base_type, modifiers, properties)
	return &IntegerType{
		width: width, name: name,
	}

}

func make_name(base_type string, modifiers string, properties string) string {
	name := base_type + modifiers
	return name
}

var base_type_width_map = map[string]int{"BOOL": 1, "BOOLEAN": 1, "TINYINT": 1, "SMALLINT": 2, "MEDIUMINT": 3, "INT": 4, "INT6": 6, "BIGINT": 8}

var TYPES = map[string]string{
	"BIT":        "BitType",
	"BOOL":       "IntegerType",
	"BOOLEAN":    "IntegerType",
	"TINYINT":    "IntegerType",
	"SMALLINT":   "IntegerType",
	"MEDIUMINT":  "IntegerType",
	"INT":        "IntegerType",
	"INT6":       "IntegerType",
	"BIGINT":     "IntegerType",
	"FLOAT":      "FloatType",
	"DOUBLE":     "DoubleType",
	"DECIMAL":    "DecimalType",
	"NUMERIC":    "DecimalType",
	"CHAR":       "CharacterType",
	"VARCHAR":    "VariableCharacterType",
	"BINARY":     "BinaryType",
	"VARBINARY":  "VariableBinaryType",
	"TINYBLOB":   "BlobType",
	"BLOB":       "BlobType",
	"MEDIUMBLOB": "BlobType",
	"LONGBLOB":   "BlobType",
	"TINYTEXT":   "BlobType",
	"TEXT":       "BlobType",
	"MEDIUMTEXT": "BlobType",
	"LONGTEXT":   "BlobType",
	"YEAR":       "YearType",
	"TIME":       "TimeType",
	"DATE":       "DateType",
	"DATETIME":   "DatetimeType",
	"TIMESTAMP":  "TimestampType",
	"TRX_ID":     "TransactionIdType",
	"ROLL_PTR":   "RollPointerType",
}

var type_struct_map = map[string]reflect.Type{
	"BOOL":      reflect.TypeOf(&IntegerType{}).Elem(),
	"BOOLEAN":   reflect.TypeOf(&IntegerType{}).Elem(),
	"TINYINT":   reflect.TypeOf(&IntegerType{}).Elem(),
	"SMALLINT":  reflect.TypeOf(&IntegerType{}).Elem(),
	"MEDIUMINT": reflect.TypeOf(&IntegerType{}).Elem(),
	"INT":       reflect.TypeOf(&IntegerType{}).Elem(),
	"INT6":      reflect.TypeOf(&IntegerType{}).Elem(),
	"BIGINT":    reflect.TypeOf(&IntegerType{}).Elem(),
}

func NewType(base_type string, modifiers string, properties string) (c interface{}, err error) {
	if v, ok := type_struct_map[base_type]; ok {
		c = reflect.New(v).Interface()
	} else {
		err = fmt.Errorf("not found %s type", base_type)
	}
	return
}

func newDataType(base_type string, modifiers string, properties string) (c interface{}, err error) {

	// legal_key := false

	// for k := range TYPES {
	// 	if base_type == k {
	// 		legal_key = true
	// 	}
	// }
	// if legal_key == false {
	// 	panic("Data type '#{base_type}' is not supported")
	// }

	return NewType(base_type, modifiers, properties)
}