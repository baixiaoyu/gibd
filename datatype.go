package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"reflect"
	"strconv"
	"strings"
)

type BitType struct {
	name  string
	width int
}

func newBitType(base_type string, modifiers string, properties string) *BitType {
	nbits := len(modifiers)

	if nbits < 0 || nbits > 64 {
		return nil
	}
	width := (nbits + 7) / 8
	name := make_name(base_type, modifiers, properties)
	return &BitType{width: width, name: name}
}

func (bit *BitType) value(data int) int {
	return data
}

type IntegerType struct {
	name     string
	width    int
	unsigned bool
}

func newIntegerType(base_type string, modifiers string, properties string) *IntegerType {
	width := base_type_width_map[base_type]
	unsigned := strings.Contains(properties, "UNSIGNED")
	name := make_name(base_type, modifiers, properties)
	return &IntegerType{
		width: width, name: name, unsigned: unsigned,
	}

}

func (integer *IntegerType) value(data []byte, index *Index) int64 {
	nbits := integer.width * 8
	if integer.unsigned {
		return integer.get_uint(data, nbits, index)
	} else {
		return integer.get_int(data, nbits, index)
	}
}

func (integer *IntegerType) get_uint(data []byte, nbits int, index *Index) int64 {
	return int64(index.Page.BytesToUIntLittleEndian(data))
}
func (integer *IntegerType) get_int(data []byte, nbits int, index *Index) int64 {
	return int64(index.Page.BytesToIntLittleEndian(data))
}

type TransactionIdType struct {
	name  string
	width int
}

func newTransactionIdType(base_type string, modifiers string, properties string) *TransactionIdType {
	width := 6
	name := make_name(base_type, modifiers, properties)
	return &TransactionIdType{
		name:  name,
		width: width,
	}
}

func (t *TransactionIdType) read(offset uint64, p *Page) uint64 {
	transaction_id := uint64(p.bufferReadat(int64(offset), 6))
	return transaction_id
}

type Pointer struct {
	is_insert bool
	rseg_id   uint64
	undo_log  *Address
}

func newPointer(is_insert bool, rseg_id uint64, undo_log *Address) *Pointer {
	return &Pointer{
		is_insert: is_insert,
		rseg_id:   rseg_id,
		undo_log:  undo_log,
	}
}

type RollPointerType struct {
	name  string
	width int
	p     *Pointer
}

func newRollPointerType(base_type string, modifiers string, properties string) *RollPointerType {
	width := 7
	name := make_name(base_type, modifiers, properties)
	return &RollPointerType{
		name:  name,
		width: width,
	}
}

func (r *RollPointerType) parse_roll_pointer(roll_ptr uint64) *Pointer {
	is_insert := read_bits_at_offset(roll_ptr, 1, 55) == 1
	rseg_id := read_bits_at_offset(roll_ptr, 7, 48)
	page := read_bits_at_offset(roll_ptr, 32, 16)
	offset := read_bits_at_offset(roll_ptr, 16, 0)
	undo_log := newAddress(page, offset)

	return newPointer(is_insert, rseg_id, undo_log)
}

func read_bits_at_offset(data uint64, bits int, offset int) uint64 {
	return ((data & (((1 << bits) - 1) << offset)) >> offset)
}

func BytesToInt(b []byte) uint64 {
	bytesBuffer := bytes.NewBuffer(b)

	var x int32
	binary.Read(bytesBuffer, binary.BigEndian, &x)

	return uint64(x)
}

func (r *RollPointerType) value(data []uint8) *Pointer {
	roll_ptr := BytesToInt(data)
	p := r.parse_roll_pointer(roll_ptr)
	r.p = p
	return r.p
}

type VariableCharacterType struct {
	name  string
	width int
}

func newVariableCharacterType(base_type string, modifiers string, properties string) *VariableCharacterType {
	width, _ := strconv.Atoi(modifiers)
	name := make_name(base_type, modifiers, properties)
	return &VariableCharacterType{
		name:  name,
		width: width,
	}
}

func (r *VariableCharacterType) value(data string) string {

	return strings.TrimRight(data, " ")

}

func make_name(base_type string, modifiers string, properties string) string {
	Log.Info("make_name======base_type,%+v\n", base_type)
	Log.Info("make_name======modifiers,%+v\n", modifiers)
	Log.Info("make_name======properties,%+v\n", properties)

	if len(modifiers) > 0 && modifiers != " " {
		name := base_type + "(" + modifiers + ")" + properties
		return name
	} else {
		name := base_type + modifiers + properties
		return name
	}

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
	switch base_type {
	case "BOOL":
		return newIntegerType(base_type, modifiers, properties), nil
	case "BOOLEAN":
		return newIntegerType(base_type, modifiers, properties), nil
	case "TINYINT":
		return newIntegerType(base_type, modifiers, properties), nil
	case "SMALLINT":
		return newIntegerType(base_type, modifiers, properties), nil
	case "MEDIUMINT":
		return newIntegerType(base_type, modifiers, properties), nil
	case "INT":
		return newIntegerType(base_type, modifiers, properties), nil
	case "INT6":
		return newIntegerType(base_type, modifiers, properties), nil
	case "BIGINT":
		return newIntegerType(base_type, modifiers, properties), nil
	case "TRX_ID":
		return newTransactionIdType(base_type, modifiers, properties), nil
	case "ROLL_PTR":
		return newRollPointerType(base_type, modifiers, properties), nil
	case "VARCHAR":
		return newVariableCharacterType(base_type, modifiers, properties), nil
	}
	return nil, errors.New("not found datatype!")
}

func newDataType(base_type string, modifiers string, properties string) (c interface{}, err error) {
	return NewType(base_type, modifiers, properties)
}
