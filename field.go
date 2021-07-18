package main

import (
	"fmt"
	"reflect"
	"strings"
)

const EXTERN_FIELD_SIZE = 20

type ExternReference struct {
	space_id    uint64
	page_number uint64
	offset      uint64
	length      uint64
}

func newExternReference(space_id uint64, page_number uint64, offset uint64, length uint64) *ExternReference {
	return &ExternReference{space_id: space_id, page_number: page_number, offset: offset, length: length}
}

type Recordfield struct {
	Extern    ExternReference
	position  int
	name      string
	data_type interface{}
	nullable  bool
}

func newRecordfield(position int, name string, type_definition string, properties string) *Recordfield {
	nullable := true
	// if strings.Contains(properties, "NOT_NULL") {
	// 	nullable = false
	// } else {
	// 	nullable = true
	// }
	result := strings.Index(properties, "NOT_NULL")
	if result >= 0 {
		nullable = false
		properties = properties[:result] + properties[result+8:]
	} else {
		nullable = true
	}

	base_type, modifiers := parse_type_definition(type_definition)
	data_type, _ := newDataType(base_type, modifiers, properties)
	return &Recordfield{
		position: position, name: name, data_type: data_type, nullable: nullable,
	}
}

func (rf *Recordfield) is_nullable(record *Record) bool {
	return rf.nullable
}

// func (rf *Recordfield) is_null(record *Record) bool {

// 	return rf.is_nullable() && record.header.nulls.include(rf.name)
// }

// func (rf *Recordfield) is_extern(record *Record) bool {
// 	return record.header.externs.include(rf.name)
// }

func (rf *Recordfield) is_variable() bool {
	types := []string{"BlobType", "VariableBinaryType", "VariableCharacterType"}
	for _, element := range types {
		if rf.data_type == element {
			return true
		}
	}
	return false
}

func (rf *Recordfield) is_blob() bool {
	if rf.data_type == "BlobType" {
		return true
	}
	return false
}

func parse_type_definition(type_definition string) (string, string) {
	// base_type := "aaa"
	modifiers := " "
	return type_definition, modifiers
}

func (rf *Recordfield) value(offset uint64, record *UserRecord, index *Index) (interface{}, uint64) {
	if record == nil {
		return nil, 0
	}

	return rf.value_by_length(offset, rf.length(record), index)
}

func (rf *Recordfield) length(record *UserRecord) int64 {
	var len int64
	fmt.Println("length() recordfield name, datatype =====>", rf.name, rf.data_type)
	fmt.Println("length() record.header.lengths =====>%v", record.header.lengths)
	name_in_map := false
	for k, _ := range record.header.lengths {
		if rf.name == k {
			name_in_map = true
		}
	}
	if name_in_map {
		len = int64(record.header.lengths[rf.name])
		fmt.Println("length() record filed length =====>", len)
	} else {
		switch value := rf.data_type.(type) {
		case IntegerType:
			len = int64(rf.data_type.(IntegerType).width)
		case BitType:
			len = int64(rf.data_type.(BitType).width)
		default:
			fmt.Println("unkown data type===>", value)

		}
	}

	if rf.is_extern(record) {
		return len - EXTERN_FIELD_SIZE
	}
	return len
}

func (rf *Recordfield) is_extern(record *UserRecord) bool {
	for i := 0; i < len(record.header.externs); i++ {
		if rf.name == record.header.externs[i] {
			return true
		}
	}
	return false
}

func (rf *Recordfield) extern(offset int64, index *Index, record *UserRecord) *ExternReference {
	if rf.is_extern(record) {
		return rf.read_extern(offset, index)
	}
	return nil
}

func (rf *Recordfield) read_extern(offset int64, index *Index) *ExternReference {
	space_id := index.Page.bufferReadat(offset, 4)
	page_number := index.Page.bufferReadat(offset+4, 4)
	e_offset := index.Page.bufferReadat(offset+8, 4)
	length := index.Page.bufferReadat(offset+12, 8) & 0x3fffffff
	return newExternReference(uint64(space_id), uint64(page_number), uint64(e_offset), uint64(length))
}

func (rf *Recordfield) has_method(data_type interface{}, method_name string) bool {

	switch value := data_type.(type) {
	case IntegerType:
		val := reflect.ValueOf(data_type.(IntegerType))
		typ := val.Type()
		for i := 0; i < val.NumMethod(); i++ {
			fmt.Println(fmt.Sprintf("method[%d]%s and type is %v", i, typ.Method(i).Name, typ.Method(i).Type))
			if typ.Method(i).Name == method_name {
				return true
			}
		}

	default:
		fmt.Println("unkown data type%T", value)

	}

	return false
}

func (rf *Recordfield) value_by_length(offset uint64, field_length int64, index *Index) (interface{}, uint64) {

	if _, ok := rf.data_type.(interface{ value() }); ok {
		switch rf.data_type.(type) {
		case *IntegerType:
			return rf.data_type.(*IntegerType).value(rf.read(offset, field_length, index), index), uint64(field_length)
		}
	} else if _, ok := rf.data_type.(interface{ read() }); ok {
		switch rf.data_type.(type) {
		case *TransactionIdType:
			return rf.data_type.(*TransactionIdType).read(offset, index.Page), 6

		}
	} else {
		return rf.read(offset, field_length, index), 0
	}
	return nil, 0

}

func (rf *Recordfield) read(offset uint64, field_length int64, index *Index) []byte {

	return (index.Page.readbytes(int64(offset), field_length))
}
