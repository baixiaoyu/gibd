package main

import "strings"

const EXTERN_FIELD_SIZE = 20

type ExternReference struct {
	space_id    uint64
	page_number uint64
	offset      uint64
	length      uint64
}

type Recordfield struct {
	Extern    ExternReference
	position  uint64
	name      string
	data_type interface{}
	nullable  bool
}

func newRecordfield(position uint64, name string, type_definition string, properties string) *Recordfield {
	nullable := true
	if strings.Contains(properties, "NOT_NULL") {
		nullable = false
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
	base_type := "aaa"
	modifiers := "bbb"
	return base_type, modifiers
}
