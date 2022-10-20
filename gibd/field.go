package gibd

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

func NewExternReference(space_id uint64, page_number uint64, offset uint64, length uint64) *ExternReference {
	return &ExternReference{space_id: space_id, page_number: page_number, offset: offset, length: length}
}

// 列名以及对应的值,此处应该包含RecordField 对象，不是进行重复定义
type FieldDescriptor struct {
	name       string
	field_type string
	value      interface{}
	extern     *ExternReference
}

func NewFieldDescriptor(name string, field_type string, value interface{}, extern *ExternReference) *FieldDescriptor {
	return &FieldDescriptor{
		name:       name,
		field_type: field_type,
		value:      value,
		extern:     extern,
	}
}

type Field struct {
	FieldName string `json:"fieldname"`
	// FieldDesc string `json:"fieldesc"`
	DataType   string `json:"datatype"`
	Properties string `json:Properties`
	IsNull     string `json:"isnull"`
	Length     int    `json:"length"`
	Is_key     bool   `json:"is_key"`
}

// Field 这两个冲突
type RecordField struct {
	Extern    ExternReference
	position  int
	name      string
	data_type interface{}
	nullable  bool
}

func NewRecordField(position int, name string, type_definition string, properties string) *RecordField {
	nullable := true

	result := strings.Index(properties, "NOT_NULL")
	if result >= 0 {
		nullable = false
		properties = properties[:result] + properties[result+8:]
	} else {
		nullable = true
	}

	base_type, modifiers := Parse_Type_Definition(type_definition)
	data_type, _ := NewDataType(base_type, modifiers, properties)
	return &RecordField{
		position: position, name: name, data_type: data_type, nullable: nullable,
	}
}

func (rf *RecordField) Is_Nullable(record *Record) bool {
	return rf.nullable
}

func (rf *RecordField) Is_Variable() bool {
	types := []string{"BlobType", "VariableBinaryType", "VariableCharacterType"}
	for _, element := range types {
		if rf.data_type == element {
			return true
		}
	}
	return false
}

func (rf *RecordField) Is_Blob() bool {
	if rf.data_type == "BlobType" {
		return true
	}
	return false
}

func Parse_Type_Definition(type_definition string) (string, string) {
	// base_type := "varchar(100)" modifiers=100
	if strings.Contains(type_definition, "(") && strings.Contains(type_definition, ")") {
		start_pos := strings.Index(type_definition, "(")
		end_pos := strings.Index(type_definition, ")")
		modifiers := type_definition[start_pos+1 : end_pos]
		type_def := type_definition[0:start_pos]
		return type_def, modifiers
	} else {
		modifiers := " "
		return type_definition, modifiers
	}

}

func (rf *RecordField) Value(offset uint64, record *UserRecord, index *Index) (interface{}, uint64) {
	if record == nil {
		return nil, 0
	}

	return rf.Value_By_Length(offset, rf.length(record), index)
}

func (rf *RecordField) Value_By_Length(offset uint64, field_length int64, index *Index) (interface{}, uint64) {

	switch rf.data_type.(type) {
	case *IntegerType:
		return rf.data_type.(*IntegerType).Value(rf.Read(offset, field_length, index), index), uint64(field_length)
	case *TransactionIdType:
		return rf.data_type.(*TransactionIdType).Read(offset, index.Page), 6
	case *RollPointerType:
		bytes := rf.Read(offset, field_length, index)

		return rf.data_type.(*RollPointerType).Value(bytes), uint64(field_length)
	case *VariableCharacterType:

		return rf.data_type.(*VariableCharacterType).Value(string(rf.Read(offset, field_length, index))), uint64(field_length)
	default:
		Log.Info("value_by_length() 还未实现的类型========%\n")
	}

	return nil, 0

}

func (rf *RecordField) length(record *UserRecord) int64 {
	var len int64
	//字段名称在header中，这个字段是变长的字符串，需要在header 中获取长度信息
	name_in_header_lengths_map := false
	for k, _ := range record.header.Lengths {
		if rf.name == string(k) {
			name_in_header_lengths_map = true
		}
	}
	// name_in_header_lengths_map = false //暂时设置
	if name_in_header_lengths_map {
		len = int64(record.header.Lengths[rf.name])
	} else {
		switch value := rf.data_type.(type) {
		case *IntegerType:
			len = int64(rf.data_type.(*IntegerType).width)
		case *BitType:
			len = int64(rf.data_type.(*BitType).width)
		// case *VariableCharacterType:
		// 	//此处的变长字段长度值，需要在record header 中的variable field lengths中获取
		// 	len = int64(rf.data_type.(*VariableCharacterType).width)
		default:
			fmt.Println("unkown data type===>", value)
		}
	}

	if rf.Is_Extern(record) {
		return len - EXTERN_FIELD_SIZE
	}
	return len
}

func (rf *RecordField) Is_Extern(record *UserRecord) bool {
	for i := 0; i < len(record.header.Externs); i++ {
		if rf.name == string(record.header.Externs[i]) {
			return true
		}
	}
	return false
}

func (rf *RecordField) extern(offset int64, index *Index, record *UserRecord) *ExternReference {
	if rf.Is_Extern(record) {
		return rf.Read_Extern(offset, index)
	}
	return nil
}

func (rf *RecordField) Read_Extern(offset int64, index *Index) *ExternReference {
	space_id := BufferReadAt(index.Page, offset, 4)
	page_number := BufferReadAt(index.Page, offset+4, 4)
	e_offset := BufferReadAt(index.Page, offset+8, 4)
	length := BufferReadAt(index.Page, offset+12, 8) & 0x3fffffff
	return NewExternReference(uint64(space_id), uint64(page_number), uint64(e_offset), uint64(length))
}

func (rf *RecordField) Has_Method(data_type interface{}, method_name string) bool {

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

func (rf *RecordField) Read(offset uint64, field_length int64, index *Index) []byte {

	return (ReadBytes(index.Page, int64(offset), field_length))
}
