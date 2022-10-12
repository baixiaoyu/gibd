package gibd

import "fmt"

const RECORD_INFO_MIN_REC_FLAG = 1
const RECORD_INFO_DELETED_FLAG = 2
const RECORD_NEXT_SIZE = 2
const RECORD_REDUNDANT_BITS_SIZE = 4
const RECORD_REDUNDANT_OFF1_OFFSET_MASK = 0x7f
const RECORD_REDUNDANT_OFF1_NULL_MASK = 0x80
const RECORD_REDUNDANT_OFF2_OFFSET_MASK = 0x3fff
const RECORD_REDUNDANT_OFF2_NULL_MASK = 0x8000
const RECORD_REDUNDANT_OFF2_EXTERN_MASK = 0x4000
const RECORD_COMPACT_BITS_SIZE = 3
const RECORD_MAX_N_SYSTEM_FIELDS = 3
const RECORD_MAX_N_FIELDS = 1024 - 1
const RECORD_MAX_N_USER_FIELDS = RECORD_MAX_N_FIELDS - RECORD_MAX_N_SYSTEM_FIELDS*2
const PAGE_DIR_SLOT_SIZE = 2
const PAGE_DIR_SLOT_MIN_N_OWNED = 4
const PAGE_DIR_SLOT_MAX_N_OWNED = 8

var RECORD_TYPES = map[uint64]string{
	0: "conventional", // A normal user record in a leaf page.
	1: "node_pointer", // A node pointer in a non-leaf page.
	2: "infimum",      // The system "infimum" record.
	3: "supremum",     // The system "supremum" record.
}

// record format https://blog.jcole.us/2013/01/10/the-physical-structure-of-records-in-innodb/
type RecordHeader struct {
	Offset      uint64         `json:"offset"`
	Length      uint64         `json:"length"`
	Next        uint64         `json:"next"`
	Prev        uint64         `json:"prev"`
	Record_Type string         `json:"type"`
	Heap_Number uint64         `json:"heap_number"`
	N_owned     uint64         `json:"n_owned"`
	Info_flags  uint64         `json:"info_flags"`
	Offset_size uint64         `json:"offset_size"`
	N_fields    uint64         `json:"n_fields"`
	Nulls       string         `json:"nulls"`
	Lengths     map[string]int `json:"lengths"`
	Externs     string         `json:"externs"`
}

func NewRecordHeader(offset uint64) *RecordHeader {
	return &RecordHeader{Offset: offset}

}

func (rh *RecordHeader) Is_Min_Rec() bool {
	return (rh.Info_flags & RECORD_INFO_MIN_REC_FLAG) != 0
}

func (rh *RecordHeader) Is_Deleted() bool {
	return (rh.Info_flags & RECORD_INFO_DELETED_FLAG) != 0
}

type Record struct {
	Page   *Page
	record interface{} //UserRecord or SystemRecord
	//	fields map[string]interface{}
}

func NewRecord(page *Page, record interface{}) *Record {
	return &Record{
		Page:   page,
		record: record,
	}
}

func NewRecord2() *Record {
	return &Record{}
}

func (record *Record) Get_Fields() map[string]interface{} {
	fields_hash := make(map[string]interface{})
	keys := record.record.(*UserRecord).key
	rows := record.record.(*UserRecord).row

	for _, value := range keys {
		Log.Info("get_fields() keys name====>%+v\n", value.name)
		Log.Info("get_fields() keys value====>%+v\n", value.value)
		fields_hash[value.name] = value.value
	}

	for _, value := range rows {
		Log.Info("get_fields() rows name====>%+v\n", value.name)
		Log.Info("get_fields() rows value====>%+v\n", value.value)
		fields_hash[value.name] = value.value
	}
	return fields_hash

}

type SystemRecord struct {
	offset uint64
	header *RecordHeader
	next   uint64
	data   []byte
	length uint64
}

func NewSystemRecord(offset uint64, header *RecordHeader, next uint64, data []byte, length uint64) *SystemRecord {

	return &SystemRecord{
		offset: offset, header: header, next: next, data: data, length: length,
	}

}

type UserRecord struct {
	record_type       string
	format            string
	offset            uint64
	header            *RecordHeader
	next              uint64
	key               []*FieldDescriptor
	row               []*FieldDescriptor
	sys               []*FieldDescriptor
	Child_page_number uint64   `json:"child_page_number"`
	Transaction_id    uint64   `json:"trx_id"`
	Roll_pointer      *Pointer `json:"roll_pointer"`
	Length            uint64   `json:"record_length"`
}

func NewUserRecord(format string, offset uint64, header *RecordHeader, next uint64) *UserRecord {
	return &UserRecord{

		format: format,
		offset: offset,
		header: header,
		next:   next,
	}
}

func (s *UserRecord) String() string {
	return fmt.Sprintf("[record_type => %v, format => %v, offset => %v, next => %v, child_page_number=> %v transaction_id=> %v roll_pointer=>%v length=> %v]",
		s.record_type, s.format, s.offset, s.next, s.Child_page_number, s.Transaction_id, s.Roll_pointer, s.Length)
}

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
