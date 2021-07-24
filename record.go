package main

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

// type FieldDescriptor struct {
// 	name      string
// 	desc_type uint64
// 	value     uint64
// 	extern    uint64
// }
type FsegEntry struct {
}

type Record struct {
	Page   *Page
	record interface{} //UserRecord or SystemRecord
	//	fields map[string]interface{}
}

func newRecord(page *Page, record interface{}) *Record {
	return &Record{
		Page:   page,
		record: record,
	}
}

func newRecord2() *Record {
	return &Record{}
}

func (record *Record) get_fields() map[string]interface{} {
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
