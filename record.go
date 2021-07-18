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
	record interface{}

	// header            *RecordHeader
	// offset            uint64
	// length            uint64
	// next              uint64
	// key               uint64
	// row               uint64
	// transaction_id    uint64
	// roll_pointer      uint64
	// child_page_number uint64

	// record_type uint64
	// heap_number uint64
	// n_owned     uint64
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
