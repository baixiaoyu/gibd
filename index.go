package main

import (
	"encoding/json"
	"fmt"
	"reflect"
)

type FsegHeader struct {
	leaf     uint64
	internal uint64
}

type RecordHeader struct {
	length      uint64
	next        uint64
	prev        uint64
	record_type string
	heap_number uint64
	n_owned     uint64
	info_flags  uint64
	offset_size uint64
	n_fields    uint64
	nulls       []string
	lengths     map[string]int
	externs     []string
}

func newRecordHeader() *RecordHeader {
	return &RecordHeader{}

}

type SystemRecord struct {
	offset uint64
	header *RecordHeader
	next   uint64
	data   []byte
	length uint64
}

func newSystemRecord(offset uint64, header *RecordHeader, next uint64, data []byte, length uint64) *SystemRecord {

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
	child_page_number uint64
	transaction_id    uint64
	roll_pointer      *Pointer
	length            uint64
}

func (s *UserRecord) String() string {
	return fmt.Sprintf("[record_type => %v, format => %v, offset => %v, next => %v, child_page_number=> %v transaction_id=> %v roll_pointer=>%v length=> %v]",
		s.record_type, s.format, s.offset, s.next, s.child_page_number, s.transaction_id, s.roll_pointer, s.length)
}

func newUserRecord(format string, offset uint64, header *RecordHeader, next uint64) *UserRecord {
	return &UserRecord{

		format: format,
		offset: offset,
		header: header,
		next:   next,
	}
}

type FieldDescriptor struct {
	name       string
	field_type string
	value      interface{}
	extern     *ExternReference
}

func newFieldDescriptor(name string, field_type string, value interface{}, extern *ExternReference) *FieldDescriptor {
	return &FieldDescriptor{
		name:       name,
		field_type: field_type,
		value:      value,
		extern:     extern,
	}
}

type PageHeader struct {
	n_dir_slots        uint64
	heap_top           uint64
	n_heap_format      uint64
	n_heap             uint64
	format             string
	garbage_offset     uint64
	garbage_size       uint64
	last_insert_offset uint64
	direction          uint64
	n_direction        uint64
	n_recs             uint64
	max_trx_id         uint64
	level              uint64
	index_id           uint64
}

// The basic structure of an INDEX page is: FIL header, INDEX header, FSEG
// # header, fixed-width system records (infimum and supremum), user records
// # (the actual data) which grow ascending by offset, free space, the page
// # directory which grows descending by offset, and the FIL trailer.
type Index struct {
	Page             *Page
	recordHeader     *RecordHeader
	systemRecord     SystemRecord
	userRecord       UserRecord
	fileDesc         FieldDescriptor
	fsegHeader       FsegHeader
	pageHeader       PageHeader
	space            *Space
	record_describer interface{}
	root             *Page
	size             uint64
	record_format    map[string]interface{}
}

func newIndex(page *Page) *Index {

	index := &Index{Page: page}

	index.page_header()
	return index
}

func (index *Index) pos_index_header() uint64 {
	return pos_page_body()
}
func (index *Index) size_index_header() uint64 { //36
	return 2 + 2 + 2 + 2 + 2 + 2 + 2 + 2 + 2 + 8 + 2 + 8
}

func (index *Index) pos_fseg_header() uint64 {
	return index.pos_index_header() + index.size_index_header()
}
func (index *Index) size_fseg_header() uint64 {
	return 2 * FsegEntry_SIZE
}

func (index *Index) size_record_header() uint64 {
	switch index.pageHeader.format {
	case "compact":
		return RECORD_NEXT_SIZE + RECORD_COMPACT_BITS_SIZE
	case "redundant":
		return RECORD_NEXT_SIZE + RECORD_REDUNDANT_BITS_SIZE
	}
	return 0
}

func (index *Index) size_mum_record_header_additional() uint64 {

	switch index.pageHeader.format {
	case "compact":
		return 0
	case "redundant":
		return 1
	}
	return 0
}
func (index *Index) size_mum_record() uint64 {
	return 8
}
func (index *Index) pos_infimum() uint64 {

	return index.pos_records() + index.size_record_header() + index.size_mum_record_header_additional()
}

func (index *Index) pos_supremum() uint64 {
	return index.pos_infimum() + index.size_record_header() + index.size_mum_record_header_additional() + index.size_mum_record()
}

func (index *Index) pos_records() uint64 { //10+36+38
	return index.Page.size_fil_header() + index.size_index_header() + index.size_fseg_header()
}

func (index *Index) pos_user_records() uint64 {
	return index.pos_supremum() + index.size_mum_record()
}

func (index *Index) pos_directory() uint64 {
	return index.Page.pos_fil_trailer()
}

func (index *Index) header_space() uint64 {
	return index.pos_user_records()
}

func (index *Index) directory_slots() uint64 {
	return index.pageHeader.n_dir_slots
}
func (index *Index) directory_space() uint64 {
	return index.directory_slots() * PAGE_DIR_SLOT_SIZE
}

func (index *Index) trailer_space() uint64 {
	return index.Page.size_fil_trailer()
}

func (index *Index) free_space() uint64 {
	return index.pageHeader.garbage_size + (index.size - index.Page.size_fil_trailer() - index.directory_space() - index.pageHeader.heap_top)
}

func (index *Index) used_space() uint64 {
	return index.size - index.free_space()
}
func (index *Index) record_space() uint64 {
	return index.used_space() - index.header_space() - index.directory_space() - index.trailer_space()
}

func (index *Index) space_per_record() uint64 {
	if index.pageHeader.n_recs > 0 {
		return index.record_space() / index.pageHeader.n_recs
	} else {
		return 0
	}
}
func (index *Index) page_header() {
	jsons, _ := json.Marshal(index.Page)
	Log.Info("page_header=========>%s", string(jsons))

	n_dir_slots := uint64(index.Page.bufferReadat(int64(index.pos_index_header()), 2))
	heap_top := uint64(index.Page.bufferReadat(int64(index.pos_index_header())+2, 2))
	n_heap_format := uint64(index.Page.bufferReadat(int64(index.pos_index_header())+4, 2))
	garbage_offset := uint64(index.Page.bufferReadat(int64(index.pos_index_header())+6, 2))
	garbage_size := uint64(index.Page.bufferReadat(int64(index.pos_index_header())+8, 2))
	last_insert_offset := uint64(index.Page.bufferReadat(int64(index.pos_index_header())+10, 2))
	direction := uint64(index.Page.bufferReadat(int64(index.pos_index_header())+12, 2))
	n_direction := uint64(index.Page.bufferReadat(int64(index.pos_index_header())+14, 2))
	n_recs := uint64(index.Page.bufferReadat(int64(index.pos_index_header())+16, 2))
	max_trx_id := uint64(index.Page.bufferReadat(int64(index.pos_index_header())+18, 8))
	level := uint64(index.Page.bufferReadat(int64(index.pos_index_header())+26, 2))
	index_id := uint64(index.Page.bufferReadat(int64(index.pos_index_header())+28, 8))

	page_header := PageHeader{n_dir_slots: n_dir_slots, heap_top: heap_top, n_heap_format: n_heap_format,
		garbage_offset: garbage_offset, garbage_size: garbage_size, last_insert_offset: last_insert_offset,
		direction: direction, n_direction: n_direction, n_recs: n_recs, max_trx_id: max_trx_id, level: level, index_id: index_id}

	index.pageHeader = page_header
	index.pageHeader.n_heap = index.pageHeader.n_heap_format & (2<<14 - 1)

	if (index.pageHeader.n_heap_format & (1 << 15)) == 0 {
		index.pageHeader.format = "redundant"
	} else {
		index.pageHeader.format = "compact"
	}
}

func (index *Index) isroot() bool {
	return index.recordHeader.prev == 0 && index.recordHeader.next == 0
}

func (index *Index) isleaf() bool {

	if index.pageHeader.level == 0 {
		return true
	} else {
		return false
	}
}

func (index *Index) page(page_number uint64) *Page {
	page := index.space.page(page_number)
	page.record_describer = index.record_describer
	return page
}

type RecordCursor struct {
	Initial   bool
	Index     *Index
	Direction string
	Record    *Record
}

const min = 0
const max = 4294967295

func (rc *RecordCursor) initial_record(offset uint64) *Record {
	switch offset {
	case min:
		Log.Info("into initial_record\n")
		return rc.Index.min_record()
	case max:
		return rc.Index.max_record()
	default:
		return rc.Index.record(uint64(offset))
	}
}

func newRecordCursor(index *Index, offset uint64, direction string) *RecordCursor {
	Initial := true
	Index := index
	Direction := direction
	a := RecordCursor{Initial: Initial, Index: Index, Direction: Direction}
	a.Record = a.initial_record(offset)
	return &a
}
func (rc *RecordCursor) record() *Record {
	//var records *Record
	if rc.Initial == true {
		rc.Initial = false
		return rc.Record
	}
	switch rc.Direction {
	case "forward":
		return rc.next_record()
	case "backward":
		return rc.prev_record()
	}
	return nil
}

var page_record_cursor_next_record int

func (rc *RecordCursor) next_record() *Record {
	page_record_cursor_next_record = page_record_cursor_next_record + 1
	// Log.Info("next_record1111 this record's offset is========>%+v\n", rc.Record.record.(*UserRecord).offset)
	// Log.Info("next_record_next_record_header_next is========>%+v\n", rc.Record.record.(*UserRecord).header.next)

	rec := rc.Index.record(rc.Record.record.(*UserRecord).header.next)
	// Log.Info("next_record_next_record is========>%+v\n", rec)
	// Log.Info("next_record_next_record_offset is========>%+v\n", rec.record.(*UserRecord).offset)
	// Log.Info("next_record_next_record's_next is========>%+v\n", rec.record.(*UserRecord).header.next)

	var next_record_offset uint64
	var rc_record_offset uint64

	supremum := rc.Index.supremum()
	rc_record_offset = rc.Record.record.(*UserRecord).offset
	switch rec.record.(type) {
	case *UserRecord:

		next_record_offset = rec.record.(*UserRecord).offset
		next_record := rec.record.(*UserRecord)
		if (next_record.header.next == supremum.record.(*SystemRecord).header.next) || next_record_offset == rc_record_offset {
			return nil
		} else {
			return rec
		}
	case *SystemRecord:
		next_record_offset = rec.record.(*SystemRecord).offset
		next_record := rec.record.(*SystemRecord)
		if (next_record.header.next == supremum.record.(*SystemRecord).header.next) || next_record_offset == rc_record_offset {
			return nil
		} else {
			return rec
		}
	}
	// switch rc.Record.record.(type) {
	// case UserRecord:
	// 	rc_record_offset = rc.Record.record.(*UserRecord).offset
	// case SystemRecord:
	// 	rc_record_offset = rc.Record.record.(*SystemRecord).offset
	// }
	// switch rec.record.(type) {
	// case UserRecord:
	// 	next_record_offset = rec.record.(*UserRecord).offset
	// case SystemRecord:
	// 	next_record_offset = rec.record.(*SystemRecord).offset
	// }
	// Log.Info("next_record this record's offset is========>%+v\n", rc_record_offset)
	// Log.Info("next_record next record's offset is========>%+v\n", next_record_offset)

	//Log.Info("next_record this record's page number is========>%+v\n", next_record_offset)
	// Log.Info("next_record supremum header.next is========>%+v\n", supremum.record.(*SystemRecord).header.next)
	// Log.Info("next_record record header.next is========>%+v\n", rec.record.(*UserRecord).header.next)
	return nil
}

func (rc *RecordCursor) prev_record() *Record {
	var records *Record
	return records
}

func (index *Index) record_cursor(offset uint64, direction string) *RecordCursor {

	return newRecordCursor(index, offset, direction)
}
func (index *Index) each_record() []*Record {
	var records []*Record
	rc := index.record_cursor(min, "forward")
	// Log.Info("index each_record,========>%+v\n", rc.Record)
	//Log.Info("index each_record header next,========>%+v\n", rc.Record.record.(*UserRecord).header.next)
	r := rc.record()
	// Log.Info("index_each_record()rows1 ========>%+v\n", r.record.(*UserRecord).row)
	// for i := 0; i < len(r.record.(*UserRecord).row); i++ {
	// 	Log.Info("index_each_record()row1========>%+v\n", r.record.(*UserRecord).row[i])
	// }
	records = append(records, r)
	for ; r != nil; rc.Record, r = r, rc.record() {

		// Log.Info("index_each_record()rows ========>%+v\n", r.record.(*UserRecord).row)
		// for i := 0; i < len(r.record.(*UserRecord).row); i++ {
		// 	Log.Info("index_each_record()row ========>%+v\n", r.record.(*UserRecord).row[i])
		// }

		records = append(records, r)
	}

	Log.Info("each_record_size is ========>%+v\n", len(records))

	return records

}

//Return the minimum record on this page.
func (index *Index) min_record() *Record {

	infimum := index.infimum()

	value, ok := infimum.record.(*SystemRecord)
	if !ok {
		fmt.Println("failed")
		return nil
	}
	Log.Info("min_record infimum.next==========================================>%+v\n", value.next)
	min := index.record(uint64(value.next))

	return min
}

func (index *Index) max_record() *Record {
	//max_cursor := index.record_cursor(index.supremum().system_record.offset, "backward")
	// max := max_cursor.prev_record
	// if max != index.infimum() {
	// 	return max
	// }
	return newRecord2()

}

func (index *Index) record_fields() []*Recordfield {
	var res_arr []*Recordfield

	Log.Info("record_fields,key ==========%v\n", index.record_format["key"])

	key_arr := index.record_format["key"].([]*Recordfield)
	for i := 0; i < len(key_arr); i++ {
		res_arr = append(res_arr, key_arr[i])
	}
	Log.Info("record_fields,sys ==========%v\n", index.record_format["sys"])
	sys_arr := index.record_format["sys"].([]*Recordfield)
	for i := 0; i < len(sys_arr); i++ {
		res_arr = append(res_arr, sys_arr[i])
	}
	Log.Info("record_fields,row ==========%v\n", index.record_format["row"])
	row_arr := index.record_format["row"].([]*Recordfield)
	for i := 0; i < len(row_arr); i++ {
		res_arr = append(res_arr, row_arr[i])
	}

	return res_arr

}

func (index *Index) record(offset uint64) *Record {
	var rec_len uint64
	if offset == index.pos_infimum() {
		return index.infimum()
	} else if offset == index.pos_supremum() {
		return index.supremum()
	}

	header, header_len := index.record_header(offset)
	Log.Info("record() get header=====>%+v\n", header)

	rec_len += header_len

	var next uint64
	if header.next == 0 {
		next = 0
	} else {
		next = header.next
	}
	this_record := newUserRecord(
		index.pageHeader.format,
		offset,
		header,
		next,
	)
	Log.Info("record() this_record_offset =========>%+v\n", offset)
	rf := index.get_record_format()

	index.record_format = rf
	if index.record_format != nil {

		this_record.record_type = rf["tab_type"].(string)
	} else {
		println("record() record_format is nil")
	}
	all_field := index.record_fields()
	keys := []*FieldDescriptor{}
	rows := []*FieldDescriptor{}
	syss := []*FieldDescriptor{}

	Log.Info("record() all_field=====>%+v\n", all_field)
	Log.Info("record() record.header.lengths=====>%+v\n", this_record.header.lengths)

	for i := 0; i < len(all_field); i++ {
		f := all_field[i]
		p := fmap[f.position]
		//get value exception unkown data type===> &{ 0 false}
		Log.Info("record() this_field_offset =========>%+v\n", offset)
		filed_value, len := f.value(offset, this_record, index)
		Log.Info("record() recordfield name, datatype =====>%s, %s", f.name, f.data_type)
		Log.Info("record() recordfield value =====>%s", filed_value)
		offset = offset + len
		var f_name string
		switch f.data_type.(type) {
		case *TransactionIdType:
			f_name = f.data_type.(*TransactionIdType).name
		case *IntegerType:
			f_name = f.data_type.(*IntegerType).name
		}
		fieldDescriptor := newFieldDescriptor(f.name, f_name, filed_value, f.extern(int64(offset), index, this_record))
		switch p {
		case "key":
			keys = append(keys, fieldDescriptor)
		case "row":
			rows = append(rows, fieldDescriptor)
		case "sys":
			syss = append(syss, fieldDescriptor)
		}

	}
	this_record.key = keys
	this_record.row = rows
	this_record.sys = syss

	if index.isleaf() == false {
		this_record.child_page_number = uint64(index.Page.bufferReadat(int64(offset), 4))
		offset = offset + 4
		rec_len += 4
	}

	this_record.length = rec_len

	for i := 0; i < len(this_record.sys); i++ {
		switch this_record.sys[i].name {
		case "DB_TRX_ID":
			// if len(this_record.sys[i].value.(uint64)) == 0 {
			// 	this_record.transaction_id = 0
			// } else {
			// 	this_record.transaction_id = uint64(this_record.sys[i].value.([]uint8)[0])
			// }
			this_record.transaction_id = this_record.sys[i].value.(uint64)
			Log.Info("record this record's transaction_id is =======> %+v\n", this_record.transaction_id)
		case "DB_ROLL_PTR":
			// if len(this_record.sys[i].value.([]uint8)) == 0 {
			// 	this_record.roll_pointer = 0
			// } else {
			// 	this_record.roll_pointer = uint64(this_record.sys[i].value.([]uint8)[0])
			// }
			this_record.roll_pointer = this_record.sys[i].value.(*Pointer)

		}

	}

	return newRecord(index.Page, this_record)
}
func (index *Index) get_record_format() map[string]interface{} {
	if index.record_describer == nil {
		println("get_record_format record_describer is nil")
	}

	if index.record_describer != nil {
		if index.record_format != nil {
			return index.record_format
		} else {
			record_format := index.make_record_description()
			return record_format
		}
	}
	return nil
}

func (index *Index) get_record_describer() interface{} {
	if index.record_describer != nil {
		return index.record_describer
	} else {
		record_describer := index.make_record_describer()
		return record_describer
	}
	return nil

}
func restruct_describer(a interface{}) map[string]interface{} {

	typ := reflect.TypeOf(a)
	val := reflect.ValueOf(a) //??????reflect.Type??????

	kd := val.Kind() //?????????a???????????????

	if kd != reflect.Struct {
		return nil
	}
	//????????????????????????????????????
	num := val.NumField()
	//Log.Info("???????????????%d?????????\n", num) //4???

	var str_type string
	var str_key string
	var str_row string
	str_key = `"key":[`
	str_row = `"row":[`
	//??????????????????????????????
	for i := 0; i < num; i++ {
		//?????????struct?????????????????????reflect.Type?????????tag????????????
		tagVal := typ.Field(i).Tag.Get("json")
		//??????????????????tag????????????????????????????????????
		if tagVal != "" {
			if tagVal == "tab_type" {
				x := val.Field(i).Interface().(string)
				str_type = `{"tab_type":"` + x + `",`
			} else {
				fieldstr := val.Field(i).Interface().(Field)
				if fieldstr.Is_key {
					str_key += `{"name":"` + fieldstr.FieldName + `",` + `"type":["` + fieldstr.DataType + `","` + fieldstr.Properties + `","` + fieldstr.IsNull + `"]},`
				} else {
					str_row += `{"name":"` + fieldstr.FieldName + `",` + `"type":["` + fieldstr.DataType + `","` + fieldstr.Properties + `","` + fieldstr.IsNull + `"]},`
				}
			}
		}
	}

	str_key = str_key[:len(str_key)-1] + `],`
	str_row = str_row[:len(str_row)-1] + `]}`
	//println(str_type + str_key + str_row)
	m := make(map[string]interface{})

	b := []byte(str_type + str_key + str_row)
	err := json.Unmarshal(b, &m)
	if err != nil {
		fmt.Println("Umarshal failed:", err)
		return nil
	}
	//fmt.Println("m:", m)
	return m
}

var fmap = make(map[int]string)

//?????????????????????systable sysindex ???description
func (index *Index) make_record_description() map[string]interface{} {
	var position [1024]int
	for i := 0; i <= RECORD_MAX_N_FIELDS; i++ {
		position[i] = i
	}
	description := index.get_record_describer()
	fields := make(map[string]string)

	var ruby_description map[string]interface{}
	//??????????????????description???????????????ruby????????????????????????????????????
	switch value := description.(type) {
	case *SysTablesPrimary:

		description := description.(*SysTablesPrimary)
		fields["type"] = description.TAB_TYPE

		//?????????ruby??????????????????????????????????????????????????????
		ruby_description = restruct_describer(*description)
		Log.Info("ruby_description key ????????????=======>%v\n", ruby_description["key"])
		var counter int
		counter = 0

		var key_arr []*Recordfield
		for k, v := range ruby_description["key"].([]interface{}) {
			//key_arr = []*Recordfield{}
			Log.Info("index=%d", k, "value=%s", v)
			value := v.(map[string]interface{})
			prop := value["type"].([]interface{})
			var properties string
			for i := 1; i < len(prop); i++ {
				properties += " " + prop[i].(string)
			}
			rf := newRecordfield(position[counter], value["name"].(string), prop[0].(string), properties)
			Log.Info("record() key type_definition =====>%+v\n", prop[0].(string))
			Log.Info("record() key properties =====>%+v\n", properties)

			fmap[counter] = "key"
			key_arr = append(key_arr, rf)
			counter = counter + 1
		}

		ruby_description["key"] = key_arr

		//ruby_description["type"] = description.TAB_TYPE
		var sys_arr []*Recordfield
		if index.isleaf() && ruby_description["tab_type"] == "clustered" {

			DB_TRX_ID := newRecordfield(position[counter], "DB_TRX_ID", "TRX_ID", "NOT_NULL")
			fmap[counter] = "sys"
			counter = counter + 1
			sys_arr = append(sys_arr, DB_TRX_ID)
			DB_ROLL_PTR := newRecordfield(position[counter], "DB_ROLL_PTR", "ROLL_PTR", "NOT_NULL")
			fmap[counter] = "sys"
			counter = counter + 1
			sys_arr = append(sys_arr, DB_ROLL_PTR)
			Log.Info("sys_arr????????????=======>%T\n", sys_arr)
			ruby_description["sys"] = sys_arr
		}

		var row_arr []*Recordfield
		if (index.isleaf() && ruby_description["tab_type"] == "clustered") || (ruby_description["tab_type"] == "secondary") {
			for _, v := range ruby_description["row"].([]interface{}) {
				value := v.(map[string]interface{})
				name := value["name"].(string)
				prop := value["type"].([]interface{})
				var properties string
				for i := 1; i < len(prop); i++ {
					properties += " " + prop[i].(string)
				}
				row := newRecordfield(position[counter], name, prop[0].(string), properties)
				Log.Info("record() row type_definition =====>%+v\n", prop[0].(string))
				Log.Info("record() row properties =====>%+v\n", properties)
				fmap[counter] = "row"
				row_arr = append(row_arr, row)
				counter = counter + 1

			}
			Log.Info("row_arr??????=======>%+v\n", row_arr)
			ruby_description["row"] = row_arr
		}

		Log.Info("make_record_description ruby_description:%s", ruby_description)
		// println("fmap")
		// for k, v := range fmap {
		// 	println(k)
		// 	println(v)
		// }
		return ruby_description
	case *SysIndexesPrimary:
		description := description.(*SysIndexesPrimary)

		//?????????ruby??????????????????????????????????????????????????????
		ruby_description = restruct_describer(*description)
		Log.Info("ruby_description key ????????????=======>%v\n", ruby_description["key"])
		var counter int
		counter = 0

		var key_arr []*Recordfield
		for k, v := range ruby_description["key"].([]interface{}) {

			Log.Info("index=%d", k, "value=%s", v)
			value := v.(map[string]interface{})
			prop := value["type"].([]interface{})
			var properties string
			for i := 1; i < len(prop); i++ {
				properties += " " + prop[i].(string)
			}
			Log.Info("recordfield key is%d----------->%s", position[counter], value["name"].(string))
			rf := newRecordfield(position[counter], value["name"].(string), prop[0].(string), properties)
			Log.Info("record() key type_definition =====>%+v\n", prop[0].(string))
			Log.Info("record() key properties =====>%+v\n", properties)

			fmap[counter] = "key"
			key_arr = append(key_arr, rf)
			counter = counter + 1
		}

		ruby_description["key"] = key_arr

		//ruby_description["type"] = description.TAB_TYPE
		var sys_arr []*Recordfield
		if index.isleaf() && ruby_description["tab_type"] == "clustered" {

			DB_TRX_ID := newRecordfield(position[counter], "DB_TRX_ID", "TRX_ID", "NOT_NULL")
			fmap[counter] = "sys"
			counter = counter + 1
			sys_arr = append(sys_arr, DB_TRX_ID)
			DB_ROLL_PTR := newRecordfield(position[counter], "DB_ROLL_PTR", "ROLL_PTR", "NOT_NULL")
			fmap[counter] = "sys"
			counter = counter + 1
			sys_arr = append(sys_arr, DB_ROLL_PTR)
			Log.Info("sys_arr????????????=======>%T\n", sys_arr)
			ruby_description["sys"] = sys_arr
		}

		var row_arr []*Recordfield
		if (index.isleaf() && ruby_description["tab_type"] == "clustered") || (ruby_description["tab_type"] == "secondary") {
			for _, v := range ruby_description["row"].([]interface{}) {
				value := v.(map[string]interface{})
				name := value["name"].(string)
				prop := value["type"].([]interface{})
				var properties string
				for i := 1; i < len(prop); i++ {
					properties += " " + prop[i].(string)
				}
				row := newRecordfield(position[counter], name, prop[0].(string), properties)
				Log.Info("record() row type_definition =====>%+v\n", prop[0].(string))
				Log.Info("record() row properties =====>%+v\n", properties)
				fmap[counter] = "row"
				row_arr = append(row_arr, row)
				counter = counter + 1

			}
			Log.Info("row_arr??????=======>%+v\n", row_arr)
			ruby_description["row"] = row_arr
		}

		Log.Info("make_record_description ruby_description:%s", ruby_description)
		// println("fmap")
		// for k, v := range fmap {
		// 	println(k)
		// 	println(v)
		// }
		return ruby_description

	default:
		fmt.Println("description is of a different type%T", value)
	}

	return ruby_description
}

func (index *Index) make_record_describer() interface{} {
	if (index.Page.Space != nil) && (index.Page.Space.innodb_system != nil) && index.pageHeader.index_id != 0 {
		record_describer := index.Page.Space.innodb_system.data_dictionary.record_describer_by_index_id(index.pageHeader.index_id)
		return record_describer
	} else if index.Page.Space != nil {
		record_describer := index.Page.Space.record_describer
		return record_describer
	}
	return nil
}

func (index *Index) infimum() *Record {
	infimum := index.system_record(index.pos_infimum())

	// switch infimum.record.(type) {
	// case *UserRecord:
	// 	println(infimum.record.(*UserRecord).header.next)
	// }

	return infimum
}

func (index *Index) supremum() *Record {
	supremum := index.system_record(index.pos_supremum())
	Log.Info("supremum(),next=>%d", supremum.record.(*SystemRecord).header.next)
	return supremum
}

func (index *Index) system_record(offset uint64) *Record {
	header, _ := index.record_header(offset)
	index.recordHeader = header
	data := index.Page.readbytes(int64(offset), int64(index.size_mum_record()))
	systemrecord := newSystemRecord(offset, header, header.next, data, 0)
	record := newRecord(index.Page, systemrecord)
	return record
}

func (index *Index) record_header(offset uint64) (*RecordHeader, uint64) {

	header := newRecordHeader()
	var header_len uint64
	switch index.pageHeader.format {
	case "compact":

		header.next = uint64(index.Page.bufferReadat(int64(offset)-2, 2))
		bits1 := uint64(index.Page.bufferReadat(int64(offset)-4, 2))
		header.record_type = RECORD_TYPES[bits1&0x07]
		header.heap_number = (bits1 & 0xfff8) >> 3

		bits2 := uint64(index.Page.bufferReadat(int64(offset)-5, 1))
		header.n_owned = bits2 & 0x0f
		header.info_flags = (bits2 & 0xf0) >> 4
		index.record_header_compact_additional(header, offset)
		header_len = 2 + 2 + 1 + 0 //0 ??????record_header_compact_additional???????????????????????????

	case "redundant":
		header.next = uint64(index.Page.bufferReadat(int64(offset)-2, 2))
		//bytes := index.Page.readbytes(int64(offset)-2, 2)
		bits1 := uint64(index.Page.bufferReadat(int64(offset)-5, 3))
		if (bits1 & 1) == 0 {
			header.offset_size = 2
		} else {
			header.offset_size = 1
		}

		header.n_fields = (bits1 & (((1 << 10) - 1) << 1)) >> 1
		header.heap_number = (bits1 & (((1 << 13) - 1) << 11)) >> 11

		bits2 := uint64(index.Page.bufferReadat(int64(offset)-6, 1))
		offset = offset - 6
		header.n_owned = bits2 & 0x0f
		header.info_flags = (bits2 & 0xf0) >> 4
		//header.heap_number = (bits1 & (((1 << 13) - 1) << 11)) >> 11

		index.record_header_redundant_additional(header, offset)
		header_len = 2 + 3 + 1 + 0 //0 ??????record_header_redundant_additional???????????????????????????
		header.length = header_len
		Log.Info("header?????????========???%+v\n", header)
		Log.Info("header lengths ?????????========???%+v\n", header.lengths)
		Log.Info("header nulls ?????????========???%+v\n", header.nulls)
		Log.Info("header externs ?????????========???%+v\n", header.externs)
		//println("lengths:%v", header.lengths, "nulls:%v", header.nulls, "externs:%v", header.externs)
		//println("offset_size:", header.offset_size, "n_fields:", header.n_fields, "heap_number:", header.heap_number, "n_owned:", header.n_owned, "info_flags:", header.info_flags, "next:", header.next)
	}

	header.length = header_len
	return header, header_len
}

func (index *Index) record_header_compact_additional(header *RecordHeader, offset uint64) {
	switch header.record_type {
	case "conventional", "node_pointer":
		if index.record_format != nil {
			header.nulls = index.record_header_compact_null_bitmap(offset)
			header.lengths, header.externs = index.record_header_compact_variable_lengths_and_externs(offset, header.nulls)
		}
	}

}

func (index *Index) record_header_compact_null_bitmap(offset uint64) []string {
	//fields := index.record_fields()
	//size = fields.count(is_nullable())
	return []string{}
}

func (index *Index) record_header_compact_variable_lengths_and_externs(offset uint64, header_nulls []string) (map[string]int, []string) {
	return nil, nil

}

func (index *Index) record_header_redundant_additional(header *RecordHeader, offset uint64) {
	lengths := []int{}
	nulls := []bool{}
	externs := []bool{}
	field_offsets := index.record_header_redundant_field_end_offsets(header, offset)
	Log.Info("record_header_redundant_additional??? header.heap number ?????????==================>%v\n", header.heap_number)
	Log.Info("record_header_redundant_additional??? field_offsets ?????????==================>%v\n", field_offsets)
	this_field_offset := 0
	// var next_field_offset int
	for i := 0; i < len(field_offsets); i++ {

		switch header.offset_size {
		case 1:
			next_field_offset := (field_offsets[i] & RECORD_REDUNDANT_OFF1_OFFSET_MASK)
			Log.Info("record_header_redundant_additional??? RECORD_REDUNDANT_OFF1_OFFSET_MASK ?????????==================>%+v\n", RECORD_REDUNDANT_OFF1_OFFSET_MASK)
			Log.Info("record_header_redundant_additional??? field_offsets[i] ?????????==================>%+v\n", field_offsets[i])
			Log.Info("record_header_redundant_additional??? next_field_offset ?????????==================>%+v\n", next_field_offset)
			Log.Info("record_header_redundant_additional??? this_field_offset ?????????==================>%+v\n", this_field_offset)

			lengths = append(lengths, (next_field_offset - this_field_offset))
			nulls = append(nulls, ((field_offsets[i] & RECORD_REDUNDANT_OFF1_NULL_MASK) != 0))
			externs = append(externs, false)
			this_field_offset = next_field_offset
		case 2:
			next_field_offset := (field_offsets[i] & RECORD_REDUNDANT_OFF2_OFFSET_MASK)
			lengths = append(lengths, (next_field_offset - this_field_offset))
			nulls = append(nulls, ((field_offsets[i] & RECORD_REDUNDANT_OFF2_NULL_MASK) != 0))
			externs = append(externs, ((field_offsets[i] & RECORD_REDUNDANT_OFF2_EXTERN_MASK) != 0))
			this_field_offset = next_field_offset
		}

	}
	Log.Info("record_header_redundant_additional??? lengths ?????????==================>%v\n", lengths)
	Log.Info("record_header_redundant_additional??? nulls ?????????==================>%v\n", nulls)
	Log.Info("record_header_redundant_additional??? externs ?????????==================>%v\n", externs)
	Log.Info("record_header_redundant_additional??? record_format ?????????==================>%v\n", index.record_format)
	Log.Info("record_header_redundant_additional??? record_describer ?????????==================>%v\n", index.record_describer)

	index.record_format = index.get_record_format()
	if index.record_format != nil {
		header.lengths = make(map[string]int)
		header.nulls = []string{}
		header.externs = []string{}
		all_fields := index.record_fields()
		Log.Info("record_header_redundant_additional??? all_fields ?????????==================>%v\n", len(all_fields))
		Log.Info("record_header_redundant_additional??? field_offset ?????????==================>%v\n", len(field_offsets))
		for i := 0; i < len(all_fields); i++ {
			f := all_fields[i]
			if f.position >= len(lengths) {
				header.lengths[f.name] = -1
			} else {
				header.lengths[f.name] = lengths[f.position]
			}

			if f.position >= len(nulls) {
				header.nulls = append(header.nulls, "")
			} else {
				if nulls[f.position] {
					header.nulls = append(header.nulls, f.name)
				}
			}
			if f.position >= len(externs) {
				header.externs = append(header.externs, "")
			} else {
				if externs[f.position] {
					header.externs = append(header.externs, f.name)
				}
			}

		}
	} else {
		println("index page record_format is nil!!!!!!")
		// ???????????????????????????????????????????????????panic
		panic(-1)
		// header.lengths = lengths
		// header.nulls = nulls
		// header.externs = externs
	}
	Log.Info("record_header_redundant_additional??? header.lengths ?????????==================>%v\n", header.lengths)
	Log.Info("record_header_redundant_additional??? header.nulls ?????????==================>%v\n", header.nulls)
	Log.Info("record_header_redundant_additional??? header.externs ?????????==================>%v\n", header.externs)

}

func (index *Index) record_header_redundant_field_end_offsets(header *RecordHeader, offset uint64) []int {
	field_offsets := []int{}
	Log.Info("record_header_redundant_field_end_offsets offset ?????????==================>%v\n", offset)
	for i := 0; i < int(header.n_fields); i++ {
		field_offsets = append(field_offsets, index.Page.bufferReadat(int64(offset)-1, int64(header.offset_size)))
		Log.Info("record_header_redundant_field_end_offsets page number ???==================>%v\n", index.Page.Page_number)

		Log.Info("record_header_redundant_field_end_offsets field_offsets ?????????==================>%v\n", field_offsets)
		offset = offset - header.offset_size
	}
	return field_offsets
}
