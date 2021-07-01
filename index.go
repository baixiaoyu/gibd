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
	nulls       uint64
	lengths     uint64
	externs     uint64
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
	key               uint64
	row               uint64
	sys               uint64
	child_page_number uint64
	transaction_id    uint64
	roll_pointer      uint64
	length            uint64
}

func newUserRecord(format string, offset uint64, header *RecordHeader, next uint64) *UserRecord {
	return &UserRecord{

		format: format,
		offset: offset,
		header: header,
		next:   next,
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
	record_format    map[string]string
}

func newIndex(page *Page) *Index {
	println("new root index")
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
	println("pos_records 84") //这个错了少了10
	println(index.pos_records())
	println("size_record_header6")
	println(index.size_record_header())
	println("size_mum_record_header_additional1")
	println(index.size_mum_record_header_additional())
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
	println("into header set")
	println(index.pos_index_header())
	jsons, _ := json.Marshal(index.Page)
	println(string(jsons))

	n_dir_slots := uint64(index.Page.bufferReadat(int64(index.pos_index_header()), 2))
	println("end n_dir_slots")
	heap_top := uint64(index.Page.bufferReadat(int64(index.pos_index_header())+2, 2))
	n_heap_format := uint64(index.Page.bufferReadat(int64(index.pos_index_header())+4, 2))
	println("page_headerxxxx67")
	println(n_heap_format)
	println(n_heap_format & (1 << 15))
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
	println("into index page,create root page")
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

const min = 1
const max = 999

func (rc *RecordCursor) initial_record(offset uint64) *Record {
	switch offset {
	case min:
		println("into initial_record")
		return rc.Index.min_record()
	case max:
		return rc.Index.max_record()
	default:
		return rc.Index.record(offset)
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
func (rc *RecordCursor) records() []*Record {
	var records []*Record
	if rc.Initial == false {
		return append(records, rc.Record)
	}
	switch rc.Direction {
	case "forward":
		return rc.next_records()
	case "backward":
		return rc.prev_records()
	}
	return nil
}
func (rc *RecordCursor) next_records() []*Record {
	var records []*Record
	//record := rc.Record.record

	// for rec := rc.Index.record(record.next); rec != rc.Index.supremum() && rec.system_record.offset != record.offset; rec = rc.Index.record(rec.system_record.next) {
	// 	records = append(records, rec)

	// }
	return records
}

func (rc *RecordCursor) prev_records() []*Record {
	var records []*Record

	return append(records, newRecord2())
}

func (index *Index) min_record() *Record {

	infimum := index.infimum()
	println("into min record****")
	fmt.Printf("x的类型是%T", infimum.record)
	value, ok := infimum.record.(*SystemRecord)
	if !ok {
		fmt.Println("failed")
		return nil
	}
	println("####")
	println(value.next)
	min := index.record(value.next)

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

func (index *Index) record(offset uint64) *Record {
	if offset == index.pos_infimum() {
		return index.infimum()
	} else if offset == index.pos_supremum() {
		return index.supremum()
	}

	header := index.recordHeader
	println("index----record-----header")
	println(offset)
	println(header.next)
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
	println("========here=======")
	record_format := index.get_record_format()
	println(record_format)
	if record_format != nil {
		this_record.record_type = index.get_record_format()["type"]
		println(this_record.record_type)
	}

	panic(-1)
	return newRecord2()
}
func (index *Index) get_record_format() map[string]string {
	println("====get_record_format=====")
	println(index.record_describer)
	println(index.record_format)

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
	val := reflect.ValueOf(a) //获取reflect.Type类型

	kd := val.Kind() //获取到a对应的类别

	if kd != reflect.Struct {
		fmt.Println("expect struct")
		return nil
	}
	//获取到该结构体有几个字段
	num := val.NumField()
	//fmt.Printf("该结构体有%d个字段\n", num) //4个

	var str_type string
	var str_key string
	var str_row string
	str_key = `"key":[`
	str_row = `"row":[`
	//遍历结构体的所有字段
	for i := 0; i < num; i++ {
		//获取到struct标签，需要通过reflect.Type来获取tag标签的值
		tagVal := typ.Field(i).Tag.Get("json")
		//如果该字段有tag标签就显示，否则就不显示
		if tagVal != "" {
			if tagVal == "tab_type" {
				x := val.Field(i).Interface().(string)
				str_type = `{"tab_type":"` + x + `",`
			} else {
				fieldstr := val.Field(i).Interface().(Field)
				if fieldstr.Is_key {
					str_key += `{"name":"` + fieldstr.FieldName + `",` + `"type":["` + fieldstr.DataType + `","` + fieldstr.IsNull + `"]},`
				} else {
					str_row += `{"name":"` + fieldstr.FieldName + `",` + `"type":["` + fieldstr.DataType + `","` + fieldstr.IsNull + `"]},`
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

func (index *Index) make_record_description() map[string]string {
	println("make_record_description")
	var position [1024]int
	for i := 0; i <= RECORD_MAX_N_FIELDS; i++ {
		position[i] = i
	}
	description := index.get_record_describer()
	fields := make(map[string]string)

	//需要在这里把description格式调整成ruby的格式，统一下后续好处理
	switch value := description.(type) {
	case *SysTablesPrimary:
		// res := value
		// jsons, _ := json.Marshal(res)
		// println(jsons)
		description := description.(*SysTablesPrimary)
		fields["type"] = description.TAB_TYPE

	case *SysIndexesPrimary:
		description := description.(*SysIndexesPrimary)

		println("开始测试用反射屏蔽具体的类型，进行统一的处理")
		ruby_description := restruct_describer(*description)
		//println(ruby_description["key"])
		for k, v := range ruby_description["key"].([]interface{}) {
			fmt.Println("index=", k, "value=", v)
		}
		println("开始测试用反射屏蔽具体的类型，进行统一的处理 done")

		fields["type"] = description.TAB_TYPE

	default:
		fmt.Println("description is of a different type%T", value)
	}

	fields["key"] = ""
	fields["sys"] = ""
	fields["row"] = ""

	res := make(map[string]string)
	res["key"] = "valuexs"
	return res
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

	println("into infimum")
	println(index.pos_infimum())
	println(infimum)
	println(infimum.next)

	return infimum
}

func (index *Index) supremum() *Record {
	supremum := index.system_record(index.pos_supremum())
	return supremum
}

func (index *Index) system_record(offset uint64) *Record {
	println("into system_record")
	header := index.record_header(offset)
	index.recordHeader = header
	println("into system_record header")
	data := index.Page.readbytes(int64(offset), int64(index.size_mum_record()))
	systemrecord := newSystemRecord(offset, header, header.next, data, 0)
	record := newRecord(index.Page, systemrecord)
	println("into system_record record")
	return record
}

func (index *Index) record_header(offset uint64) *RecordHeader {
	header := newRecordHeader()
	var header_len uint64
	println(index.pageHeader.format)
	switch index.pageHeader.format {
	case "compact":

		header.next = uint64(index.Page.bufferReadat(int64(offset)-2, 2))
		bits1 := uint64(index.Page.bufferReadat(int64(offset)-4, 2))
		header.record_type = RECORD_TYPES[bits1&0x07]
		header.heap_number = (bits1 & 0xfff8) >> 3

		bits2 := uint64(index.Page.bufferReadat(int64(offset)-5, 1))
		header.n_owned = bits2 & 0x0f
		header.info_flags = (bits2 & 0xf0) >> 4
		index.record_header_compact_additional(header)
		header_len = 2 + 2 + 1 + 0 //0 代表record_header_compact_additional中处理记录，先不看

	case "redundant":
		println("xxxxxxxxxx")
		println(offset)
		header.next = uint64(index.Page.bufferReadat(int64(offset)-2, 2))

		bits1 := uint64(index.Page.bufferReadat(int64(offset)-5, 3))
		if (bits1 & 1) == 0 {
			header.offset_size = 2
		} else {
			header.offset_size = 1
		}
		header.n_fields = (bits1 & (((1 << 10) - 1) << 1)) >> 1
		header.heap_number = (bits1 & (((1 << 13) - 1) << 11)) >> 11

		bits2 := uint64(index.Page.bufferReadat(int64(offset)-6, 1))
		header.n_owned = bits2 & 0x0f
		header.info_flags = (bits2 & 0xf0) >> 4
		index.record_header_redundant_additional(header)
		header_len = 2 + 3 + 1 + 0 //0 代表record_header_redundant_additional中处理记录，先不看
	}

	header.length = header_len
	return header
}

func (index *Index) record_header_compact_additional(header *RecordHeader) int {
	return 0
}

func (index *Index) record_header_redundant_additional(header *RecordHeader) int {
	return 0
}

func (index *Index) record_cursor(offset uint64, direction string) *RecordCursor {
	println("into cursor")
	return newRecordCursor(index, offset, direction)
}
func (index *Index) each_record() []*Record {

	println("index each_index========")
	println(index.record_describer)
	c := index.record_cursor(min, "forward")

	return c.records()

}
