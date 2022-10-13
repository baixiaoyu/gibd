package gibd

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/tidwall/pretty"
)

// https://blog.jcole.us/2013/01/07/the-physical-structure-of-innodb-index-pages/
//分叶子和非叶子段
type FsegHeader struct {
	LpiSpaceId      uint64 `json:"leafpagesinodespaceid"`
	LpiPageNumber   uint64 `json:"leafpagesinodepagenumber"`
	Lpioffset       uint64 `json:"leafpagesinodeoffset"`
	InodeSpaceId    uint64 `json:"nonleafinodespaceid"`
	InodePageNumber uint64 `json:"nonleafinodepagenumber"`
	InodeOffset     uint64 `json:"nonleafinodepageoffset"`
}

func NewFsegHeader() *FsegHeader {
	return &FsegHeader{}

}

type PageHeader struct {
	N_dir_slots        uint64 `json:"n_dir_slots"`
	Heap_top           uint64 `json:"heap_top"`
	N_heap_format      uint64 `json:"n_heap_format"`
	N_heap             uint64 `json:"n_heap"`
	Format             string `json:"format"`
	Garbage_offset     uint64 `json:"garbage_offset"`
	Garbage_size       uint64 `json:"garbage_size"`
	Last_insert_offset uint64 `json:"last_insert_offset"`
	Direction          uint64 `json:"direction"`
	N_direction        uint64 `json:"n_direction"`
	N_recs             uint64 `json:"n_recs"`
	Max_trx_id         uint64 `json:"max_trx_id"`
	Level              uint64 `json:"level"`
	Index_id           uint64 `json:"index_id"`
}

// The basic structure of an INDEX page is: FIL header, INDEX header, FSEG
// # header, fixed-width system records (infimum and supremum), user records
// # (the actual data) which grow ascending by offset, free space, the page
// # directory which grows descending by offset, and the FIL trailer.
type Index struct {
	Page *Page
	// recordHeader     *RecordHeader
	SystemRecords    []SystemRecord `json:"systemrecords"`
	UserRecords      []UserRecord   `json:"userrecords"`
	fileDesc         FieldDescriptor
	FsegHeader       FsegHeader `json:"fsegheader"`
	PageHeader       PageHeader `json:"pageheader"`
	Space            *Space
	record_describer interface{} //这个跟record_format是什么关系？用来格式化Record_Format
	root             *Page
	size             uint64
	Record_Format    map[string]interface{} `json:"recordformat"` // 这个用来描述字段信息，主要用来在解析的时候遇到字段变长信息的处理
	dh               *DataDictionary        //系统表空间的时候使用，用来获取一些元信息，如果是普通的表空间，构造record_format即可
}

func NewIndex(page *Page) *Index {

	index := &Index{Page: page}
	index.Space = page.Space
	index.Index_Header()
	return index
}

func (index *Index) Pos_Index_Header() uint64 {
	return Pos_Page_Body()
}
func (index *Index) Size_Index_Header() uint64 { //36
	return 2 + 2 + 2 + 2 + 2 + 2 + 2 + 2 + 2 + 8 + 2 + 8
}

func (index *Index) Pos_Fseg_Header() uint64 {
	return index.Pos_Index_Header() + index.Size_Index_Header()
}
func (index *Index) Size_Fseg_Header() uint64 {
	return 2 * FsegEntry_SIZE
}

//compact 2+3
func (index *Index) Size_Record_Header() uint64 {
	switch index.PageHeader.Format {
	case "compact":
		return RECORD_NEXT_SIZE + RECORD_COMPACT_BITS_SIZE
	case "redundant":
		return RECORD_NEXT_SIZE + RECORD_REDUNDANT_BITS_SIZE
	}
	return 0
}

func (index *Index) Size_Mum_Record_Header_Additional() uint64 {

	switch index.PageHeader.Format {
	case "compact":
		return 0
	case "redundant":
		return 1
	}
	return 0
}
func (index *Index) Size_Mum_Record() uint64 {
	return 8
}
func (index *Index) Pos_Infimum() uint64 {
	a := index.Pos_Records()
	b := index.Size_Record_Header()
	c := index.Size_Mum_Record_Header_Additional()
	// return index.Pos_Records() + index.Size_Record_Header() + index.Size_Mum_Record_Header_Additional()
	return a + b + c
}

func (index *Index) Pos_Supremum() uint64 {
	return index.Pos_Infimum() + index.Size_Record_Header() + index.Size_Mum_Record_Header_Additional() + index.Size_Mum_Record()
}

func (index *Index) Pos_Records() uint64 { //20+36+38
	return index.Page.Size_Fil_Header() + index.Size_Index_Header() + index.Size_Fseg_Header()
}

func (index *Index) Pos_User_Records() uint64 {
	return index.Pos_Supremum() + index.Size_Mum_Record()
}

func (index *Index) Pos_Directory() uint64 {
	return index.Page.Pos_Fil_Trailer()
}

func (index *Index) Header_Space() uint64 {
	return index.Pos_User_Records()
}

func (index *Index) Directory_Slots() uint64 {
	return index.PageHeader.N_dir_slots
}
func (index *Index) Directory_Space() uint64 {
	return index.Directory_Slots() * PAGE_DIR_SLOT_SIZE
}

func (index *Index) Trailer_Space() uint64 {
	return index.Page.Size_Fil_Trailer()
}

func (index *Index) Free_Space() uint64 {
	return index.PageHeader.Garbage_size + (index.size - index.Page.Size_Fil_Trailer() - index.Directory_Space() - index.PageHeader.Heap_top)
}

func (index *Index) Used_Space() uint64 {
	return index.size - index.Free_Space()
}
func (index *Index) Record_Space() uint64 {
	return index.Used_Space() - index.Header_Space() - index.Directory_Space() - index.Trailer_Space()
}

func (index *Index) Space_Per_Record() uint64 {
	if index.PageHeader.N_recs > 0 {
		return index.Record_Space() / index.PageHeader.N_recs
	} else {
		return 0
	}
}

// index_header
func (index *Index) Index_Header() {
	// jsons, _ := json.Marshal(index.Page)

	n_dir_slots := uint64(BufferReadAt(index.Page, int64(index.Pos_Index_Header()), 2))
	heap_top := uint64(BufferReadAt(index.Page, int64(index.Pos_Index_Header())+2, 2))
	n_heap_format := uint64(BufferReadAt(index.Page, int64(index.Pos_Index_Header())+4, 2))
	garbage_offset := uint64(BufferReadAt(index.Page, int64(index.Pos_Index_Header())+6, 2))
	garbage_size := uint64(BufferReadAt(index.Page, int64(index.Pos_Index_Header())+8, 2))
	last_insert_offset := uint64(BufferReadAt(index.Page, int64(index.Pos_Index_Header())+10, 2))
	direction := uint64(BufferReadAt(index.Page, int64(index.Pos_Index_Header())+12, 2))
	n_direction := uint64(BufferReadAt(index.Page, int64(index.Pos_Index_Header())+14, 2))
	n_recs := uint64(BufferReadAt(index.Page, int64(index.Pos_Index_Header())+16, 2))
	max_trx_id := uint64(BufferReadAt(index.Page, int64(index.Pos_Index_Header())+18, 8))
	level := uint64(BufferReadAt(index.Page, int64(index.Pos_Index_Header())+26, 2))
	index_id := uint64(BufferReadAt(index.Page, int64(index.Pos_Index_Header())+28, 4))

	page_header := PageHeader{N_dir_slots: n_dir_slots, Heap_top: heap_top, N_heap_format: n_heap_format,
		Garbage_offset: garbage_offset, Garbage_size: garbage_size, Last_insert_offset: last_insert_offset,
		Direction: direction, N_direction: n_direction, N_recs: n_recs, Max_trx_id: max_trx_id, Level: level, Index_id: index_id}

	index.PageHeader = page_header
	index.PageHeader.N_heap = index.PageHeader.N_heap_format & (2<<14 - 1)

	if (index.PageHeader.N_heap_format & (1 << 15)) == 0 {
		index.PageHeader.Format = "redundant"
	} else {
		index.PageHeader.Format = "compact"
	}

}

func (index *Index) Fseg_Header() {
	//get fseg header,put them together,index的叶子和非叶子节点使用的是2个segment管理
	// pos 74开始
	pos := int64(index.Pos_Fseg_Header())
	inodeSpaceId := uint64(BufferReadAt(index.Page, pos, 4)) //leaf inode
	pos = pos + 4
	inodePageNumer := uint64(BufferReadAt(index.Page, pos, 4))
	pos = pos + 4
	inodeOffset := uint64(BufferReadAt(index.Page, pos, 2))
	pos = pos + 2

	nonLeafInodeSpaceId := uint64(BufferReadAt(index.Page, pos, 4)) //none leaf inode
	pos = pos + 4
	nonLeafInodePageNumber := uint64(BufferReadAt(index.Page, pos, 4))
	pos = pos + 4
	nonLeafInodeOffset := uint64(BufferReadAt(index.Page, pos, 2))
	pos = pos + 2

	fsegHeader := NewFsegHeader()
	fsegHeader.LpiSpaceId = inodeSpaceId
	fsegHeader.LpiPageNumber = inodePageNumer
	fsegHeader.Lpioffset = inodeOffset

	fsegHeader.InodeSpaceId = nonLeafInodeSpaceId
	fsegHeader.InodePageNumber = nonLeafInodePageNumber
	fsegHeader.InodeOffset = nonLeafInodeOffset

	index.FsegHeader = *fsegHeader
}

func (index *Index) Page_Directory() {
	pos := int64(index.Pos_Directory())

	numSlot := index.Directory_Slots()

	for i := uint64(0); i < numSlot; i++ {
		slot := uint64(BufferReadAt(index.Page, pos-2, 2))
		pos = pos - 2
		fmt.Printf("page direcotry slot %d,:%v\n", i, slot)
	}
}

func (index *Index) Is_Root() bool {
	return index.Page.FileHeader.Prev == 0 && index.Page.FileHeader.Next == 0

}

func (index *Index) IsLeaf() bool {

	if index.PageHeader.Level == 0 {
		return true
	} else {
		return false
	}
}

func (index *Index) page(page_number uint64) *Page {
	page := index.Space.Page(page_number)
	page.record_describer = index.record_describer
	return page
}

// var page_record_cursor_next_record int

func (index *Index) each_record() []*Record {
	var records []*Record

	rc := index.Record_Cursor(min, "forward")
	r := rc.record()
	records = append(records, r)

	for ; r != nil; rc.Record, r = r, rc.record() {
		records = append(records, r)
	}
	Log.Info("each_record_size is ========>%+v\n", len(records))

	return records

}

//Return the minimum record on this page.不是Infimum,是用户的最小值
func (index *Index) Min_Record() *Record {

	infimum := index.Infimum()

	value, ok := infimum.record.(*SystemRecord)
	if !ok {
		fmt.Println("failed")
		return nil
	}

	min := index.record(uint64(value.next))

	return min
}

func (index *Index) Max_Record() *Record {
	//max_cursor := index.record_cursor(index.supremum().system_record.offset, "backward")
	// max := max_cursor.prev_record
	// if max != index.infimum() {
	// 	return max
	// }
	return NewRecord2()

}

func (index *Index) Record_Fields() []*RecordField {
	var res_arr []*RecordField

	// Log.Info("record_fields,key ==========%v\n", index.Record_Format["key"])
	// fmt.Printf("record_fields,key %v\n", index.Record_Format["key"])

	//添加判断，如果没有record_format就是表示普通的表空间，普通表空间没有办法获取字段类型的
	if index.Record_Format == nil {
		return nil
	}
	key_arr := index.Record_Format["key"].([]*RecordField)
	for i := 0; i < len(key_arr); i++ {
		res_arr = append(res_arr, key_arr[i])
	}
	Log.Info("record_fields record_format ==========%v\n", index.Record_Format)
	if index.Record_Format["sys"] != nil {
		sys_arr := index.Record_Format["sys"].([]*RecordField)
		for i := 0; i < len(sys_arr); i++ {
			res_arr = append(res_arr, sys_arr[i])
		}
	}

	Log.Info("record_fields,row ==========%v\n", index.Record_Format["row"])
	// fmt.Printf("record_fields,row %v\n", index.Record_Format["row"])
	if index.Record_Format["row"] != nil {
		row_arr := index.Record_Format["row"].([]*RecordField)
		for i := 0; i < len(row_arr); i++ {
			res_arr = append(res_arr, row_arr[i])
		}
	}

	return res_arr

}

func (index *Index) record(offset uint64) *Record {
	var rec_len uint64

	if offset == index.Pos_Infimum() {
		return index.Infimum()
	} else if offset == index.Pos_Supremum() {
		return index.Supremum()
	}

	header, header_len := index.Record_Header(offset)

	rec_len += header_len

	var next uint64
	if header.Next == 0 {
		next = 0
	} else {
		next = header.Next
	}

	this_record := NewUserRecord(
		index.PageHeader.Format,
		offset,
		header,
		next,
	)

	rf := index.Get_Record_Format()

	index.Record_Format = rf
	if index.Record_Format != nil {

		this_record.record_type = rf["tab_type"].(string)
	}
	all_field := index.Record_Fields()
	if all_field == nil {
		//没有字段元数据的时候，获取下记录的长度的准确值
		// 为了测试方便，我们使用固定的表结构
		// Create Table: CREATE TABLE `dba_user5` (
		// 	`id` int(11) NOT NULL AUTO_INCREMENT,
		// 	`username` varchar(100) DEFAULT NULL COMMENT '用户名',
		// 	`class` varchar(100) DEFAULT NULL COMMENT 'class',
		// 	`account` int(11) DEFAULT NULL,
		// 	`version` int(11) DEFAULT NULL,
		// 	PRIMARY KEY (`id`)
		//   ) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8 ROW_FORMAT=COMPACT
		//   1 row in set (0.00 sec)

		//   master [localhost] {msandbox} (test) > select * from dba_user5;
		//   +----+----------+-------+---------+---------+
		//   | id | username | class | account | version |
		//   +----+----------+-------+---------+---------+
		//   |  1 | cccccc   | ll    |    NULL |      30 |
		//   +----+----------+-------+---------+---------+
		fmt.Println("offset ", offset)
		// cluster_key_fileds := (index.Page.BufferReadAt(int64(offset), 4))

		bytes := ReadBytes(index.Page, int64(offset), 4)

		cluster_key_filed := ParseMySQLInt(index, bytes)
		fmt.Println("cluster key fileds ==", cluster_key_filed)

		transaction_id := BufferReadAt(index.Page, int64(offset)+4, 6)

		fmt.Println("transaction_id ==", transaction_id)

		roll_pointer := BufferReadAt(index.Page, int64(offset)+10, 7)
		fmt.Println("roll pointer ==", roll_pointer)

		username := ReadBytes(index.Page, int64(offset)+17, 2)
		fmt.Println(" value1==", string(username))

		bytes = ReadBytes(index.Page, int64(offset)+19, 4)
		class := ParseMySQLInt(index, bytes)
		fmt.Println(" value2==", (class))

		bytes = ReadBytes(index.Page, int64(offset)+23, 4)
		account := ParseMySQLInt(index, bytes)

		fmt.Println(" value3==", (account))

		bytes = ReadBytes(index.Page, int64(offset)+29, 4)
		version := ParseMySQLInt(index, bytes)
		fmt.Println(" value4==", (version))

		return NewRecord(index.Page, this_record)
	} else {

		keys := []*FieldDescriptor{}
		rows := []*FieldDescriptor{}
		syss := []*FieldDescriptor{}

		Log.Info("record() record.header.lengths=====>%+v\n", this_record.header.Lengths)

		for i := 0; i < len(all_field); i++ {
			f := all_field[i]
			p := fmap[f.position]
			//get value exception unkown data type===> &{ 0 false}
			Log.Info("record() this_field_offset =========>%+v\n", offset)
			filed_value, len := f.Value(offset, this_record, index)
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
			fieldDescriptor := NewFieldDescriptor(f.name, f_name, filed_value, f.extern(int64(offset), index, this_record))
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

		if index.IsLeaf() == false {
			//child_page_number是在最后的4个字节，前面是最小key的值,先写死
			this_record.Child_page_number = uint64(BufferReadAt(index.Page, int64(offset)+16, 4))
			fmt.Println("child_page_number==", this_record.Child_page_number)
			offset = offset + 4
			rec_len += 4
		}

		this_record.Length = rec_len

		for i := 0; i < len(this_record.sys); i++ {
			switch this_record.sys[i].name {
			case "DB_TRX_ID":
				// if len(this_record.sys[i].value.(uint64)) == 0 {
				// 	this_record.transaction_id = 0
				// } else {
				// 	this_record.transaction_id = uint64(this_record.sys[i].value.([]uint8)[0])
				// }
				this_record.Transaction_id = this_record.sys[i].value.(uint64)
				Log.Info("record this record's transaction_id is =======> %+v\n", this_record.Transaction_id)
			case "DB_ROLL_PTR":
				// if len(this_record.sys[i].value.([]uint8)) == 0 {
				// 	this_record.roll_pointer = 0
				// } else {
				// 	this_record.roll_pointer = uint64(this_record.sys[i].value.([]uint8)[0])
				// }
				this_record.Roll_pointer = this_record.sys[i].value.(*Pointer)

			}

		}
	}
	return NewRecord(index.Page, this_record)
}

func (index *Index) Get_Record_Format() map[string]interface{} {
	// if index.record_describer == nil {
	// 	println("get_record_format record_describer is nil")
	// }

	// if index.record_describer != nil {
	// 	if index.Record_format != nil {
	// 		return index.Record_format
	// 	} else {
	// 		record_format := index.Make_Record_Description()
	// 		return record_format
	// 	}
	// }

	if index.Record_Format != nil {
		return index.Record_Format
	} else {
		record_format := index.Make_Record_Description()
		return record_format
	}
	return nil
}

func (index *Index) Get_Record_Describer() interface{} {
	if index.record_describer != nil {
		return index.record_describer
	} else {
		record_describer := index.Make_Record_Describer()
		index.record_describer = record_describer
		return record_describer
	}
	return nil

}

func Restruct_Describer(a interface{}) map[string]interface{} {

	typ := reflect.TypeOf(a)
	val := reflect.ValueOf(a) //获取reflect.Type类型

	kd := val.Kind() //获取到a对应的类别

	if kd != reflect.Struct {
		return nil
	}
	//获取到该结构体有几个字段
	num := val.NumField()
	//Log.Info("该结构体有%d个字段\n", num) //4个

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

// 记录描述符这应该重构下，描述符这有点混乱
//只实现了系统表systable sysindex 的description
func (index *Index) Make_Record_Description() map[string]interface{} {
	var position [1024]int
	for i := 0; i <= RECORD_MAX_N_FIELDS; i++ {
		position[i] = i
	}
	description := index.Get_Record_Describer() //用之前的描述符，更改下格式
	fields := make(map[string]string)

	var ruby_description map[string]interface{}
	//需要在这里把description格式调整成ruby的格式，统一下后续好处理

	switch description.(type) {
	case *SysTablesPrimary:

		description := description.(*SysTablesPrimary)
		fields["type"] = description.TAB_TYPE

		//转化成ruby那样的格式，统一下，要不后续不好处理
		ruby_description = Restruct_Describer(*description)
		Log.Info("ruby_description key 的内容是=======>%v\n", ruby_description["key"])
		var counter int
		counter = 0

		var key_arr []*RecordField
		for k, v := range ruby_description["key"].([]interface{}) {
			//key_arr = []*Recordfield{}
			Log.Info("index=%v", k, "value=%v", v)
			value := v.(map[string]interface{})
			prop := value["type"].([]interface{})
			var properties string
			for i := 1; i < len(prop); i++ {
				properties += " " + prop[i].(string)
			}
			rf := NewRecordField(position[counter], value["name"].(string), prop[0].(string), properties)
			Log.Info("record() key type_definition =====>%+v\n", prop[0].(string))
			Log.Info("record() key properties =====>%+v\n", properties)

			fmap[counter] = "key"
			key_arr = append(key_arr, rf)
			counter = counter + 1
		}

		ruby_description["key"] = key_arr

		//ruby_description["type"] = description.TAB_TYPE
		var sys_arr []*RecordField
		if index.IsLeaf() && ruby_description["tab_type"] == "clustered" {

			DB_TRX_ID := NewRecordField(position[counter], "DB_TRX_ID", "TRX_ID", "NOT_NULL")
			fmap[counter] = "sys"
			counter = counter + 1
			sys_arr = append(sys_arr, DB_TRX_ID)
			DB_ROLL_PTR := NewRecordField(position[counter], "DB_ROLL_PTR", "ROLL_PTR", "NOT_NULL")
			fmap[counter] = "sys"
			counter = counter + 1
			sys_arr = append(sys_arr, DB_ROLL_PTR)
			Log.Info("sys_arr的类型是=======>%T\n", sys_arr)
			ruby_description["sys"] = sys_arr
		}

		var row_arr []*RecordField
		if (ruby_description["tab_type"] == "clustered") || (ruby_description["tab_type"] == "secondary") {
			for _, v := range ruby_description["row"].([]interface{}) {
				value := v.(map[string]interface{})
				name := value["name"].(string)
				prop := value["type"].([]interface{})
				var properties string
				for i := 1; i < len(prop); i++ {
					properties += " " + prop[i].(string)
				}
				row := NewRecordField(position[counter], name, prop[0].(string), properties)
				Log.Info("record() row type_definition =====>%+v\n", prop[0].(string))
				Log.Info("record() row properties =====>%+v\n", properties)
				fmap[counter] = "row"
				row_arr = append(row_arr, row)
				counter = counter + 1

			}
			Log.Info("row_arr的值=======>%+v\n", row_arr)
			ruby_description["row"] = row_arr
		}

		Log.Info("make_record_description_SysTablesPrimary ruby_description:%s", ruby_description)
		// println("fmap")
		// for k, v := range fmap {
		// 	println(k)
		// 	println(v)
		// }
		return ruby_description
	case *SysIndexesPrimary:
		description := description.(*SysIndexesPrimary)
		fmt.Println("\n((( description:\n", description)
		//转化成ruby那样的格式，统一下，要不后续不好处理
		ruby_description = Restruct_Describer(*description)
		fmt.Printf("ruby_description  的内容是=======>%v\n", ruby_description)
		var counter int
		counter = 0

		var key_arr []*RecordField
		for k, v := range ruby_description["key"].([]interface{}) {

			fmt.Printf("\nindex=%v ,value=%v\n", k, v)
			value := v.(map[string]interface{})
			prop := value["type"].([]interface{})
			var properties string
			for i := 1; i < len(prop); i++ {
				properties += " " + prop[i].(string)
			}
			Log.Info("recordfield key is%d----------->%s", position[counter], value["name"].(string))
			rf := NewRecordField(position[counter], value["name"].(string), prop[0].(string), properties)
			Log.Info("record() key type_definition =====>%+v\n", prop[0].(string))
			Log.Info("record() key properties =====>%+v\n", properties)

			fmap[counter] = "key"
			key_arr = append(key_arr, rf)
			counter = counter + 1
		}

		ruby_description["key"] = key_arr

		var sys_arr []*RecordField
		// 叶子结点加上回滚段和事务id的值
		if index.IsLeaf() && ruby_description["tab_type"] == "clustered" {

			DB_TRX_ID := NewRecordField(position[counter], "DB_TRX_ID", "TRX_ID", "NOT_NULL")
			fmap[counter] = "sys"
			counter = counter + 1
			sys_arr = append(sys_arr, DB_TRX_ID)
			DB_ROLL_PTR := NewRecordField(position[counter], "DB_ROLL_PTR", "ROLL_PTR", "NOT_NULL")
			fmap[counter] = "sys"
			counter = counter + 1
			sys_arr = append(sys_arr, DB_ROLL_PTR)
			Log.Info("sys_arr的类型是=======>%T\n", sys_arr)
			ruby_description["sys"] = sys_arr
		}

		var row_arr []*RecordField
		if (ruby_description["tab_type"] == "clustered") || (ruby_description["tab_type"] == "secondary") {
			for _, v := range ruby_description["row"].([]interface{}) {
				value := v.(map[string]interface{})
				name := value["name"].(string)
				prop := value["type"].([]interface{})
				var properties string
				for i := 1; i < len(prop); i++ {
					properties += " " + prop[i].(string)
				}
				row := NewRecordField(position[counter], name, prop[0].(string), properties)
				Log.Info("record() row type_definition =====>%+v\n", prop[0].(string))
				Log.Info("record() row properties =====>%+v\n", properties)
				fmap[counter] = "row"
				row_arr = append(row_arr, row)
				counter = counter + 1

			}
			Log.Info("row_arr的值=======>%+v\n", row_arr)
			ruby_description["row"] = row_arr
		}

		fmt.Printf("\nmake_record_description_SysIndexesPrimary ruby_description:%s\n", ruby_description)
		// println("fmap")
		// for k, v := range fmap {
		// 	println(k)
		// 	println(v)
		// }
		return ruby_description

	default:
		//  fmt.Println("description is of a different type%T", value)
		fmt.Printf("\n")
	}

	return ruby_description
}

func (index *Index) Make_Record_Describer() interface{} {
	if (index.Page.Space != nil) && index.Space.IsSystemSpace && index.PageHeader.Index_id != 0 {
		record_describer := Record_Describer_By_Index_Id(index.dh, index.PageHeader.Index_id)
		return record_describer
	} else if index.Page.Space != nil {
		record_describer := index.Page.Space.Record_describer
		return record_describer
	}
	return nil
}

func (index *Index) Infimum() *Record {
	infimum := index.System_Record(index.Pos_Infimum())

	// switch infimum.record.(type) {
	// case *UserRecord:
	// 	println(infimum.record.(*UserRecord).header.next)
	// }

	return infimum
}

func (index *Index) Supremum() *Record {
	supremum := index.System_Record(index.Pos_Supremum())
	Log.Info("supremum(),next=>%d", supremum.record.(*SystemRecord).header.Next)
	return supremum
}

func (index *Index) System_Record(offset uint64) *Record {

	header, _ := index.Record_Header(offset)
	// index.recordHeader = header
	data := ReadBytes(index.Page, int64(offset), int64(index.Size_Mum_Record()))
	systemrecord := NewSystemRecord(offset, header, header.Next, data, 0)
	record := NewRecord(index.Page, systemrecord)
	return record
}

func (index *Index) Record_Header(offset uint64) (*RecordHeader, uint64) {

	header := NewRecordHeader(offset)

	var header_len uint64
	switch index.PageHeader.Format {
	case "compact":
		// 这个next是相对的offset, 需要加上当前的offset
		header.Next = uint64(BufferReadAtToSignInt(index.Page, int64(offset)-2, 2)) + offset

		bits1 := uint64(BufferReadAt(index.Page, int64(offset)-4, 2))

		header.Record_Type = RECORD_TYPES[bits1&0x07]
		header.Heap_Number = (bits1 & 0xfff8) >> 3

		bits2 := uint64(BufferReadAt(index.Page, int64(offset)-5, 1))
		header.N_owned = bits2 & 0x0f
		header.Info_flags = (bits2 & 0xf0) >> 4
		//用户记录去查additional
		if header.Record_Type == "conventional" {
			fmt.Println("offset", offset-5)
			index.Record_Header_Compact_Additional(header, offset-5)
		}

		header_len = 2 + 2 + 1 + 0 //0 代表record_header_compact_additional中处理记录

	case "redundant":
		header.Next = uint64(BufferReadAt(index.Page, int64(offset)-2, 2))
		//bytes := index.Page.readbytes(int64(offset)-2, 2)
		bits1 := uint64(BufferReadAt(index.Page, int64(offset)-5, 3))
		if (bits1 & 1) == 0 {
			header.Offset_size = 2
		} else {
			header.Offset_size = 1
		}

		header.N_fields = (bits1 & (((1 << 10) - 1) << 1)) >> 1
		header.Heap_Number = (bits1 & (((1 << 13) - 1) << 11)) >> 11

		bits2 := uint64(BufferReadAt(index.Page, int64(offset)-6, 1))
		offset = offset - 6
		header.N_owned = bits2 & 0x0f
		header.Info_flags = (bits2 & 0xf0) >> 4
		//header.heap_number = (bits1 & (((1 << 13) - 1) << 11)) >> 11

		index.Record_Header_Redundant_Additional(header, offset)
		header_len = 2 + 3 + 1 + 0 //0 代表record_header_redundant_additional中处理记录，先不看
		header.Length = header_len

	}

	header.Length = header_len

	data, _ := json.Marshal(header)
	outStr := pretty.Pretty(data)

	fmt.Printf("record header: %s\n", outStr)

	return header, header_len
}

func (index *Index) Record_Header_Compact_Additional(header *RecordHeader, offset uint64) {
	switch header.Record_Type {
	// node_pointer 是中间节点记录 conventional 是正常的记录
	case "conventional", "node_pointer":
		// 变长部分，如果没有列的元数据信息，没法取长度，所以如果自己知道表信息，可以手工设置字节长度，在else中判断
		if index.Record_Format != nil {
			header.Nulls = index.Record_Header_Compact_Null_Bitmap(offset)
			header.Lengths, header.Externs = index.Record_Header_Compact_Variable_Lengths_And_Externs(offset, header.Nulls)
		} else {
			//人为决定获取长度处理
			fmt.Println("offset for null", offset)
			offset = offset - 2

			header.Nulls = index.Record_Header_Compact_Null_Bitmap(offset)
			header.Lengths, header.Externs = index.Record_Header_Compact_Variable_Lengths_And_Externs(offset, header.Nulls)

		}
	}

}

func (index *Index) Record_Header_Compact_Null_Bitmap(offset uint64) string {
	//fields := index.record_fields()
	//size = fields.count(is_nullable())
	//方便测试，将null bitmap和extern的信息放在了一起，默认测试分别占用1个字节。

	nulls := ReadBytes(index.Page, int64(offset), 1)

	nullString := BytesToBinaryString(nulls)
	return nullString
}

func (index *Index) Record_Header_Compact_Variable_Lengths_And_Externs(offset uint64, header_nulls string) (map[string]int, string) {
	return nil, ""

}

func (index *Index) Record_Header_Redundant_Additional(header *RecordHeader, offset uint64) {
	lengths := []int{}
	nulls := []bool{}
	externs := []bool{}
	field_offsets := index.Record_Header_Redundant_Field_End_Offsets(header, offset)
	Log.Info("record_header_redundant_additional的 header.heap number 内容是==================>%v\n", header.Heap_Number)
	Log.Info("record_header_redundant_additional的 field_offsets 内容是==================>%v\n", field_offsets)
	this_field_offset := 0
	// var next_field_offset int
	for i := 0; i < len(field_offsets); i++ {

		switch header.Offset_size {
		case 1:
			next_field_offset := (field_offsets[i] & RECORD_REDUNDANT_OFF1_OFFSET_MASK)
			Log.Info("record_header_redundant_additional的 RECORD_REDUNDANT_OFF1_OFFSET_MASK 内容是==================>%+v\n", RECORD_REDUNDANT_OFF1_OFFSET_MASK)
			Log.Info("record_header_redundant_additional的 field_offsets[i] 内容是==================>%+v\n", field_offsets[i])
			Log.Info("record_header_redundant_additional的 next_field_offset 内容是==================>%+v\n", next_field_offset)
			Log.Info("record_header_redundant_additional的 this_field_offset 内容是==================>%+v\n", this_field_offset)

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
	Log.Info("record_header_redundant_additional的 lengths 内容是==================>%v\n", lengths)
	Log.Info("record_header_redundant_additional的 nulls 内容是==================>%v\n", nulls)
	Log.Info("record_header_redundant_additional的 externs 内容是==================>%v\n", externs)
	Log.Info("record_header_redundant_additional的 record_format 内容是==================>%v\n", index.Record_Format)
	Log.Info("record_header_redundant_additional的 record_describer 内容是==================>%v\n", index.record_describer)

	index.Record_Format = index.Get_Record_Format()

	if index.Record_Format != nil {
		header.Lengths = make(map[string]int)
		header.Nulls = ""
		header.Externs = ""
		all_fields := index.Record_Fields()
		Log.Info("record_header_redundant_additional的 all_fields 内容是==================>%v\n", len(all_fields))
		Log.Info("record_header_redundant_additional的 field_offset 长度是==================>%v\n", len(field_offsets))
		for i := 0; i < len(all_fields); i++ {
			f := all_fields[i]
			if f.position >= len(lengths) {
				header.Lengths[f.name] = -1
			} else {
				header.Lengths[f.name] = lengths[f.position]
			}

			if f.position >= len(nulls) {
				header.Nulls = header.Nulls + ""
			} else {
				if nulls[f.position] {
					header.Nulls = header.Nulls + f.name
				}
			}
			if f.position >= len(externs) {
				header.Externs = header.Externs + ""
			} else {
				if externs[f.position] {
					header.Externs = header.Externs + f.name
				}
			}

		}
	} else {
		println("index page record_format is nil!!!!!!")
		// 还不知道什么情况出触发这个条件，先panic
		panic(-1)
		// header.lengths = lengths
		// header.nulls = nulls
		// header.externs = externs
	}
	Log.Info("record_header_redundant_additional的 header.lengths 内容是==================>%v\n", header.Lengths)
	Log.Info("record_header_redundant_additional的 header.nulls 内容是==================>%v\n", header.Nulls)
	Log.Info("record_header_redundant_additional的 header.externs 内容是==================>%v\n", header.Externs)

}

func (index *Index) Record_Header_Redundant_Field_End_Offsets(header *RecordHeader, offset uint64) []int {
	field_offsets := []int{}
	Log.Info("record_header_redundant_field_end_offsets offset 内容是==================>%v\n", offset)
	for i := 0; i < int(header.N_fields); i++ {
		field_offsets = append(field_offsets, BufferReadAt(index.Page, int64(offset)-1, int64(header.Offset_size)))
		Log.Info("record_header_redundant_field_end_offsets page number 是==================>%v\n", index.Page.Page_number)

		Log.Info("record_header_redundant_field_end_offsets field_offsets 内容是==================>%v\n", field_offsets)
		offset = offset - header.Offset_size
	}
	return field_offsets
}

func (f *Index) Dump() {
	println("Index dump:")

	data, _ := json.Marshal(f)
	outStr := pretty.Pretty(data)
	fmt.Printf("%s\n", outStr)
	f.Page_Directory()
}
