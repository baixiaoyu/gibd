package gibd

import (
	"encoding/json"
	"fmt"
	"sort"

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
//IndexPge
type IndexPage struct {
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

func NewIndex(page *Page) *IndexPage {

	index := &IndexPage{Page: page}
	index.Space = page.Space
	index.Index_Header()
	return index
}

func (index *IndexPage) Pos_Index_Header() uint64 {
	return Pos_Page_Body()
}
func (index *IndexPage) Size_Index_Header() uint64 { //36
	return 2 + 2 + 2 + 2 + 2 + 2 + 2 + 2 + 2 + 8 + 2 + 8
}

func (index *IndexPage) Pos_Fseg_Header() uint64 {
	return index.Pos_Index_Header() + index.Size_Index_Header()
}
func (index *IndexPage) Size_Fseg_Header() uint64 {
	return 2 * FsegEntry_SIZE
}

//compact 2+3
func (index *IndexPage) Size_Record_Header() uint64 {
	switch index.PageHeader.Format {
	case "compact":
		return RECORD_NEXT_SIZE + RECORD_COMPACT_BITS_SIZE
	case "redundant":
		return RECORD_NEXT_SIZE + RECORD_REDUNDANT_BITS_SIZE
	}
	return 0
}

func (index *IndexPage) Size_Mum_Record_Header_Additional() uint64 {

	switch index.PageHeader.Format {
	case "compact":
		return 0
	case "redundant":
		return 1
	}
	return 0
}
func (index *IndexPage) Size_Mum_Record() uint64 {
	return 8
}
func (index *IndexPage) Pos_Infimum() uint64 {
	a := index.Pos_Records()
	b := index.Size_Record_Header()
	c := index.Size_Mum_Record_Header_Additional()
	// return index.Pos_Records() + index.Size_Record_Header() + index.Size_Mum_Record_Header_Additional()
	return a + b + c
}

func (index *IndexPage) Pos_Supremum() uint64 {
	return index.Pos_Infimum() + index.Size_Record_Header() + index.Size_Mum_Record_Header_Additional() + index.Size_Mum_Record()
}

func (index *IndexPage) Pos_Records() uint64 { //20+36+38
	return index.Page.Size_Fil_Header() + index.Size_Index_Header() + index.Size_Fseg_Header()
}

func (index *IndexPage) Pos_User_Records() uint64 {
	return index.Pos_Supremum() + index.Size_Mum_Record()
}

func (index *IndexPage) Pos_Directory() uint64 {
	return index.Page.Pos_Fil_Trailer()
}

func (index *IndexPage) Header_Space() uint64 {
	return index.Pos_User_Records()
}

func (index *IndexPage) Directory_Slots() uint64 {
	return index.PageHeader.N_dir_slots
}
func (index *IndexPage) Directory_Space() uint64 {
	return index.Directory_Slots() * PAGE_DIR_SLOT_SIZE
}

func (index *IndexPage) Trailer_Space() uint64 {
	return index.Page.Size_Fil_Trailer()
}

func (index *IndexPage) Free_Space() uint64 {
	return index.PageHeader.Garbage_size + (index.size - index.Page.Size_Fil_Trailer() - index.Directory_Space() - index.PageHeader.Heap_top)
}

func (index *IndexPage) Used_Space() uint64 {
	return index.size - index.Free_Space()
}
func (index *IndexPage) Record_Space() uint64 {
	return index.Used_Space() - index.Header_Space() - index.Directory_Space() - index.Trailer_Space()
}

func (index *IndexPage) Space_Per_Record() uint64 {
	if index.PageHeader.N_recs > 0 {
		return index.Record_Space() / index.PageHeader.N_recs
	} else {
		return 0
	}
}

// index_header
func (index *IndexPage) Index_Header() {
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

func (index *IndexPage) Fseg_Header() {
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

func (index *IndexPage) Page_Directory() {
	pos := int64(index.Pos_Directory())

	numSlot := index.Directory_Slots()

	for i := uint64(0); i < numSlot; i++ {
		slot := uint64(BufferReadAt(index.Page, pos-2, 2))
		pos = pos - 2
		fmt.Printf("page direcotry slot %d,:%v\n", i, slot)
	}
}

func (index *IndexPage) Is_Root() bool {
	return index.Page.FileHeader.Prev == 0 && index.Page.FileHeader.Next == 0

}

func (index *IndexPage) IsLeaf() bool {

	if index.PageHeader.Level == 0 {
		return true
	} else {
		return false
	}
}

func (index *IndexPage) page(page_number uint64) *Page {
	page := index.Space.Page(page_number)
	page.record_describer = index.record_describer
	return page
}

func (index *IndexPage) each_record() []*Record {
	var records []*Record

	rc := index.Record_Cursor(min, "forward")
	r := rc.record()
	records = append(records, r)

	for ; r != nil; rc.Record, r = r, rc.record() {
		records = append(records, r)
	}

	return records

}

//Return the minimum record on this page.不是Infimum,是用户的最小值
func (index *IndexPage) Min_Record() *Record {

	infimum := index.Infimum()

	value, ok := infimum.record.(*SystemRecord)
	if !ok {
		fmt.Println("failed")
		return nil
	}

	min := index.record(uint64(value.next))

	return min
}

//获取用户的最大值
func (index *IndexPage) Max_Record() *Record {
	//max_cursor := index.record_cursor(index.supremum().system_record.offset, "backward")
	// max := max_cursor.prev_record
	// if max != index.infimum() {
	// 	return max
	// }
	return NewRecord2()

}

func (index *IndexPage) Get_Record_Fields_From_Format() ([]*RecordFieldMeta, []*RecordFieldMeta, []*RecordFieldMeta) {
	var res_arr []*RecordFieldMeta
	//添加判断，如果没有record_format就是表示普通的表空间，普通表空间没有办法获取字段类型的
	if index.Record_Format == nil {
		return nil, nil, nil
	}
	key_arr := index.Record_Format["key"].([]*RecordFieldMeta)
	for i := 0; i < len(key_arr); i++ {
		res_arr = append(res_arr, key_arr[i])
	}

	if index.Record_Format["sys"] != nil {
		sys_arr := index.Record_Format["sys"].([]*RecordFieldMeta)
		for i := 0; i < len(sys_arr); i++ {
			res_arr = append(res_arr, sys_arr[i])
		}
	}

	row_arr := index.Record_Format["row"].([]*RecordFieldMeta)
	// fmt.Printf("record_fields,row %v\n", index.Record_Format["row"])
	if index.Record_Format["row"] != nil {
		row_arr := index.Record_Format["row"].([]*RecordFieldMeta)
		for i := 0; i < len(row_arr); i++ {
			res_arr = append(res_arr, row_arr[i])
		}
	}

	return res_arr, key_arr, row_arr

}

func (index *IndexPage) record(offset uint64) *Record {
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
	all_field, key_arr, row_arr := index.Get_Record_Fields_From_Format()
	if all_field == nil {
		//读取建表语句记录，然后根据header获取的具体长度信息，获取字段的值，然后创建记录对象
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
		fmt.Println("header:")

		data, _ := json.Marshal(this_record.header)
		outStr := pretty.Pretty(data)
		fmt.Printf("%s\n", outStr)

		// cluster_key_fileds := (index.Page.BufferReadAt(int64(offset), 4))

		bytes := ReadBytes(index.Page, int64(offset), 4)

		cluster_key_filed := ParseMySQLInt(index, bytes)
		fmt.Println("cluster key fileds ==", cluster_key_filed)

		transaction_id := BufferReadAt(index.Page, int64(offset)+4, 6)

		fmt.Println("transaction_id ==", transaction_id)

		roll_pointer := BufferReadAt(index.Page, int64(offset)+10, 7)
		fmt.Println("roll pointer ==", roll_pointer)

		username := ReadBytes(index.Page, int64(offset)+17, 6)
		fmt.Println(" value1==", string(username))

		class := ReadBytes(index.Page, int64(offset)+23, 2)
		fmt.Println(" value2==", string(class))

		bytes = ReadBytes(index.Page, int64(offset)+25, 4)
		account := ParseMySQLInt(index, bytes)
		fmt.Println(" value3==", (account))

		//datetime没有微妙，用的是5个字节，不是8个，这里需要判断，否则后面的字段解析的都不对了
		bytes = ReadBytes(index.Page, int64(offset)+29, 5)
		// year := bytes[]

		dt := ParseMySQLDateTime(bytes)
		fmt.Println("value4", dt.String())
		// version := ParseMySQLInt(index, bytes)

		bytes = ReadBytes(index.Page, int64(offset)+34, 4)

		ctime := ParseMySQLTimeStamp(bytes)
		fmt.Println("value5==", (ctime.value))

		return NewRecord(index.Page, this_record)
	} else {
		record_offset := offset
		keys := []*FieldDescriptor{}
		rows := []*FieldDescriptor{}
		syss := []*FieldDescriptor{}

		sort.Sort(FiledSort(key_arr))

		sort.Sort(FiledSort(row_arr))
		//待修改，获取记录的值，这部分需要分叶子结点和非叶子结点分别处理，非叶子结点只需要获取key值，获取child_page_number
		// https://blog.jcole.us/2013/01/10/the-physical-structure-of-records-in-innodb/
		if index.IsLeaf() == true {
			//先获取key字段，然后是transaction id 然后是roll pointer 然后是non-key字段
			var keyLen uint64
			for i := 0; i < len(key_arr); i++ {
				f := key_arr[i]
				p := fmap[f.Position]

				filed_value, len := f.Value(offset, this_record, index)

				keyLen = keyLen + len

				offset = offset + len
				var f_name string
				switch f.DataType.(type) {
				case *TransactionIdType:
					f_name = f.DataType.(*TransactionIdType).name
				case *IntegerType:
					f_name = f.DataType.(*IntegerType).name
				}
				fieldDescriptor := NewFieldDescriptor(f.Name, f_name, filed_value, f.extern(int64(offset), index, this_record))
				switch p {
				case "key":
					keys = append(keys, fieldDescriptor)
				case "row":
					rows = append(rows, fieldDescriptor)
				case "sys":
					syss = append(syss, fieldDescriptor)
				}

			}

			//获取事务id,回滚段指针
			// transaction_id := BufferReadAt(index.Page, int64(offset)+int64(keyLen), 6)

			// fmt.Println("transaction_id ==", transaction_id)

			// roll_pointer := BufferReadAt(index.Page, int64(offset)+int64(keyLen)+6, 7)
			// fmt.Println("roll pointer ==", roll_pointer)

			//获取non-key field
			offset = offset + 13

			for i := 0; i < len(row_arr); i++ {
				f := row_arr[i]
				p := fmap[f.Position]
				//get value exception unkown data type===> &{ 0 false}

				filed_value, len := f.Value(offset, this_record, index)
				offset = offset + len

				var f_name string
				switch f.DataType.(type) {
				case *TransactionIdType:
					f_name = f.DataType.(*TransactionIdType).name
				case *IntegerType:
					f_name = f.DataType.(*IntegerType).name
				}
				fieldDescriptor := NewFieldDescriptor(f.Name, f_name, filed_value, f.extern(int64(offset), index, this_record))
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
			//叶子结点的系统字段
			for i := 0; i < len(this_record.sys); i++ {
				switch this_record.sys[i].FieldMeta.Name {
				case "DB_TRX_ID":
					// if len(this_record.sys[i].value.(uint64)) == 0 {
					// 	this_record.transaction_id = 0
					// } else {
					// 	this_record.transaction_id = uint64(this_record.sys[i].value.([]uint8)[0])
					// }
					this_record.Transaction_id = this_record.sys[i].Value.(uint64)
					Log.Info("record this record's transaction_id is =======> %+v\n", this_record.Transaction_id)
				case "DB_ROLL_PTR":
					// if len(this_record.sys[i].value.([]uint8)) == 0 {
					// 	this_record.roll_pointer = 0
					// } else {
					// 	this_record.roll_pointer = uint64(this_record.sys[i].value.([]uint8)[0])
					// }
					this_record.Roll_pointer = this_record.sys[i].Value.(*Pointer)

				}

			}
		}

		//叶子结点，记录值是key和child_page_number
		if index.IsLeaf() == false {

			var keyLen uint64
			for i := 0; i < len(key_arr); i++ {
				f := key_arr[i]

				_, len := f.Value(offset, this_record, index)
				offset = offset + len
				keyLen = keyLen + len

			}
			//child_page_number是在最后的4个字节，前面是最小key的值,这里key的信息需要在描述符中获取
			// fmt.Println("offset==?", record_offset)
			this_record.Child_page_number = uint64(BufferReadAt(index.Page, int64(record_offset)+int64(keyLen), 4))

			offset = offset + 4
			rec_len += 4
		}

		this_record.Length = rec_len
	}
	return NewRecord(index.Page, this_record)
}

func (index *IndexPage) Get_Record_Format() map[string]interface{} {

	if index.Record_Format != nil {
		return index.Record_Format
	} else {
		record_format := index.Make_Record_Description()
		return record_format
	}
	return nil
}

func (index *IndexPage) Get_Record_Describer() interface{} {
	if index.record_describer != nil {
		return index.record_describer
	} else {
		record_describer := index.Make_Record_Describer()
		index.record_describer = record_describer
		return record_describer
	}
	return nil

}

var fmap = make(map[int]string)

func (index *IndexPage) Make_Record_Describer() interface{} {
	if (index.Page.Space != nil) && index.Space.IsSystemSpace && index.PageHeader.Index_id != 0 {
		record_describer := Record_Describer_By_Index_Id(index.dh, index.PageHeader.Index_id)
		return record_describer
	} else if index.Page.Space != nil {
		record_describer := index.Page.Space.Record_describer
		return record_describer
	}
	return nil
}

func (index *IndexPage) Infimum() *Record {
	infimum := index.System_Record(index.Pos_Infimum())

	return infimum
}

func (index *IndexPage) Supremum() *Record {
	supremum := index.System_Record(index.Pos_Supremum())
	return supremum
}

func (index *IndexPage) System_Record(offset uint64) *Record {

	header, _ := index.Record_Header(offset)
	// index.recordHeader = header
	data := ReadBytes(index.Page, int64(offset), int64(index.Size_Mum_Record()))
	systemrecord := NewSystemRecord(offset, header, header.Next, data, 0)
	record := NewRecord(index.Page, systemrecord)
	return record
}

func (index *IndexPage) Record_Header(offset uint64) (*RecordHeader, uint64) {

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

	Log.Info("record header: %s\n", outStr)

	return header, header_len
}

func (index *IndexPage) Record_Header_Compact_Additional(header *RecordHeader, offset uint64) {
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

func (index *IndexPage) Record_Header_Compact_Null_Bitmap(offset uint64) string {
	//fields := index.record_fields()
	//size = fields.count(is_nullable())
	//方便测试，将null bitmap和extern的信息放在了一起，默认测试分别占用1个字节，根据具体情况修改，因为没有字段元数据信息

	nulls := ReadBytes(index.Page, int64(offset), 2)

	nullString := BytesToBinaryString(nulls)
	return nullString
}

func (index *IndexPage) Record_Header_Compact_Variable_Lengths_And_Externs(offset uint64, header_nulls string) (map[string]int, string) {
	return nil, ""

}

func (index *IndexPage) Record_Header_Redundant_Additional(header *RecordHeader, offset uint64) {
	lengths := []int{}
	nulls := []bool{}
	externs := []bool{}
	field_offsets := index.Record_Header_Redundant_Field_End_Offsets(header, offset)

	this_field_offset := 0
	// var next_field_offset int
	for i := 0; i < len(field_offsets); i++ {

		switch header.Offset_size {
		case 1:
			next_field_offset := (field_offsets[i] & RECORD_REDUNDANT_OFF1_OFFSET_MASK)

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

	index.Record_Format = index.Get_Record_Format()

	if index.Record_Format != nil {
		header.Lengths = make(map[string]int)
		header.Nulls = ""
		header.Externs = ""
		all_fields, _, _ := index.Get_Record_Fields_From_Format()
		for i := 0; i < len(all_fields); i++ {
			f := all_fields[i]
			if f.Position >= len(lengths) {
				header.Lengths[f.Name] = -1
			} else {
				header.Lengths[f.Name] = lengths[f.Position]
			}

			if f.Position >= len(nulls) {
				header.Nulls = header.Nulls + ""
			} else {
				if nulls[f.Position] {
					header.Nulls = header.Nulls + f.Name
				}
			}
			if f.Position >= len(externs) {
				header.Externs = header.Externs + ""
			} else {
				if externs[f.Position] {
					header.Externs = header.Externs + f.Name
				}
			}

		}
	} else {
		println("index page record_format is nil!!!!!!")
		// 还不知道什么情况出触发这个条件，先panic
		panic(-1)

	}

}

func (index *IndexPage) Record_Header_Redundant_Field_End_Offsets(header *RecordHeader, offset uint64) []int {
	field_offsets := []int{}
	for i := 0; i < int(header.N_fields); i++ {
		field_offsets = append(field_offsets, BufferReadAt(index.Page, int64(offset)-1, int64(header.Offset_size)))
		offset = offset - header.Offset_size
	}
	return field_offsets
}

func (f *IndexPage) Dump() {
	println("Index dump:")

	data, _ := json.Marshal(f)
	outStr := pretty.Pretty(data)
	fmt.Printf("%s\n", outStr)
	f.Page_Directory()
}
