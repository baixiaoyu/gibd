package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"reflect"
	"strconv"
	"strings"
	"unsafe"
)

const DEFAULT_PAGE_SIZE = 16 * 1024
const DEFAULT_EXTENT_SIZE = 64 * DEFAULT_PAGE_SIZE
const SYSTEM_SPACE_ID = 0

var SYSTEM_SPACE_PAGE_MAP = map[int]string{
	0: "FSP_HDR",
	1: "IBUF_BITMAP",
	2: "INODE",
	3: "SYS",
	4: "INDEX",
	5: "TRX_SYS",
	6: "SYS",
	7: "SYS",
}
var PAGE_DIRECTION = map[int]string{
	1: "left",         // Inserts have been in descending order.
	2: "right",        // Inserts have been in ascending order.
	3: "same_rec",     // Unused by InnoDB.
	4: "same_page",    // Unused by InnoDB.
	5: "no_direction", // Inserts have been in random order.
}

var RECORD_TYPES = map[int]string{
	0: "conventional", // A normal user record in a leaf page.
	1: "node_pointer", // A node pointer in a non-leaf page.
	2: "infimum",      // The system "infimum" record.
	3: "supremum",     // The system "supremum" record.
}
var XDES_LISTS = [...]string{
	"free",
	"free_frag",
	"full_frag",
}

var INODE_LISTS = [...]string{
	"full_inodes",
	"free_inodes",
}

type Address struct {
	Page   uint64 `json:"page"`
	Offset uint64 `json:"offset"`
}
type FilHeader struct {
	Checksum  uint64 `json:"checksum"`
	Offset    uint64 `json:"offset"`
	Prev      uint64 `json:"prev"`
	Next      uint64 `json:"next"`
	Lsn       uint64 `json:"lsn"`
	Page_type uint64 `json:"page_type"`
	Flush_lsn uint64 `json:"flush_lsn"`
	Space_id  uint64 `json:"space_id"`
}

func (s *FilHeader) lsn_low32(offset uint64) uint64 {
	return s.Lsn & 0xffffffff
}

func (filHeader FilHeader) String() string {
	jsons, _ := json.Marshal(filHeader)
	println(string(jsons))
	return string(jsons)
}

type FilTrailer struct {
	checksum  uint64
	lsn_low32 uint64
}

func (filTrailer FilTrailer) String() string {

	res := "checksum:" + strconv.FormatUint(filTrailer.checksum, 10) + ",offset:" + strconv.FormatUint(filTrailer.lsn_low32, 10)
	return res
}

type Region struct {
	offset uint64
	length uint64
	name   string
	info   string
}

var specialized_classes = map[uint64]string{
	6: "SYS",
}

type SYS struct {
	Page
}

type Page struct {
	Address     Address    `json:"address"`
	FileHeader  FilHeader  `json:"fileheader"`
	FileTrailer FilTrailer `json:"filetrailer"`
	Region      Region     `json:"region"`
	Space       *Space     `json:"space"`
	Buffer      *[]byte    `json:"-"`
	Page_number uint64     `json:"page_number"`
}

func newPage(space *Space, buffer *[]byte, page_number uint64) *Page {
	return &Page{
		Space:       space,
		Buffer:      buffer,
		Page_number: page_number,
	}

}

func (p *Page) page_dump() {
	println()
	fmt.Println("fil header:")

	p.fil_header()
	p.fil_trailer()
	jsons, _ := json.Marshal(p)
	println(string(jsons))

	println()
	if p.FileHeader.Page_type == 6 {
		dict_header := newSysDataDictionaryHeader(p)
		dict_header.dump()
	}

}

func (p *Page) bufferReadat(offset int64, size int64) int {

	byteStorage := make([]byte, size)
	byteReader := bytes.NewReader(*p.Buffer)
	byteReader.ReadAt(byteStorage, offset)

	return p.BytesToUIntLittleEndian(byteStorage)
}

func (p *Page) BytesToUIntLittleEndian(b []byte) int {

	if len(b) == 3 {
		b = append([]byte{0}, b...)
	}
	bytesBuffer := bytes.NewBuffer(b)
	switch len(b) {
	case 1:
		var tmp uint8
		binary.Read(bytesBuffer, binary.BigEndian, &tmp)
		return int(tmp)
	case 2:
		var tmp uint16
		binary.Read(bytesBuffer, binary.BigEndian, &tmp)
		return int(tmp)
	case 4:
		var tmp uint32
		binary.Read(bytesBuffer, binary.BigEndian, &tmp)
		return int(tmp)

	case 8:
		var tmp uint64
		binary.Read(bytesBuffer, binary.BigEndian, &tmp)
		return int(tmp)
	default:
		return 0
	}
}

func (p Page) String() string {

	page_offset := p.bufferReadat(4, 4)
	page_type := p.bufferReadat(24, 2)
	res := "page: " + strconv.Itoa(page_offset) + ",type=" + PAGE_TYPE[page_type]
	return res
}

func (p *Page) pos_fil_header() int {
	return 0
}

func (p *Page) fil_header() {

	p.FileHeader.Checksum = uint64(p.bufferReadat(int64(p.pos_fil_header()), 4))
	p.FileHeader.Offset = uint64(p.bufferReadat(int64(p.pos_fil_header())+4, 4))
	p.FileHeader.Prev = uint64(p.bufferReadat(int64(p.pos_fil_header())+8, 4))
	p.FileHeader.Next = uint64(p.bufferReadat(int64(p.pos_fil_header())+12, 4))
	p.FileHeader.Lsn = uint64(p.bufferReadat(int64(p.pos_fil_header())+16, 8))
	p.FileHeader.Page_type = uint64(p.bufferReadat(int64(p.pos_fil_header())+24, 2))
	p.FileHeader.Flush_lsn = uint64(p.bufferReadat(int64(p.pos_fil_header())+26, 8))
	p.FileHeader.Space_id = uint64(p.bufferReadat(int64(p.pos_fil_header())+34, 4))
}

func (p *Page) fil_trailer() {
	p.FileTrailer.checksum = uint64(p.bufferReadat(int64(p.pos_fil_trailer()), 4))
	p.FileTrailer.lsn_low32 = uint64(p.bufferReadat(int64(p.pos_fil_trailer())+4, 4))
}

func (p *Page) size_fil_header() int {
	return 4 + 4 + 4 + 4 + 8 + 2 + 8 + 4
}

func (p *Page) pos_partial_page_header() int {
	return p.pos_fil_header() + 4
}

func (p *Page) size_partial_page_header() int {
	return p.size_fil_header() - 4 - 8 - 4
}
func (p *Page) size_fil_trailer() int {
	return 4 + 4
}

func (p *Page) pos_fil_trailer() int {
	return DEFAULT_PAGE_SIZE - p.size_fil_trailer()
}

func (p *Page) pos_page_body() int {
	return p.pos_fil_header() + p.size_fil_header()
}

func (p *Page) size_page_body() int {
	return DEFAULT_PAGE_SIZE - p.size_fil_trailer() - p.size_fil_header()
}

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

type RecordHeader struct {
	length      uint64
	next        uint64
	prev        uint64
	record_type uint64
	heap_number uint64
	n_owned     uint64
	info_flags  uint64
	offset_size uint64
	n_fields    uint64
	nulls       uint64
	lengths     uint64
	externs     uint64
}
type SystemRecord struct {
	offset uint64
	header uint64
	next   uint64
	data   uint64
	length uint64
}

type UserRecord struct {
	record_type       uint64
	format            uint64
	offset            uint64
	header            uint64
	next              uint64
	key               uint64
	row               uint64
	sys               uint64
	child_page_number uint64
	transaction_id    uint64
	roll_pointer      uint64
	length            uint64
}

type FieldDescriptor struct {
	name      string
	desc_type uint64
	value     uint64
	extern    uint64
}

type FsegHeader struct {
	leaf     uint64
	internal uint64
}

type PageHeader struct {
	n_dir_slots        uint64
	heap_top           uint64
	n_heap_format      uint64
	n_heap             uint64
	format             uint64
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
	Page
	recordHeader RecordHeader
	systemRecord SystemRecord
	userRecord   UserRecord
	fileDesc     FieldDescriptor
	fsegHeader   FsegHeader
	pageHeader   PageHeader
}

func (index *Index) isroot() bool {
	return index.recordHeader.prev == 0 && index.recordHeader.next == 0
}

type SYS_TABLES struct {
	PRIMARY uint64 `json:"primary"`
	ID      uint64 `json:"id"`
}
type SYS_COLUMNS struct {
	PRIMARY uint64 `json:"primary"`
}
type SYS_INDEXES struct {
	PRIMARY uint64 `json:"primary"`
}
type SYS_FIELDS struct {
	PRIMARY uint64 `json:"primary"`
}

type Dict_Index struct {
	Sys_tables  SYS_TABLES  `json:"sys_tables"`
	Sys_columns SYS_COLUMNS `json:"sys_columns"`
	Sys_indexes SYS_INDEXES `json:"sys_indexes"`
	Sys_fields  SYS_FIELDS  `json:"sys_fields"`
}

type DataDictionary struct {
	system_space *System
}

func newDataDictionary(system_space *System) *DataDictionary {
	return &DataDictionary{system_space: system_space}
}
func (dh *DataDictionary) each_table() {
	dh.each_record_from_data_dictionary_index("Sys_tables", "PRIMARY")
}

func (dh *DataDictionary) each_record_from_data_dictionary_index(table string, index string) {
	dh.data_dictionary_index(table, index)
}

func (dh *DataDictionary) CheckNestedStruct(table_entry interface{}, table_name string, index_name string, find_table bool, find_all bool) uint64 {
	// find_table := false
	println("&&")
	data, _ := json.Marshal(table_entry)
	println(string(data))
	println(find_all)
	var res uint64

	e := reflect.ValueOf(table_entry).Elem()
	for i := 0; i < e.NumField(); i++ {
		field := e.Field(i)
		varName := e.Type().Field(i).Name
		//varType := e.Type().Field(i).Type
		fieldType := field.Kind()
		if find_table && (varName == index_name) {
			varValue := e.Field(i).Interface()
			find_all = true

			switch value := varValue.(type) {
			case uint64:
				res = value
				return res
				//record_describer := data_dictionary_index_describer(table_name, index_name)
				//dh.system_space.index(index_root_page, record_describer)
			case string:
				fmt.Printf("list is a string and its value is %s\n", value)
			default:
				fmt.Println("list is of a different type%s", value)
			}

			//return varValue
		}
		//fmt.Printf("%v  %v\n", varName, fieldType)
		if fieldType == reflect.Struct {
			if varName == table_name {
				find_table = true

			} else {
				find_table = false
			}
			if !find_all {

				res = res + dh.CheckNestedStruct(field.Addr().Interface(), table_name, index_name, find_table, find_all)
			}

		}
	}

	return res

}
func (dh *DataDictionary) data_dictionary_index(table_name string, index_name string) {
	table_entry := dh.data_dictionary_indexes(table_name)
	res := dh.CheckNestedStruct(&table_entry, table_name, index_name, false, false)
	fmt.Printf("xxxxxx的类型是%T", res)
	println(res)
	// switch value := res.(type) {
	// case int:
	// 	index_root_page := value
	// 	println(index_root_page)
	// 	//record_describer := data_dictionary_index_describer(table_name, index_name)
	// 	//dh.system_space.index(index_root_page, record_describer)
	// case string:
	// 	fmt.Printf("list is a string and its value is %s\n", value)
	// default:
	// 	fmt.Println("list is of a different type%s", value)
	// }

}

func (dh *DataDictionary) data_dictionary_indexes(table_name string) Dict_Index {
	page := dh.system_space.system_space().data_dictionary_page()
	header := newSysDataDictionaryHeader(page)
	header.data_dictionary_header()
	return header.Indexes
}

type SysDataDictionaryHeader struct {
	Max_row_id        uint64     `json:"max_row_id"`
	Max_table_id      uint64     `json:"max_table_id"`
	Max_index_id      uint64     `json:"max_index_id"`
	Max_space_id      uint64     `json:"max_space_id"`
	Unused_mix_id_low uint64     `json:"unused_mix_id_low"`
	Indexes           Dict_Index `json:"indexes"`
	Unused_space      uint64     `json:"unused_space"`
	Fseg              uint64     `json:"fseg"` //先不管这个
	Page              *Page      `json:"-"`
}

func newSysDataDictionaryHeader(p *Page) *SysDataDictionaryHeader {
	return &SysDataDictionaryHeader{Page: p}

}
func (dh *SysDataDictionaryHeader) dump() {
	println("data_dictionary header:")
	dh.data_dictionary_header()
	data, _ := json.Marshal(dh)
	println(string(data))
}

func (dh *SysDataDictionaryHeader) pos_data_dictionary_header() int {
	return pos_page_body()
}

func (dh *SysDataDictionaryHeader) size_data_dictionary_header() int {
	return ((8 * 3) + (4 * 7) + 4 + 4 + 4 + 2) //最后三个是FSEG entry大小
}

func (dh *SysDataDictionaryHeader) data_dictionary_header() {
	//dict_page := sys.system_space().data_dictionary_page()

	dh.Max_row_id = uint64(dh.Page.bufferReadat(int64(dh.pos_data_dictionary_header()), 8))
	dh.Max_table_id = uint64(dh.Page.bufferReadat(int64(dh.pos_data_dictionary_header())+8, 8))
	dh.Max_index_id = uint64(dh.Page.bufferReadat(int64(dh.pos_data_dictionary_header())+16, 8))
	dh.Max_space_id = uint64(dh.Page.bufferReadat(int64(dh.pos_data_dictionary_header())+32, 4))
	dh.Unused_mix_id_low = uint64(dh.Page.bufferReadat(int64(dh.pos_data_dictionary_header())+36, 4))
	primary := dh.Page.bufferReadat(int64(dh.pos_data_dictionary_header())+40, 4)
	id := dh.Page.bufferReadat(int64(dh.pos_data_dictionary_header())+44, 4)
	var sys_table = SYS_TABLES{PRIMARY: uint64(primary), ID: uint64(id)}
	primary = dh.Page.bufferReadat(int64(dh.pos_data_dictionary_header())+48, 4)
	var sys_column = SYS_COLUMNS{PRIMARY: uint64(primary)}
	primary = dh.Page.bufferReadat(int64(dh.pos_data_dictionary_header())+52, 4)
	var sys_indexes = SYS_INDEXES{PRIMARY: uint64(primary)}
	primary = dh.Page.bufferReadat(int64(dh.pos_data_dictionary_header())+56, 4)
	var sys_field = SYS_FIELDS{PRIMARY: uint64(primary)}
	dh.Unused_space = uint64(dh.Page.bufferReadat(int64(dh.pos_data_dictionary_header())+60, 4))
	dh.Fseg = 4 //先不处理
	var indexes = Dict_Index{sys_table, sys_column, sys_indexes, sys_field}
	dh.Indexes = indexes

}

func (dh SysDataDictionaryHeader) String() string {
	dh.data_dictionary_header()
	res := "sysdatadictionaryHeader: xxxxx"
	return res
}

type DataFile struct {
	filename string
	size     uint64
	offset   uint64
}

func newDataFile(filename string, offset uint64) *DataFile {
	fi, err := os.Stat(filename)
	if err != nil {
		fmt.Println("file access err ", err)
	}
	return &DataFile{
		filename: filename,
		size:     uint64(fi.Size()),
		offset:   offset,
	}

}

type Space struct {
	datafiles     []*DataFile
	size          uint64
	pages         uint64
	name          string
	space_id      uint64
	innodb_system bool
}

func newSpace(filenames []string) *Space {
	var size uint64
	datafiles := []*DataFile{}
	size = 0
	innodb_system := false
	var name string
	for _, value := range filenames {
		file := newDataFile(value, size)
		size += file.size
		datafiles = append(datafiles, file)
		name += value
	}
	pages := size / DEFAULT_PAGE_SIZE
	if strings.Contains(name, "ibdata") {
		innodb_system = true
	}
	return &Space{
		size:          size,
		datafiles:     datafiles,
		pages:         pages,
		name:          name,
		innodb_system: innodb_system,
	}
}

func (s *Space) data_file_for_offset(offset uint64) *DataFile {
	var i uint64
	i = 1
	for _, file := range s.datafiles {
		if (i * file.size) < offset {
			i = i + 1
			continue
		}
		return file

	}
	return nil
}

func (s *Space) read_at_offset(offset uint64, size uint64) []byte {

	data_file := s.data_file_for_offset(offset)

	file_name := data_file.filename
	file, _ := os.Open(file_name)
	defer file.Close()

	file.Seek(int64(offset-data_file.offset), 0)
	//file.read(size)

	var buffer bytes.Buffer
	io.CopyN(&buffer, file, int64(size))
	_bytes := buffer.Bytes()

	return _bytes
}

func (s *Space) page_data(page_number uint64) []byte {
	//shuol return new page
	return s.read_at_offset(page_number*DEFAULT_PAGE_SIZE, DEFAULT_PAGE_SIZE)

}
func (s *Space) page(page_number uint64) *Page {
	data := s.page_data(page_number)
	page := newPage(s, &data, page_number)
	return page
	// page := Page.parse(self, data, page_number)

}

func (s *Space) data_dictionary_page() *Page {
	if s.is_system_space() {
		return s.page(7)
	}
	return nil
}

func (Space Space) String() string {
	println("why not me")
	res := "space: " + Space.name + ",pages=" + strconv.FormatUint(Space.pages, 10)
	return res
}

func (s *Space) is_system_space() bool {

	//_bytes = getData(file, 24, 2)
	offset := 24 + (0 * DEFAULT_PAGE_SIZE)
	fsp_hdr := s.read_at_offset(uint64(offset), 2)
	fsp_hdr_v, _ := BytesToUIntLittleEndian(fsp_hdr)

	offset = 24 + (1 * DEFAULT_PAGE_SIZE)
	IBUF_BITMAP := s.read_at_offset(uint64(offset), 2)
	IBUF_BITMAP_v, _ := BytesToUIntLittleEndian(IBUF_BITMAP)

	offset = 24 + (2 * DEFAULT_PAGE_SIZE)
	INODE := s.read_at_offset(uint64(offset), 2)
	INODE_v, _ := BytesToUIntLittleEndian(INODE)

	offset = 24 + (3 * DEFAULT_PAGE_SIZE)
	SYS1 := s.read_at_offset(uint64(offset), 2)
	SYS1_v, _ := BytesToUIntLittleEndian(SYS1)

	offset = 24 + (4 * DEFAULT_PAGE_SIZE)
	INDEX := s.read_at_offset(uint64(offset), 2)
	INDEX_v, _ := BytesToUIntLittleEndian(INDEX) //check this value

	offset = 24 + (5 * DEFAULT_PAGE_SIZE)
	TRX_SYS := s.read_at_offset(uint64(offset), 2)
	TRX_SYS_v, _ := BytesToUIntLittleEndian(TRX_SYS)

	offset = 24 + (6 * DEFAULT_PAGE_SIZE)
	SYS2 := s.read_at_offset(uint64(offset), 2)
	SYS2_v, _ := BytesToUIntLittleEndian(SYS2)

	offset = 24 + (7 * DEFAULT_PAGE_SIZE)
	SYS3 := s.read_at_offset(uint64(offset), 2)
	SYS3_v, _ := BytesToUIntLittleEndian(SYS3)

	if fsp_hdr_v == 8 && IBUF_BITMAP_v == 5 && INODE_v == 3 && SYS1_v == 6 && INDEX_v == 17855 && TRX_SYS_v == 7 && SYS2_v == 6 && SYS3_v == 6 {
		return true
	}
	return false
}

func pos_page_body() int {
	return 38
}

type System struct {
	config          map[string]string
	spaces          map[uint64]*Space
	orphans         []Space
	data_dictionary *DataDictionary
}

func newSystem(filenames []string) *System {
	system := new(System)
	system.config = make(map[string]string)
	system.config["datadir"] = filenames[0]
	space := newSpace(filenames)
	system.spaces = make(map[uint64]*Space)
	system.spaces[space.space_id] = space
	system.data_dictionary = newDataDictionary(system)
	return system
}
func (system *System) add_space(space *Space) {
	system.spaces[space.space_id] = space
}

func (system *System) system_space() *Space {
	for _, value := range system.spaces {
		if value.innodb_system == true {
			return value
		}
	}
	return nil
}

// from fil0file.h
var PAGE_TYPE = map[int]string{
	0:  "FIL_PAGE_TYPE_ALLOCATED", //*!< Freshly allocated page */
	2:  "FIL_PAGE_UNDO_LOG",       /*!< Undo log page */
	3:  "FIL_PAGE_INODE",          /*!< Index node */
	4:  "FIL_PAGE_IBUF_FREE_LIST", /*!< Insert buffer free list */
	5:  "FIL_PAGE_IBUF_BITMAP",    /*!< Insert buffer bitmap */
	6:  "FIL_PAGE_TYPE_SYS",       /*!< System page */
	7:  "FIL_PAGE_TYPE_TRX_SYS",   /*!< Transaction system data */
	8:  "FIL_PAGE_TYPE_FSP_HDR",   /*!< File space header */
	9:  "FIL_PAGE_TYPE_XDES",      /*!< Extent descriptor page */
	10: "FIL_PAGE_TYPE_BLOB",      /*!< Uncompressed BLOB page */
	11: "FIL_PAGE_TYPE_ZBLOB",     /*!< First compressed BLOB page */
	12: "FIL_PAGE_TYPE_ZBLOB2",    /*!< Subsequent compressed BLOB page */
	13: "FIL_PAGE_TYPE_UNKNOWN",   /*!< In old tablespaces, garbage in FIL_PAGE_TYPE is replaced with this
	value when flushing pages.*/
	14:    "FIL_PAGE_COMPRESSED",               /*!< Compressed page */
	15:    "FIL_PAGE_ENCRYPTED",                /*!< Encrypted page */
	16:    "FIL_PAGE_COMPRESSED_AND_ENCRYPTED", /*!< Compressed and Encrypted page */
	17:    "FIL_PAGE_ENCRYPTED_RTREE",          /*!< Encrypted R-tree page */
	17855: "FIL_PAGE_INDEX",                    /*!< B-tree node */
	17854: "FIL_PAGE_RTREE",                    /*!< B-tree node */
}

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}

func IsLittleEndian() bool {
	var i int32 = 0x01020304
	u := unsafe.Pointer(&i)
	pb := (*byte)(u)
	b := *pb
	return (b == 0x04)
}

func getData(f *os.File, pos int64, bsize int64) []byte {
	f.Seek(pos, 0)
	var buffer bytes.Buffer
	io.CopyN(&buffer, f, int64(bsize))
	_bytes := buffer.Bytes()

	return _bytes

}

func BytesToUIntLittleEndian(b []byte) (int, error) {

	if len(b) == 3 {
		b = append([]byte{0}, b...)
	}
	bytesBuffer := bytes.NewBuffer(b)
	switch len(b) {
	case 1:
		var tmp uint8
		err := binary.Read(bytesBuffer, binary.BigEndian, &tmp)
		return int(tmp), err
	case 2:
		var tmp uint16
		err := binary.Read(bytesBuffer, binary.BigEndian, &tmp)
		return int(tmp), err
	case 4:
		var tmp uint32
		err := binary.Read(bytesBuffer, binary.BigEndian, &tmp)
		return int(tmp), err

	case 8:
		var tmp uint64
		err := binary.Read(bytesBuffer, binary.BigEndian, &tmp)
		return int(tmp), err
	default:
		return 0, fmt.Errorf("%s", "BytesToInt bytes lenth is invaild!")
	}
}

func system_spaces(innodb_system *System) {
	dd := newDataDictionary(innodb_system)
	dd.each_table()

}

func main() {

	var sysfile string
	var page_no int
	var mode string

	flag.StringVar(&sysfile, "s", "", "系统表空间文件")
	flag.IntVar(&page_no, "p", 7, "块号")
	flag.StringVar(&mode, "m", "page-dump", "运行模式")

	//解析命令行参数
	flag.Parse()
	fmt.Println(sysfile, page_no, mode)

	file_arr := strings.Split(sysfile, ",")

	var innodb_system *System
	if sysfile != "" {
		innodb_system = newSystem(file_arr)
	}
	space := innodb_system.system_space()
	page := space.page(uint64(page_no))
	if page.FileHeader.Page_type == 3 {
		//index := space.index(page_no)

	}

	switch mode {
	case "system-spaces":
		system_spaces(innodb_system)

	case "page-dump":
		if page_no == 7 {
			page.page_dump()
		}

	default:
		println("no match mode")
	}

}
