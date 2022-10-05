package gibd

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"reflect"
	"strconv"
	"strings"
)

type DataFile struct {
	filename string
	size     uint64
	offset   uint64
}

func NewDataFile(filename string, offset uint64) *DataFile {
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
	datafiles        []*DataFile
	size             uint64
	Pages            uint64
	name             string
	space_id         uint64
	innodb_system    *System
	record_describer interface{}
}

func NewSpace(filenames []string) *Space {
	var size uint64
	datafiles := []*DataFile{}
	size = 0
	// innodb_system := false
	var name string
	for _, value := range filenames {
		file := NewDataFile(value, size)
		size += file.size
		datafiles = append(datafiles, file)
		name += value
	}
	pages := size / DEFAULT_PAGE_SIZE
	// if strings.Contains(name, "ibdata") {
	// 	innodb_system = true
	// }
	return &Space{
		size:      size,
		datafiles: datafiles,
		Pages:     pages,
		name:      name,
	}
}

// func (s *Space) Page_Fsp_Hdr() uint64 {
// 	return 0
// }

// func (s *Space) Fsp() FspHdrXdes {

// 	page := s.Page(s.Page_Fsp_Hdr())
// 	fsp := NewFspHdrXdes()
// 	fsp.Fsp_Header()
// 	return fsp

// }

//获取结构体中字段的名称
func GetFieldName(columnName string, info FspHdrXdes) uint64 {

	var val uint64
	t := reflect.TypeOf(info)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		fmt.Println("Check type error not Struct")
		return 0
	}
	fieldNum := t.NumField()
	for i := 0; i < fieldNum; i++ {
		if strings.ToUpper(t.Field(i).Name) == strings.ToUpper(columnName) {
			v := reflect.ValueOf(info)
			val := v.FieldByName(t.Field(i).Name).Uint()
			return val
		}
	}
	return val
}

func (s *Space) Get_Space_Id() uint64 {

	return s.space_id
	// fsp := s.Fsp()

	// return fsp.FspHeader.Space_id
	//return GetFieldName("space_id", *fsp)
}

func (s *Space) Index(root_page_number uint64, record_describer interface{}) *BTreeIndex {

	return NewBTreeIndex(s, root_page_number, record_describer)
}

func RemoveRepeatedElement(arr []uint64) (newArr []uint64) {
	newArr = make([]uint64, 0)
	for i := 0; i < len(arr); i++ {
		repeat := false
		for j := i + 1; j < len(arr); j++ {
			if arr[i] == arr[j] {
				repeat = true
				break
			}
		}
		if !repeat {
			newArr = append(newArr, arr[i])
		}
	}
	return
}

func (s *Space) Each_Index() []*BTreeIndex {
	var indexes []*BTreeIndex
	root_pages := RemoveRepeatedElement(s.Each_Index_Root_Page_Number())
	Log.Info("eache_index all_root_page_number=========>%+v", root_pages)
	for _, value := range root_pages {
		indexes = append(indexes, s.Index(value, nil))
	}
	return indexes

}

//获取表空间所有index的
func (s *Space) Each_Index_Root_Page_Number() []uint64 {
	var root_page_numer []uint64
	if s.innodb_system != nil {
		//s.innodb_system.data_dictionary.each_index_by_space_id(s.get_space_id())
		//data_dict := s.innodb_system.
		for _, value := range s.innodb_system.data_dictionary.each_index_by_space_id(s.Get_Space_Id()) {
			page_no := uint64(value["PAGE_NO"].(int64))
			root_page_numer = append(root_page_numer, page_no)
		}
		return root_page_numer
	}
	return nil
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

func (s *Space) Read_At_Offset(offset uint64, size uint64) []byte {

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

func (s *Space) Page_Data(page_number uint64) []byte {
	//shuol return new page
	return s.Read_At_Offset(page_number*DEFAULT_PAGE_SIZE, DEFAULT_PAGE_SIZE)

}
func (s *Space) Page(page_number uint64) *Page {

	data := s.Page_Data(page_number)
	page := NewPage(s, &data, page_number)
	return page
	// page := Page.parse(self, data, page_number)

}

func (s *Space) Data_Dictionary_Page() *Page {
	if s.Is_System_Space() {
		return s.Page(7)
	}
	return nil
}

func (Space Space) String() string {
	res := "space: " + Space.name + ",pages=" + strconv.FormatUint(Space.Pages, 10)
	return res
}

func (s *Space) Is_System_Space() bool {

	//_bytes = getData(file, 24, 2)
	offset := 24 + (0 * DEFAULT_PAGE_SIZE)
	fsp_hdr := s.Read_At_Offset(uint64(offset), 2)
	fsp_hdr_v, _ := BytesToUIntLittleEndian(fsp_hdr)

	offset = 24 + (1 * DEFAULT_PAGE_SIZE)
	IBUF_BITMAP := s.Read_At_Offset(uint64(offset), 2)
	IBUF_BITMAP_v, _ := BytesToUIntLittleEndian(IBUF_BITMAP)

	offset = 24 + (2 * DEFAULT_PAGE_SIZE)
	INODE := s.Read_At_Offset(uint64(offset), 2)
	INODE_v, _ := BytesToUIntLittleEndian(INODE)

	offset = 24 + (3 * DEFAULT_PAGE_SIZE)
	SYS1 := s.Read_At_Offset(uint64(offset), 2)
	SYS1_v, _ := BytesToUIntLittleEndian(SYS1)

	offset = 24 + (4 * DEFAULT_PAGE_SIZE)
	INDEX := s.Read_At_Offset(uint64(offset), 2)
	INDEX_v, _ := BytesToUIntLittleEndian(INDEX) //check this value

	offset = 24 + (5 * DEFAULT_PAGE_SIZE)
	TRX_SYS := s.Read_At_Offset(uint64(offset), 2)
	TRX_SYS_v, _ := BytesToUIntLittleEndian(TRX_SYS)

	offset = 24 + (6 * DEFAULT_PAGE_SIZE)
	SYS2 := s.Read_At_Offset(uint64(offset), 2)
	SYS2_v, _ := BytesToUIntLittleEndian(SYS2)

	offset = 24 + (7 * DEFAULT_PAGE_SIZE)
	SYS3 := s.Read_At_Offset(uint64(offset), 2)
	SYS3_v, _ := BytesToUIntLittleEndian(SYS3)

	if fsp_hdr_v == 8 && IBUF_BITMAP_v == 5 && INODE_v == 3 && SYS1_v == 6 && INDEX_v == 17855 && TRX_SYS_v == 7 && SYS2_v == 6 && SYS3_v == 6 {
		return true
	}
	return false
}
