package gibd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"

	"github.com/tidwall/pretty"
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

//普通表空间
type Space struct {
	Datafiles []*DataFile
	Size      uint64
	Pages     uint64
	Name      string
	Space_id  uint64
	// Innodb_system    *System
	Record_describer interface{}
	IsSystemSpace    bool
}

func NewSpace(filenames []string) *Space {
	var size uint64

	datafiles := []*DataFile{}
	size = 0

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

	s := &Space{
		Size:      size,
		Datafiles: datafiles,
		Pages:     pages,
		Name:      name,
		// Space_id:  spaceId,
	}
	page := s.Page(0)
	page.Fil_Header()
	s.Space_id = page.FileHeader.Space_id
	s.IsSystemSpace = Is_System_Space(s)

	return s
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

func (s *Space) Get_Space_Id() uint64 {

	return s.Space_id
	// fsp := s.Fsp()

	// return fsp.FspHeader.Space_id
	//return GetFieldName("space_id", *fsp)
}

func (s *Space) Get_Index_Tree(root_page_number uint64, record_describer interface{}) *BTreeIndex {

	return NewBTreeIndex(s, root_page_number, record_describer)
}

func (s *Space) Each_Index(innodb_system *System) []*BTreeIndex {
	//普通用户表空间一般只有一个index，如果没有设置参数，可能都放到系统表空间,针对这种情况需要获取所有的index的根
	var indexes []*BTreeIndex
	root_pages := RemoveRepeatedElement(s.Each_Index_Root_Page_Number(innodb_system))

	for _, root := range root_pages {
		indexes = append(indexes, s.Get_Index_Tree(root, nil))
	}
	return indexes

}

//获取表空间所有index的root page number
func (s *Space) Each_Index_Root_Page_Number(innodb_system *System) []uint64 {
	var root_page_numer []uint64
	if s.IsSystemSpace {
		//s.innodb_system.data_dictionary.each_index_by_space_id(s.get_space_id())
		//data_dict := s.innodb_system.
		for _, value := range innodb_system.data_dictionary.Each_Index_By_Space_Id(s.Get_Space_Id()) {
			page_no := uint64(value["PAGE_NO"].(int64))
			root_page_numer = append(root_page_numer, page_no)
		}
		return root_page_numer
	} else {
		//获取普通index表空间的root page number
	}
	return nil
}
func (s *Space) data_file_for_offset(offset uint64) *DataFile {
	var i uint64
	i = 1

	for _, file := range s.Datafiles {
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

}

func (s *Space) Data_Dictionary_Header_Page() *Page {
	if Is_System_Space(s) {
		return s.Page(7)
	}
	return nil
}

func (Space Space) String() string {
	res := "space: " + Space.Name + ",pages=" + strconv.FormatUint(Space.Pages, 10)
	return res
}

func Is_System_Space(s *Space) bool {

	if s.Space_id == 0 {
		return true
	}
	return false
	//_bytes = getData(file, 24, 2)
	offset := 24 + (0 * DEFAULT_PAGE_SIZE)
	fsp_hdr := s.Read_At_Offset(uint64(offset), 2)
	fsp_hdr_v := BytesToUIntLittleEndian(fsp_hdr)

	offset = 24 + (1 * DEFAULT_PAGE_SIZE)
	IBUF_BITMAP := s.Read_At_Offset(uint64(offset), 2)
	IBUF_BITMAP_v := BytesToUIntLittleEndian(IBUF_BITMAP)

	offset = 24 + (2 * DEFAULT_PAGE_SIZE)
	INODE := s.Read_At_Offset(uint64(offset), 2)
	INODE_v := BytesToUIntLittleEndian(INODE)

	offset = 24 + (3 * DEFAULT_PAGE_SIZE)
	SYS1 := s.Read_At_Offset(uint64(offset), 2)
	SYS1_v := BytesToUIntLittleEndian(SYS1)

	offset = 24 + (4 * DEFAULT_PAGE_SIZE)
	INDEX := s.Read_At_Offset(uint64(offset), 2)
	INDEX_v := BytesToUIntLittleEndian(INDEX) //check this value

	offset = 24 + (5 * DEFAULT_PAGE_SIZE)
	TRX_SYS := s.Read_At_Offset(uint64(offset), 2)
	TRX_SYS_v := BytesToUIntLittleEndian(TRX_SYS)

	offset = 24 + (6 * DEFAULT_PAGE_SIZE)
	SYS2 := s.Read_At_Offset(uint64(offset), 2)
	SYS2_v := BytesToUIntLittleEndian(SYS2)

	offset = 24 + (7 * DEFAULT_PAGE_SIZE)
	SYS3 := s.Read_At_Offset(uint64(offset), 2)
	SYS3_v := BytesToUIntLittleEndian(SYS3)

	if fsp_hdr_v == 8 && IBUF_BITMAP_v == 5 && INODE_v == 3 && SYS1_v == 6 && INDEX_v == 17855 && TRX_SYS_v == 7 && SYS2_v == 6 && SYS3_v == 6 {
		return true
	}
	return false
}

func (s *Space) Dump() {
	println("space:")

	data, _ := json.Marshal(s)
	outStr := pretty.Pretty(data)
	fmt.Printf("%s\n", outStr)
}
