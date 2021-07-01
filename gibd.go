package main

import (
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"unsafe"
)

const DEFAULT_PAGE_SIZE = 16 * 1024
const DEFAULT_EXTENT_SIZE = 64 * DEFAULT_PAGE_SIZE
const SYSTEM_SPACE_ID = 0
const FsegEntry_SIZE = 4 + 4 + 2

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

var XDES_LISTS = [...]string{
	"free",
	"free_frag",
	"full_frag",
}

var INODE_LISTS = [...]string{
	"full_inodes",
	"free_inodes",
}

var specialized_classes = map[uint64]string{
	6: "SYS",
}

type SYS struct {
	Page *Page
}

func pos_page_body() uint64 {
	return 38
}

// from fil0file.h

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
	//dd := newDataDictionary(innodb_system)
	//dd.each_table()
	innodb_system.system_space().each_index()

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
