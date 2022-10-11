package main

import (
	"flag"
	"fmt"
	"gibd/gibd"
	"strings"
)

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
	Page *gibd.Page
}

// from fil0file.h

// func CheckErr(err error) {
// 	if err != nil {
// 		panic(err)
// 	}
// }

// func GetData(f *os.File, pos int64, bsize int64) []byte {
// 	f.Seek(pos, 0)
// 	var buffer bytes.Buffer
// 	io.CopyN(&buffer, f, int64(bsize))
// 	_bytes := buffer.Bytes()

// 	return _bytes

// }

func Print_System_Spaces(innodb_system *gibd.System) {
	//dd := newDataDictionary(innodb_system)
	//dd.each_table()
	btreeindexes := innodb_system.System_Space().Each_Index(innodb_system)
	fmt.Printf("name\t,pages\t,btreeindexes\n")
	fmt.Printf("%+v\t", "System")
	fmt.Printf("%+v\t", innodb_system.System_Space().Pages)
	fmt.Printf("%+v\n", len(btreeindexes))

	tables := innodb_system.Each_Table_Name()

	for _, value := range tables {
		fmt.Println(value)
	}

}

func main() {

	var file string
	var page_no int
	var mode string

	flag.StringVar(&file, "s", "", "表空间文件名")
	//共享表空间第7块是数据字典头块
	flag.IntVar(&page_no, "p", 7, "块号")
	flag.StringVar(&mode, "m", "page-dump", "运行模式")

	//解析命令行参数
	flag.Parse()

	file_arr := strings.Split(file, ",")

	switch mode {
	case "system-spaces":
		var innodb_system *gibd.System
		if file != "" {
			innodb_system = gibd.NewSystem(file_arr)
		}

		space := innodb_system.System_Space()
		page := space.Page(uint64(page_no))
		page.Page_Dump()
		Print_System_Spaces(innodb_system)
		// if page.FileHeader.Page_type == 3 {
		// 	//index := space.index(page_no)
		// }
	case "page-dump":
		space := gibd.NewSpace(file_arr)
		page := space.Page(uint64(page_no))
		page.Page_Dump()

	default:
		println("no match mode")
	}

}
