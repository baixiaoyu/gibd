package gibd

import (
	"encoding/json"
	"fmt"
	"strconv"
)

// https://blog.jcole.us/2013/01/03/the-basics-of-innodb-space-file-layout/
const (
	FIL_PAGE_TYPE_ALLOCATED = 0
	FIL_PAGE_UNDO_LOG       = 2
	FIL_PAGE_INODE          = 3
	FIL_PAGE_IBUF_BITMAP    = 5
	FIL_PAGE_TYPE_SYS       = 6

	FIL_PAGE_TYPE_TRX_SYS = 7
	FIL_PAGE_TYPE_FSP_HDR = 8
	FIL_PAGE_INDEX        = 17855
	FIL_PAGE_RTREE        = 17854
)

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

type Address struct {
	Page uint64 `json:"pageno"`
	// Offset uint64 `json:"offset"`
}

func NewAddress(Page uint64) *Address {
	return &Address{
		Page: Page,
		// Offset: Offset,
	}

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

func (s *FilHeader) Lsn_Low32(offset uint64) uint64 {
	return s.Lsn & 0xffffffff
}

func (filHeader FilHeader) String() string {
	jsons, _ := json.Marshal(filHeader)
	return string(jsons)
}

type FilTrailer struct {
	Checksum  uint64 `json:"checksum"`
	Lsn_low32 uint64 `json:"lsn_low32"`
}

func (filTrailer FilTrailer) String() string {

	res := "checksum:" + strconv.FormatUint(filTrailer.Checksum, 10) + ",offset:" + strconv.FormatUint(filTrailer.Lsn_low32, 10)
	return res
}

type Region struct {
	offset uint64
	length uint64
	name   string
	info   string
}

type Page struct {
	Address          Address    `json:"address"`
	FileHeader       FilHeader  `json:"fileheader"`
	FileTrailer      FilTrailer `json:"filetrailer"`
	Region           Region     `json:"region"`
	Space            *Space     `json:"space"`
	Buffer           *[]byte    `json:"-"`
	Page_number      uint64     `json:"page_number"`
	record_describer interface{}
	// Fsphdxdes        FspHdrXdes `json:"fsphdxdes"` // 这个只是在表空间的第一个页上有
}

func NewPage(space *Space, buffer *[]byte, page_number uint64) *Page {
	p := &Page{
		Space:       space,
		Buffer:      buffer,
		Page_number: page_number,
	}

	p.Fil_Header()
	p.Fil_Trailer()
	return p

}

func (p *Page) Page_Dump() {

	p.Fil_Header()
	p.Fil_Trailer()

	// jsons, _ := json.Marshal(p)
	// println(string(jsons))

	println()

	if p.FileHeader.Page_type == FIL_PAGE_TYPE_SYS {
		dict_header := NewSysDataDictionaryHeader(p)
		dict_header.Dump()
	}
	if p.FileHeader.Page_type == FIL_PAGE_TYPE_FSP_HDR {
		//表空间从block 0 是fsp extent descriptor信息，
		fmt.Println("fsp header:")
		fsphdxdes := NewFspHdrXdes(p)
		fsphdxdes.Fsp_Header()
		fsphdxdes.Dump()

	}
	if p.FileHeader.Page_type == FIL_PAGE_IBUF_BITMAP {
		// TODO
		//表空间从block 1 是IBUF_BITMAP信息，
	}

	if p.FileHeader.Page_type == FIL_PAGE_INODE {
		//表空间从block 2 是INODE信息，
		iNodePage := NewInode(p)
		iNodePage.ParseInodeBlock()
		iNodePage.Dump()

	}
	if p.FileHeader.Page_type == FIL_PAGE_INDEX {
		//表空间从block 3开始是用户数据页
		indexPage := NewIndex(p)
		indexPage.Index_Header()
		indexPage.Fseg_Header()

		indexPage.Dump()
		indexPage.each_record()
	}
	if p.FileHeader.Page_type == FIL_PAGE_TYPE_ALLOCATED {
		// do nothing,new page
	}

	if p.FileHeader.Page_type == FIL_PAGE_UNDO_LOG {
		// undo block parse
	}

}

func (p *Page) String() string {

	page_offset := BufferReadAt(p, 4, 4)
	page_type := BufferReadAt(p, 24, 2)
	res := "page: " + strconv.Itoa(page_offset) + ",type=" + PAGE_TYPE[page_type]
	return res
}

func (p *Page) Pos_Fil_Header() uint64 {
	return 0
}

func (p *Page) Fil_Header() {

	p.FileHeader.Checksum = uint64(BufferReadAt(p, int64(p.Pos_Fil_Header()), 4)) // 这个是checksum还是FIL_PAGE_SPACE
	p.FileHeader.Offset = uint64(BufferReadAt(p, int64(p.Pos_Fil_Header())+4, 4))
	p.FileHeader.Prev = uint64(BufferReadAt(p, int64(p.Pos_Fil_Header())+8, 4))
	p.FileHeader.Next = uint64(BufferReadAt(p, int64(p.Pos_Fil_Header())+12, 4))
	p.FileHeader.Lsn = uint64(BufferReadAt(p, int64(p.Pos_Fil_Header())+16, 8))
	p.FileHeader.Page_type = uint64(BufferReadAt(p, int64(p.Pos_Fil_Header())+24, 2))
	p.FileHeader.Flush_lsn = uint64(BufferReadAt(p, int64(p.Pos_Fil_Header())+26, 8))
	p.FileHeader.Space_id = uint64(BufferReadAt(p, int64(p.Pos_Fil_Header())+34, 4))

}

func (p *Page) Fil_Trailer() {
	p.FileTrailer.Checksum = uint64(BufferReadAt(p, int64(p.Pos_Fil_Trailer()), 4))
	p.FileTrailer.Lsn_low32 = uint64(BufferReadAt(p, int64(p.Pos_Fil_Trailer())+4, 4))
}

func (p *Page) Size_Fil_Header() uint64 { //38
	return 4 + 4 + 4 + 4 + 8 + 2 + 8 + 4
}

func (p *Page) Pos_Partial_Page_Header() uint64 {
	return p.Pos_Fil_Header() + 4
}

func (p *Page) Size_Partial_Page_Header() uint64 {
	return p.Size_Fil_Header() - 4 - 8 - 4
}
func (p *Page) Size_Fil_Trailer() uint64 {
	return 4 + 4
}

func (p *Page) Pos_Fil_Trailer() uint64 {
	return DEFAULT_PAGE_SIZE - p.Size_Fil_Trailer()
}

func (p *Page) Pos_Page_Body() uint64 {
	return p.Pos_Fil_Header() + p.Size_Fil_Header()
}

func (p *Page) Size_Page_Body() uint64 {
	return DEFAULT_PAGE_SIZE - p.Size_Fil_Trailer() - p.Size_Fil_Header()
}

func Pos_Page_Body() uint64 {
	return 38
}
