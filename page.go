package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strconv"
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
	Page   uint64 `json:"page"`
	Offset uint64 `json:"offset"`
}

func newAddress(Page uint64, Offset uint64) *Address {
	return &Address{
		Page:   Page,
		Offset: Offset,
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

func (s *FilHeader) lsn_low32(offset uint64) uint64 {
	return s.Lsn & 0xffffffff
}

func (filHeader FilHeader) String() string {
	jsons, _ := json.Marshal(filHeader)
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

type Page struct {
	Address          Address    `json:"address"`
	FileHeader       FilHeader  `json:"fileheader"`
	FileTrailer      FilTrailer `json:"filetrailer"`
	Region           Region     `json:"region"`
	Space            *Space     `json:"space"`
	Buffer           *[]byte    `json:"-"`
	Page_number      uint64     `json:"page_number"`
	record_describer interface{}
}

func newPage(space *Space, buffer *[]byte, page_number uint64) *Page {
	p := &Page{
		Space:       space,
		Buffer:      buffer,
		Page_number: page_number,
	}
	p.fil_header()
	p.fil_trailer()
	return p

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

func (p *Page) readbytes(offset int64, size int64) []byte {

	byteStorage := make([]byte, size)
	byteReader := bytes.NewReader(*p.Buffer)
	byteReader.ReadAt(byteStorage, offset)

	return byteStorage
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

func (p *Page) BytesToIntLittleEndian(b []byte) int {

	if len(b) == 3 {
		b = append([]byte{0}, b...)
	}
	bytesBuffer := bytes.NewBuffer(b)
	switch len(b) {
	case 1:
		var tmp int8
		binary.Read(bytesBuffer, binary.BigEndian, &tmp)
		return int(tmp)
	case 2:
		var tmp int16
		binary.Read(bytesBuffer, binary.BigEndian, &tmp)
		return int(tmp)
	case 4:
		var tmp int32
		binary.Read(bytesBuffer, binary.BigEndian, &tmp)
		return int(tmp)

	case 8:
		var tmp int64
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

func (p *Page) pos_fil_header() uint64 {
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

func (p *Page) size_fil_header() uint64 { //38
	return 4 + 4 + 4 + 4 + 8 + 2 + 8 + 4
}

func (p *Page) pos_partial_page_header() uint64 {
	return p.pos_fil_header() + 4
}

func (p *Page) size_partial_page_header() uint64 {
	return p.size_fil_header() - 4 - 8 - 4
}
func (p *Page) size_fil_trailer() uint64 {
	return 4 + 4
}

func (p *Page) pos_fil_trailer() uint64 {
	return DEFAULT_PAGE_SIZE - p.size_fil_trailer()
}

func (p *Page) pos_page_body() uint64 {
	return p.pos_fil_header() + p.size_fil_header()
}

func (p *Page) size_page_body() uint64 {
	return DEFAULT_PAGE_SIZE - p.size_fil_trailer() - p.size_fil_header()
}
