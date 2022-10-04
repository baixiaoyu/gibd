package gibd

import (
	"strconv"
)

type Flags struct {
	system_page_size uint64
	compressed       bool
	page_size        uint64
	post_antelope    uint64
	atomic_blobs     uint64
	data_directory   uint64
	value            uint64
}

type BaseNode struct {
	Flst_first_len    uint64 `json:"flst_first_len"`
	Flst_first_page   uint64 `json:"flst_first_page"`
	Flst_first_offset uint64 `json:"flst_first_offset"`
	Flst_last_len     uint64 `json:"flst_last_len"`
	Flst_last_page    uint64 `json:"flst_last_page"`
	Flst_last_offset  uint64 `json:"flst_last_offset"`
}

type Node struct {
	Prev_page   uint64 `json:"prev_page"`
	Prev_offset uint64 `json:"prev_offset"`
	Next_page   uint64 `json:"next_page"`
	Next_offset uint64 `json:"next_offset"`
}

func NewNode() Node {
	return Node{}
}
func NewBaseNode() BaseNode {
	return BaseNode{}
}

type FspHeader struct {
	Space_id         uint64   `json:"space_id"`
	Unused           uint64   `json:"unused"`
	Size             uint64   `json:"size"`
	Free_limit       uint64   `json:"free_limit"`
	Flags            uint64   `json:"flags"`
	Frag_n_used      uint64   `json:"frag_n_used"`
	Free             BaseNode `json:"freelist"`  // base node for free list
	Free_frag        uint64   `json:"free_frag"` // base node for free frag
	Full_frag        uint64   `json:"full_frag"` // base node for ful frag
	First_unused_seg uint64   `json:"first_unused_seg"`
	Full_inodes      uint64   `json:"full_inodes"` // base node for full_inodes list
	Free_inodes      uint64   `json:"free_inodes"` // base node for free_inodes list
}
type Xdes struct {
	F_seg_id  uint64 `json:"F_seg_id"`
	Xdes_List Node   `json:"xdes_list"`
	State     uint64 `json:"state"`
	Bitmap    []byte
}
type FspHdrXdes struct {
	// Page      *Page
	// Flags     Flags
	FspHeader FspHeader `json:"fspheader"`
	Xdes      [256]Xdes
}

func NewXdes() Xdes {
	return Xdes{}
}
func NewFspHdrXdes() FspHdrXdes {
	return FspHdrXdes{
		// Page: page,
	}
}
func (fsp FspHdrXdes) String() string {

	res := "fsp header space id: " + strconv.FormatUint(fsp.FspHeader.Space_id, 10)
	return res
}

func (f *FspHdrXdes) Pos_Fsp_Header() uint64 {
	return Pos_Page_Body()
}

// https://blog.jcole.us/2013/01/04/page-management-in-innodb-space-files/
func (f *FspHdrXdes) Fsp_Header(p *Page) {

	space_id := uint64(p.BufferReadAt(int64(f.Pos_Fsp_Header()), 4))
	unused := uint64(p.BufferReadAt(int64(f.Pos_Fsp_Header())+4, 4))
	size := uint64(p.BufferReadAt(int64(f.Pos_Fsp_Header())+8, 4))
	free_limit := uint64(p.BufferReadAt(int64(f.Pos_Fsp_Header())+12, 4))
	flags := uint64(p.BufferReadAt(int64(f.Pos_Fsp_Header())+16, 4))
	frag_n_used := uint64(p.BufferReadAt(int64(f.Pos_Fsp_Header())+20, 4))
	free_node := NewBaseNode()
	flst_first_len := uint64(p.BufferReadAt(int64(f.Pos_Fsp_Header())+24, 4))
	flst_first_page := uint64(p.BufferReadAt(int64(f.Pos_Fsp_Header())+28, 4))
	flst_first_offset := uint64(p.BufferReadAt(int64(f.Pos_Fsp_Header())+32, 2))

	flst_last_page := uint64(p.BufferReadAt(int64(f.Pos_Fsp_Header())+34, 4))
	flst_last_offset := uint64(p.BufferReadAt(int64(f.Pos_Fsp_Header())+38, 2))

	free_node.Flst_first_len = flst_first_len
	free_node.Flst_first_page = flst_first_page
	free_node.Flst_first_offset = flst_first_offset
	free_node.Flst_last_page = flst_last_page
	free_node.Flst_last_offset = flst_last_offset
	// free_frag暂时不看
	// full_frag暂时不看
	first_unused_seg := uint64(p.BufferReadAt(int64(f.Pos_Fsp_Header())+72, 8))
	// full_inodes暂时不看
	// free_inodes暂时不看

	header := FspHeader{Space_id: space_id, Unused: unused, Size: size, Free_limit: free_limit, Flags: flags, Frag_n_used: frag_n_used, Free: free_node, First_unused_seg: first_unused_seg}
	f.FspHeader = header

	//获取xdes信息
	for i := int64(0); i < 256; i++ {
		pos := int64(150) + i*int64(40)
		xdes := NewXdes()
		f_seg_id := uint64(p.BufferReadAt(pos, 8))
		pos = pos + 8
		node := NewNode()
		node.Prev_page = uint64(p.BufferReadAt(pos, 4))
		pos = pos + 4
		node.Prev_offset = uint64(p.BufferReadAt(pos, 2))
		pos = pos + 2
		node.Next_page = uint64(p.BufferReadAt(pos, 4))
		pos = pos + 4
		node.Next_offset = uint64(p.BufferReadAt(pos, 2))
		pos = pos + 2
		state := uint64(p.BufferReadAt(pos, 4))
		pos = pos + 4
		bitmap := p.ReadBytes(pos, 16)
		pos = pos + 16
		xdes.Bitmap = bitmap
		xdes.F_seg_id = f_seg_id
		xdes.State = state
		xdes.Xdes_List = node
		f.Xdes[i] = xdes
	}

}
