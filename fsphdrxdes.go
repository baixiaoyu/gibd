package main

import "strconv"

type Flags struct {
	system_page_size uint64
	compressed       bool
	page_size        uint64
	post_antelope    uint64
	atomic_blobs     uint64
	data_directory   uint64
	value            uint64
}

type Header struct {
	space_id         uint64
	unused           uint64
	size             uint64
	free_limit       uint64
	flags            uint64
	frag_n_used      uint64
	free             uint64
	free_frag        uint64
	full_frag        uint64
	first_unused_seg uint64
	full_inodes      uint64
	free_inodes      uint64
}
type FspHdrXdes struct {
	Page   *Page
	Flags  Flags
	Header Header
}

func newFspHdrXdes(page *Page) *FspHdrXdes {
	return &FspHdrXdes{
		Page: page,
	}
}
func (fsp FspHdrXdes) String() string {

	res := "fsp header space id: " + strconv.FormatUint(fsp.Header.space_id, 10)
	return res
}

func (f *FspHdrXdes) pos_fsp_header() uint64 {
	return pos_page_body()
}
func (f *FspHdrXdes) fsp_header() {

	space_id := uint64(f.Page.bufferReadat(int64(f.pos_fsp_header()), 4))
	unused := uint64(f.Page.bufferReadat(int64(f.pos_fsp_header())+4, 4))
	size := uint64(f.Page.bufferReadat(int64(f.pos_fsp_header())+8, 4))
	free_limit := uint64(f.Page.bufferReadat(int64(f.pos_fsp_header())+12, 4))
	//flags := uint64(f.Page.bufferReadat(int64(f.pos_fsp_header())+16, 4)) 暂时不看
	frag_n_used := uint64(f.Page.bufferReadat(int64(f.pos_fsp_header())+20, 4))
	// free暂时不看
	// free_frag暂时不看
	// full_frag暂时不看
	first_unused_seg := uint64(f.Page.bufferReadat(int64(f.pos_fsp_header())+72, 8))
	// full_inodes暂时不看
	// free_inodes暂时不看

	header := Header{space_id: space_id, unused: unused, size: size, free_limit: free_limit, frag_n_used: frag_n_used, first_unused_seg: first_unused_seg}
	f.Header = header
}
