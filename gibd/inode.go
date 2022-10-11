package gibd

import (
	"encoding/json"
	"fmt"

	"github.com/tidwall/pretty"
)

type Inode struct {
	Page   *Page          `json:"page"`
	Lnode  *Node          `json:"nodelist"`
	Inodes [85]InodeEntry `json:"Inodes"`
}

func NewInode(page *Page) *Inode {
	return &Inode{Page: page}
}

type FragEntry struct {
	V uint64
}

func NewFragEntry(v uint64) *FragEntry {
	fragEntry := &FragEntry{}
	fragEntry.V = v
	return fragEntry

}

type InodeEntry struct {
	Fseg_id            uint64        `json:"fsegid"`
	N_page_in_not_full uint64        `json:"npagenotfull"`
	Free               *BaseNode     `json:"freelist"`
	NotFull            *BaseNode     `json:"notfulllist"`
	Full               *BaseNode     `json:"fulllist"`
	Magic              uint64        `json:"magicnumber"`
	FragArrayEntry     [32]FragEntry `json:"FragEntry"`
}

func NewInodeEntry() *InodeEntry {
	return &InodeEntry{}
}

func Pos_List_Node() int64 {
	return 38
}

func (inode *Inode) ParseInodeBlock() {
	pos := Pos_List_Node()
	node := NewNode()
	node.Prev_page = uint64(BufferReadAt(inode.Page, pos, 4))
	node.Prev_offset = uint64(BufferReadAt(inode.Page, pos+4, 2))
	node.Next_page = uint64(BufferReadAt(inode.Page, pos+6, 4))
	node.Next_offset = uint64(BufferReadAt(inode.Page, pos+10, 2))
	inode.Lnode = node

	pos = 50
	for i := 0; i < 85; i++ {
		nodeEntry := NewInodeEntry()

		freeListBaseNode := NewBaseNode()
		notFullListBaseNode := NewBaseNode()
		fullListBaseNode := NewBaseNode()

		nodeEntry.Fseg_id = uint64(BufferReadAt(inode.Page, pos, 8))
		nodeEntry.N_page_in_not_full = uint64(BufferReadAt(inode.Page, pos+8, 4))

		freeListBaseNode.ListLen = uint64(BufferReadAt(inode.Page, pos+12, 4))
		freeListBaseNode.First_page = uint64(BufferReadAt(inode.Page, pos+16, 4))
		freeListBaseNode.First_offset = uint64(BufferReadAt(inode.Page, pos+20, 2))
		freeListBaseNode.Last_page = uint64(BufferReadAt(inode.Page, pos+22, 4))
		freeListBaseNode.Last_offset = uint64(BufferReadAt(inode.Page, pos+26, 2))

		nodeEntry.Free = freeListBaseNode
		notFullListBaseNode.ListLen = uint64(BufferReadAt(inode.Page, pos+28, 4))
		notFullListBaseNode.First_page = uint64(BufferReadAt(inode.Page, pos+32, 4))
		notFullListBaseNode.First_offset = uint64(BufferReadAt(inode.Page, pos+36, 2))
		notFullListBaseNode.Last_page = uint64(BufferReadAt(inode.Page, pos+38, 4))
		notFullListBaseNode.Last_offset = uint64(BufferReadAt(inode.Page, pos+42, 2))

		nodeEntry.NotFull = notFullListBaseNode

		fullListBaseNode.ListLen = uint64(BufferReadAt(inode.Page, pos+44, 4))
		fullListBaseNode.First_page = uint64(BufferReadAt(inode.Page, pos+48, 4))
		fullListBaseNode.First_offset = uint64(BufferReadAt(inode.Page, pos+52, 2))
		fullListBaseNode.Last_page = uint64(BufferReadAt(inode.Page, pos+54, 4))
		fullListBaseNode.Last_offset = uint64(BufferReadAt(inode.Page, pos+58, 2))

		nodeEntry.Full = fullListBaseNode

		nodeEntry.Magic = uint64(BufferReadAt(inode.Page, pos+60, 4))

		for j := int64(0); j < 32; j++ {
			entry := uint64(BufferReadAt(inode.Page, pos+60+j*4, 4))
			nodeEntry.FragArrayEntry[j] = *NewFragEntry(entry)

		}
		pos = pos + 192

	}
}

func (inode *Inode) Dump() {
	println("Inode dump:")

	data, _ := json.Marshal(inode)
	outStr := pretty.Pretty(data)
	fmt.Printf("%s\n", outStr)

}
