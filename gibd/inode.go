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
}

type InodeEntry struct {
	Fseg_id            uint64        `json:"fsegid"`
	N_page_in_not_full uint64        `json:"npagenotfull"`
	Free               Node          `json:"freelist"`
	NotFull            Node          `json:"notfulllist"`
	Full               Node          `json:"fulllist"`
	Magic              Node          `json:"magicnumber"`
	FragEntryArray     [32]FragEntry `json:"FragEntry"`
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
	node.Prev_page = uint64(inode.Page.BufferReadAt(pos, 4))
	node.Prev_offset = uint64(inode.Page.BufferReadAt(pos+4, 2))
	node.Next_page = uint64(inode.Page.BufferReadAt(pos+6, 4))
	node.Next_offset = uint64(inode.Page.BufferReadAt(pos+10, 2))
	inode.Lnode = node

	pos = 50
	for i := 0; i < 85; i++ {
		nodeEntry := NewInodeEntry()

		freeListBaseNode := NewBaseNode()
		notFullListBaseNode := NewBaseNode()
		fullListBaseNode := NewBaseNode()

		nodeEntry.Fseg_id = uint64(inode.Page.BufferReadAt(pos, 8))
		nodeEntry.N_page_in_not_full = uint64(inode.Page.BufferReadAt(pos+8, 4))

		freeListBaseNode.ListLen = uint64(inode.Page.BufferReadAt(pos+12, 4))
		freeListBaseNode.First_page = uint64(inode.Page.BufferReadAt(pos+16, 4))
		freeListBaseNode.First_offset = uint64(inode.Page.BufferReadAt(pos+20, 2))
		freeListBaseNode.Last_page = uint64(inode.Page.BufferReadAt(pos+22, 4))
		freeListBaseNode.Last_offset = uint64(inode.Page.BufferReadAt(pos+26, 2))

		pos = pos + 192

	}
}

func (inode *Inode) Dump() {
	println("Inode dump:")

	data, _ := json.Marshal(inode)
	outStr := pretty.Pretty(data)
	fmt.Printf("%s\n", outStr)

}
