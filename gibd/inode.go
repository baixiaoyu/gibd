package gibd

type Inode struct {
	Lnode  Node           `json:"nodelist"`
	Inodes [85]InodeEntry `json:"Inodes"`
}

type FragEntry struct {
}

type InodeEntry struct {
	Fseg_id        uint64        `json:"fsegid"`
	Free           Node          `json:"freelist"`
	NotFull        Node          `json:"notfulllist"`
	Full           Node          `json:"fulllist"`
	Magic          Node          `json:"magicnumber"`
	FragEntryArray [32]FragEntry `json:"FragEntry"`
}
