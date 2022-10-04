package gibd

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
