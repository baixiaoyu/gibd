package main

import "encoding/json"

type SYS_TABLES struct {
	PRIMARY uint64 `json:"primary"`
	ID      uint64 `json:"id"`
}
type SYS_COLUMNS struct {
	PRIMARY uint64 `json:"primary"`
}
type SYS_INDEXES struct {
	PRIMARY uint64 `json:"primary"`
}
type SYS_FIELDS struct {
	PRIMARY uint64 `json:"primary"`
}

type Dict_Index struct {
	SYS_TABLES  SYS_TABLES  `json:"sys_tables"`
	SYS_COLUMNS SYS_COLUMNS `json:"sys_columns"`
	SYS_INDEXES SYS_INDEXES `json:"sys_indexes"`
	SYS_FIELDS  SYS_FIELDS  `json:"sys_fields"`
}

type SysDataDictionaryHeader struct {
	Max_row_id        uint64     `json:"max_row_id"`
	Max_table_id      uint64     `json:"max_table_id"`
	Max_index_id      uint64     `json:"max_index_id"`
	Max_space_id      uint64     `json:"max_space_id"`
	Unused_mix_id_low uint64     `json:"unused_mix_id_low"`
	Indexes           Dict_Index `json:"indexes"`
	Unused_space      uint64     `json:"unused_space"`
	Fseg              uint64     `json:"fseg"` //先不管这个
	Page              *Page      `json:"-"`
}

func newSysDataDictionaryHeader(p *Page) *SysDataDictionaryHeader {
	return &SysDataDictionaryHeader{Page: p}

}
func (dh *SysDataDictionaryHeader) dump() {
	println("data_dictionary header:")
	dh.data_dictionary_header()
	data, _ := json.Marshal(dh)
	println(string(data))
}

func (dh *SysDataDictionaryHeader) pos_data_dictionary_header() uint64 {
	return pos_page_body()
}

func (dh *SysDataDictionaryHeader) size_data_dictionary_header() int {
	return ((8 * 3) + (4 * 7) + 4 + 4 + 4 + 2) //最后三个是FSEG entry大小
}

func (dh *SysDataDictionaryHeader) data_dictionary_header() {
	//dict_page := sys.system_space().data_dictionary_page()

	dh.Max_row_id = uint64(dh.Page.bufferReadat(int64(dh.pos_data_dictionary_header()), 8))
	dh.Max_table_id = uint64(dh.Page.bufferReadat(int64(dh.pos_data_dictionary_header())+8, 8))
	dh.Max_index_id = uint64(dh.Page.bufferReadat(int64(dh.pos_data_dictionary_header())+16, 8))
	dh.Max_space_id = uint64(dh.Page.bufferReadat(int64(dh.pos_data_dictionary_header())+24, 4))
	dh.Unused_mix_id_low = uint64(dh.Page.bufferReadat(int64(dh.pos_data_dictionary_header())+28, 4))
	primary := dh.Page.bufferReadat(int64(dh.pos_data_dictionary_header())+32, 4)
	id := dh.Page.bufferReadat(int64(dh.pos_data_dictionary_header())+36, 4)
	var sys_table = SYS_TABLES{PRIMARY: uint64(primary), ID: uint64(id)}
	primary = dh.Page.bufferReadat(int64(dh.pos_data_dictionary_header())+40, 4)
	var sys_column = SYS_COLUMNS{PRIMARY: uint64(primary)}
	primary = dh.Page.bufferReadat(int64(dh.pos_data_dictionary_header())+44, 4)
	var sys_indexes = SYS_INDEXES{PRIMARY: uint64(primary)}
	primary = dh.Page.bufferReadat(int64(dh.pos_data_dictionary_header())+48, 4)
	var sys_field = SYS_FIELDS{PRIMARY: uint64(primary)}
	dh.Unused_space = uint64(dh.Page.bufferReadat(int64(dh.pos_data_dictionary_header())+52, 4))
	dh.Fseg = 4 //先不处理
	var indexes = Dict_Index{sys_table, sys_column, sys_indexes, sys_field}
	dh.Indexes = indexes

}

func (dh SysDataDictionaryHeader) String() string {
	dh.data_dictionary_header()
	res := "sysdatadictionaryHeader: xxxxx"
	return res
}
