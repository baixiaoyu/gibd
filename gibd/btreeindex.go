package gibd

//表示树，针对树的一些操作
type BTreeIndex struct {
	Root             *Index //相当于节点
	Space            *Space
	Record_describer interface{}
}

func NewBTreeIndex(space *Space, root_page_number uint64, record_describer interface{}) *BTreeIndex {
	index := &BTreeIndex{Space: space}
	root := index.Page(root_page_number)
	index.Record_describer = record_describer
	root.record_describer = record_describer
	index.Root = root
	return index
}

func (index *BTreeIndex) Page(page_number uint64) *Index {
	page := index.Space.Page(page_number)
	page.record_describer = index.Record_describer
	i := NewIndex(page)
	return i
}

func (index *BTreeIndex) Each_Record(dh *DataDictionary) []*Record {
	var records []*Record
	pages_at_level0 := index.Each_Page_At_Level(0, dh)
	for _, value := range pages_at_level0 {
		Log.Info("btreeindex pages_at_level0,========>%+v\n", value)

	}
	Log.Info("btreeindex pages_at_level0 length,========>%+v\n", len(pages_at_level0))
	for i := 0; i < len(pages_at_level0); i++ {
		res := pages_at_level0[i].each_record()
		for j := 0; j < len(res); j++ {
			records = append(records, res[j])
		}
	}
	Log.Info("btreeindex record length,========>%+v\n", len(records))

	return records
}

func (index *BTreeIndex) Each_Page_At_Level(level int, dh *DataDictionary) []*Index {
	min_page := index.Min_Page_At_Level(level)
	min_page.dh = dh
	pages := index.Each_Page_From(min_page)
	return pages
}

func (index *BTreeIndex) Each_Page_From(idx *Index) []*Index {
	var pages []*Index
	for {
		Log.Info("each_page_from  idx.next,========>%+v\n", idx.Page.FileHeader.Next)

		if idx.Page.FileHeader.Page_type == 17855 {
			pages = append(pages, idx)
		}
		if idx.Page.FileHeader.Next == 4294967295 {
			break
		} else {
			idx = index.Page(idx.Page.FileHeader.Next)
		}

	}
	return pages
}

func (index *BTreeIndex) Min_Page_At_Level(level int) *Index {
	Log.Info("min_page_at_level get root min_record root number is ==>%d", index.Root.Page.Page_number)
	idx := index.Root
	record := idx.Min_Record()
	Log.Info("min_page_at_level get record,========>%+v\n", record)
	Log.Info("min_page_at_level ,idx.pageHeader.level========>%+v\n", idx.PageHeader.Level)

	for record != nil && idx.PageHeader.Level > uint64(level) {
		switch record.record.(type) {
		case *UserRecord:
			idx := index.Page(record.record.(*UserRecord).Child_page_number)
			record = idx.Min_Record()
			if idx.PageHeader.Level == uint64(level) {
				return idx
			}
		}

	}
	return idx
}

func (index *BTreeIndex) Min_Record_In_Index(level int) *Record {
	return index.Min_Page_At_Level(0).Min_Record()
}
