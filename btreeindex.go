package main

type BTreeIndex struct {
	Root             *Index
	Space            *Space
	Record_describer interface{}
}

func newBTreeIndex(space *Space, root_page_number uint64, record_describer interface{}) *BTreeIndex {
	index := &BTreeIndex{Space: space}
	root := index.page(root_page_number)
	index.Record_describer = record_describer
	root.record_describer = record_describer
	index.Root = root
	return index
}

func (index *BTreeIndex) page(page_number uint64) *Index {
	page := index.Space.page(page_number)
	page.record_describer = index.Record_describer
	i := newIndex(page)
	return i
}

func (index *BTreeIndex) each_record() []*Record {
	var records []*Record
	pages_at_level0 := index.each_page_at_level(0)
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

func (index *BTreeIndex) each_page_at_level(level int) []*Index {
	min_page := index.min_page_at_level(level)
	pages := index.each_page_from(min_page)
	return pages
}

func (index *BTreeIndex) each_page_from(idx *Index) []*Index {
	var pages []*Index
	for {
		Log.Info("each_page_from  idx.next,========>%+v\n", idx.Page.FileHeader.Next)

		if idx.Page.FileHeader.Page_type == 17855 {
			pages = append(pages, idx)
		}
		if idx.Page.FileHeader.Next == 4294967295 {
			break
		} else {
			idx = index.page(idx.Page.FileHeader.Next)
		}

	}
	return pages
}

func (index *BTreeIndex) min_page_at_level(level int) *Index {
	Log.Info("min_page_at_level get root min_record root number is ==>%d", index.Root.Page.Page_number)
	idx := index.Root
	record := idx.min_record()
	Log.Info("min_page_at_level get record,========>%+v\n", record)
	Log.Info("min_page_at_level ,idx.pageHeader.level========>%+v\n", idx.pageHeader.level)

	for record != nil && idx.pageHeader.level > uint64(level) {
		switch record.record.(type) {
		case *UserRecord:
			idx := index.page(record.record.(*UserRecord).child_page_number)
			record = idx.min_record()
			if idx.pageHeader.level == uint64(level) {
				return idx
			}
		}

	}
	return idx
}

func (index *BTreeIndex) min_record_in_index(level int) *Record {
	return index.min_page_at_level(0).min_record()
}
