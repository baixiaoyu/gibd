package main

type BTreeIndex struct {
	Root             *Index
	Space            *Space
	Record_describer interface{}
}

func newBTreeIndex(space *Space, root_page_number uint64, record_describer interface{}) *BTreeIndex {
	println("btree index")

	index := &BTreeIndex{Space: space}
	root := index.page(root_page_number)
	index.Record_describer = record_describer
	root.record_describer = record_describer
	index.Root = root
	return index
}

func (index *BTreeIndex) page(page_number uint64) *Index {
	page := index.Space.page(page_number)
	//page.record_describer = Record_describer
	i := newIndex(page)
	return i
}

func (index *BTreeIndex) each_record() []*Record {
	println("btindexeach_record")
	var records []*Record
	pages_at_level0 := index.each_page_at_level(0)

	for i := 0; i < len(pages_at_level0); i++ {
		println("=======")
		println(pages_at_level0[i])
		res := pages_at_level0[i].each_record()
		for j := 0; j < len(res); j++ {
			records = append(records, res[j])
		}
	}
	return records
}

func (index *BTreeIndex) each_page_at_level(level int) []*Index {
	// var pages []*Page
	println("each_page_at_level")
	min_page := index.min_page_at_level(level)
	pages := index.each_page_from(min_page)
	return pages
}

func (index *BTreeIndex) each_page_from(idx *Index) []*Index {
	var pages []*Index
	for {
		if idx.Page.FileHeader.Page_type == 17855 {
			pages = append(pages, idx)
		}
		idx = index.page(idx.Page.FileHeader.Next)
	}
}

func (index *BTreeIndex) min_page_at_level(level int) *Index {
	println("min_page_at_level")
	idx := index.Root
	record := idx.min_record()

	for {
		if record != nil && idx.pageHeader.level > uint64(level) {
			idx := index.page(record.child_page_number)
			record = idx.min_record()
		}
	}
	if idx.pageHeader.level == uint64(level) {
		return idx
	}
	return nil
}
