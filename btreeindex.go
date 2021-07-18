package main

import "fmt"

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
	page.record_describer = index.Record_describer
	i := newIndex(page)
	return i
}

func (index *BTreeIndex) each_record() []*Record {
	var records []*Record
	pages_at_level0 := index.each_page_at_level(0)

	for i := 0; i < len(pages_at_level0); i++ {
		res := pages_at_level0[i].each_record()
		for j := 0; j < len(res); j++ {
			records = append(records, res[j])
		}
	}
	return records
}

func (index *BTreeIndex) each_page_at_level(level int) []*Index {
	// var pages []*Page
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
	println("min_page_at_level get root min_record root number is ==>", index.Root.Page.Page_number)
	idx := index.Root
	record := idx.min_record()
	fmt.Printf("min_page_at_level get record========>%+v\n", record)
	panic(-1)
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
	// for {
	// 	if record != nil && idx.pageHeader.level > uint64(level) {
	// 		idx := index.page(record.child_page_number)
	// 		record = idx.min_record()
	// 	}
	// }

	return nil
}

func (index *BTreeIndex) min_record_in_index(level int) *Record {
	return index.min_page_at_level(0).min_record()
}
