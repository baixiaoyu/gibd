package gibd

//表示树，针对树的一些操作
type BTreeIndex struct {
	Root             *Index //相当于节点
	Space            *Space
	Record_describer interface{}
}

func NewBTreeIndex(space *Space, root_page_number uint64, record_describer interface{}) *BTreeIndex {
	tree := &BTreeIndex{Space: space}
	root := tree.Page(root_page_number)
	tree.Record_describer = record_describer
	tree.Space.Record_describer = record_describer
	root.record_describer = record_describer
	root.Index_Header()
	tree.Root = root
	return tree
}

func (tree *BTreeIndex) Page(page_number uint64) *Index {
	page := tree.Space.Page(page_number)
	page.record_describer = tree.Record_describer
	i := NewIndex(page)
	return i
}

func (tree *BTreeIndex) Each_Record(dh *DataDictionary) []*Record {
	var records []*Record
	pages_at_level0 := tree.Each_Page_At_Level(0, dh)
	// for _, value := range pages_at_level0 {
	// 	fmt.Printf("btreeindex pages_at_level0,========>%+v\n", value)

	// }
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

func (tree *BTreeIndex) Each_Page_At_Level(level int, dh *DataDictionary) []*Index {
	min_page := tree.Min_Page_At_Level(level)
	min_page.dh = dh
	pages := tree.Each_Page_From(min_page)
	return pages
}

func (tree *BTreeIndex) Each_Page_From(idx *Index) []*Index {
	var pages []*Index
	for {
		Log.Info("each_page_from  idx.next,========>%+v\n", idx.Page.FileHeader.Next)

		if idx.Page.FileHeader.Page_type == 17855 {
			pages = append(pages, idx)
		}
		if idx.Page.FileHeader.Next == 4294967295 {
			break
		} else {
			idx = tree.Page(idx.Page.FileHeader.Next)
		}

	}
	return pages
}

func (tree *BTreeIndex) Min_Page_At_Level(level int) *Index {
	// fmt.Printf("min_page_at_level get root min_record root number is ==>%d\n", tree.Root.Page.Page_number)
	// fmt.Printf("min_page_at_level level is ==>%d\n", level)
	root_index_page := tree.Root
	// fmt.Printf("root_index_page ,========>%+v\n", root_index_page)
	record := root_index_page.Min_Record()
	// fmt.Printf("min_page_at_level get record,========>%+v\n", record)
	// fmt.Printf("min_page_at_level ,idx.pageHeader.level========>%+v\n", root_index_page.PageHeader.Level)

	for record != nil && root_index_page.PageHeader.Level > uint64(level) {
		switch record.record.(type) {
		case *UserRecord:
			child_page_number := record.record.(*UserRecord).Child_page_number
			idx := tree.Page(child_page_number)

			record = idx.Min_Record()
			if idx.PageHeader.Level == uint64(level) {
				return idx
			}
		}

	}
	return root_index_page
}

func (tree *BTreeIndex) Min_Record_In_Index(level int) *Record {
	return tree.Min_Page_At_Level(0).Min_Record()
}
