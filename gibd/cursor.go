package gibd

type RecordCursor struct {
	Initial   bool
	Index     *Index
	Direction string
	Record    *Record
}

const min = 0
const max = 4294967295

func (rc *RecordCursor) Initial_Record(offset uint64) *Record {
	switch offset {
	case min:
		return rc.Index.Min_Record()
	case max:
		return rc.Index.Max_Record()
	default:
		return rc.Index.record(uint64(offset))
	}
}

func NewRecordCursor(index *Index, offset uint64, direction string) *RecordCursor {
	Initial := true
	Index := index
	Direction := direction
	a := RecordCursor{Initial: Initial, Index: Index, Direction: Direction}
	a.Record = a.Initial_Record(offset)
	return &a
}

func (index *Index) Record_Cursor(offset uint64, direction string) *RecordCursor {

	return NewRecordCursor(index, offset, direction)
}

func (rc *RecordCursor) record() *Record {
	//var records *Record
	if rc.Initial == true {
		rc.Initial = false
		return rc.Record
	}
	switch rc.Direction {
	case "forward":
		return rc.Next_Record()
	case "backward":
		return rc.Prev_Record()
	}
	return nil
}

func (rc *RecordCursor) Next_Record() *Record {
	// page_record_cursor_next_record = page_record_cursor_next_record + 1

	rec := rc.Index.record(rc.Record.record.(*UserRecord).header.Next)

	var next_record_offset uint64
	var rc_record_offset uint64

	supremum := rc.Index.Supremum()
	rc_record_offset = rc.Record.record.(*UserRecord).offset

	switch rec.record.(type) {
	case *UserRecord:

		next_record_offset = rec.record.(*UserRecord).offset
		next_record := rec.record.(*UserRecord)
		if (next_record.header.Next == supremum.record.(*SystemRecord).header.Next) || next_record_offset == rc_record_offset {
			return nil
		} else {
			return rec
		}
	case *SystemRecord:
		next_record_offset = rec.record.(*SystemRecord).offset
		next_record := rec.record.(*SystemRecord)
		if (next_record.header.Next == supremum.record.(*SystemRecord).header.Next) || next_record_offset == rc_record_offset {
			return nil
		} else {
			return rec
		}
	}

	return nil
}

func (rc *RecordCursor) Prev_Record() *Record {
	var records *Record
	return records
}
