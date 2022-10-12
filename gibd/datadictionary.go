package gibd

import (
	"fmt"
	"reflect"
	"strconv"
)

type Field struct {
	FieldName string `json:"fieldname"`
	// FieldDesc string `json:"fieldesc"`
	DataType   string `json:"datatype"`
	Properties string `json:Properties`
	IsNull     string `json:"isnull"`
	Length     int    `json:"length"`
	Is_key     bool   `json:"is_key"`
}
type SysTablesPrimary struct {
	TAB_TYPE     string `json:"tab_type"`
	NAME         Field  `json:"name"`
	ID           Field  `json:"id"`
	N_COLS       Field  `json:"n_cols"`
	TYPE         Field  `json:"type"`
	MIX_ID       Field  `json:"mix_id"`
	MIX_LEN      Field  `json:"mix_len"`
	CLUSTER_NAME Field  `json:"clcuster_name"`
	SPACE        Field  `json:"space"`
}

func NewSysTablesPrimary() *SysTablesPrimary {
	field_name := Field{"NAME", "VARCHAR(100)", "", "NOT_NULL", 100, true}
	field_id := Field{"ID", "BIGINT", "UNSIGNED", "NOT_NULL", 0, false}
	field_n_cols := Field{"N_COLS", "INT", "UNSIGNED", "NOT_NULL", 0, false}
	field_type := Field{"TYPE", "INT", "UNSIGNED", "NOT_NULL", 0, false}
	field_mix_id := Field{"MIX_ID", "BIGINT", "UNSIGNED", "NOT_NULL", 0, false}
	field_mix_len := Field{"MIX_LEN", "INT", "UNSIGNED", "NOT_NULL", 0, false}
	field_cluster_name := Field{"CLUSTER_NAME", "VARCHAR(100)", "", "NOT_NULL", 100, false}
	field_space := Field{"SPACE", "INT", "UNSIGNED", "NOT_NULL", 0, false}
	return &SysTablesPrimary{"clustered", field_name, field_id, field_n_cols, field_type, field_mix_id, field_mix_len, field_cluster_name, field_space}
}

type SysTablesId struct {
	TAB_TYPE string
	ID       Field
	NAME     Field
}

func NewSysTablesId() *SysTablesId {
	field_name := Field{"NAME", "VARCHAR(100)", "", "NOT_NULL", 100, false}
	field_id := Field{"ID", "BIGINT", "UNSIGNED", "NOT_NULL", 0, true}

	return &SysTablesId{"secondary", field_name, field_id}
}

type SysColumnsPrimary struct {
	TAB_TYPE string `json:"tab_type"`
	TABLE_ID Field  `json:"table_id"`
	POS      Field  `json:"pos"`
	NAME     Field  `json:"name"`
	MTYPE    Field  `json:"mtype"`
	PRTYPE   Field  `json:"prtype"`
	LEN      Field  `json:"len"`
	PREC     Field  `json:"prec"`
}

func NewSysColumnsPrimary() *SysColumnsPrimary {
	field_table_id := Field{"TABLE_ID", "BIGINT", "", "NOT_NULL", 0, true}
	field_pos := Field{"POS", "INT", "UNSIGNED", "NOT_NULL", 0, true}
	field_name := Field{"NAME", "VARCHAR(100)", "", "NOT_NULL", 100, false}
	field_mtype := Field{"MTYPE", "INT", "", "NOT_NULL", 0, false}
	field_prtype := Field{"PRTYPE", "INT", "UNSIGNED", "NOT_NULL", 0, false}
	field_len := Field{"LEN", "INT", "UNSIGNED", "NOT_NULL", 0, false}
	field_prec := Field{"PREC", "INT", "UNSIGNED", "NOT_NULL", 0, false}

	return &SysColumnsPrimary{"clustered", field_table_id, field_pos, field_name, field_mtype, field_prtype, field_len, field_prec}
}

type SysIndexesPrimary struct {
	TAB_TYPE string `json:"tab_type"`
	TABLE_ID Field  `json:"table_id"`
	ID       Field  `json:"id"`
	NAME     Field  `json:"name"`
	N_FIELDS Field  `json:"n_fields"`
	TYPE     Field  `json:"type"`
	SPACE    Field  `json:"space"`
	PAGE_NO  Field  `json:"page_no"`
}

func NewSysIndexesPrimary() *SysIndexesPrimary {
	field_table_id := Field{"TABLE_ID", "BIGINT", "UNSIGNED", "NOT_NULL", 0, true}
	field_id := Field{"ID", "BIGINT", "UNSIGNED", "NOT_NULL", 0, true}
	field_name := Field{"NAME", "VARCHAR(100)", "", "NOT_NULL", 100, false}
	field_n_field := Field{"N_FIELDS", "INT", "UNSIGNED", "NOT_NULL", 0, false}
	field_type := Field{"TYPE", "INT", "UNSIGNED", "NOT_NULL", 0, false}
	field_space := Field{"SPACE", "INT", "UNSIGNED", "NOT_NULL", 0, false}
	field_page_no := Field{"PAGE_NO", "INT", "UNSIGNED", "NOT_NULL", 0, false}

	return &SysIndexesPrimary{"clustered", field_table_id, field_id, field_name, field_n_field, field_type, field_space, field_page_no}
}

type SysFieldsPrimary struct {
	TAB_TYPE string `json:"tab_type"`
	INDEX_ID Field  `json:"index_id"`
	POS      Field  `json:"pos"`
	COL_NAME Field  `json:"col_name"`
}

func NewSysFieldsPrimary() *SysFieldsPrimary {
	field_index_id := Field{"INDEX_ID", "BIGINT", "UNSIGNED", "NOT_NULL", 0, true}
	field_pos := Field{"POS", "INT", "UNSIGNED", "NOT_NULL", 0, true}
	field_col_name := Field{"COL_NAME", "VARCHAR(100)", "", "NOT_NULL", 100, false}

	return &SysFieldsPrimary{"clustered", field_index_id, field_pos, field_col_name}
}

var DATA_DICTIONARY_RECORD_DESCRIBERS = map[string]map[string]string{
	"SYS_TABLES": {
		"PRIMARY": "SysTablesPrimary",
		"ID":      "SysTablesId",
	},
	"SYS_COLUMNS": {"PRIMARY": "SysColumnsPrimary"},
	"SYS_INDEXES": {"PRIMARY": "SysIndexesPrimary"},
	"SYS_FIELDS":  {"PRIMARY": "SysFieldsPrimary"},
}

var describer_struct_map = map[string]reflect.Type{
	"SysTablesPrimary":  reflect.TypeOf(&SysTablesPrimary{}).Elem(),
	"SysColumnsPrimary": reflect.TypeOf(&SysColumnsPrimary{}).Elem(),
	"SysIndexesPrimary": reflect.TypeOf(&SysIndexesPrimary{}).Elem(),
	"SysFieldsPrimary":  reflect.TypeOf(&SysFieldsPrimary{}).Elem(),
}

func New(name string) (c interface{}, err error) {
	if v, ok := describer_struct_map[name]; ok {
		fmt.Println("vvvvvvv======", v)
		c = reflect.New(v).Interface()
	} else {
		err = fmt.Errorf("not found %s struct", name)
	}
	return
}

type DataDictionary struct {
	system *System
}

func NewDataDictionary(system *System) *DataDictionary {
	return &DataDictionary{system: system}
}
func (dh *DataDictionary) Each_Table() []map[string]interface{} {
	res := dh.Each_Record_From_Data_Dictionary_Index("SYS_TABLES", "PRIMARY")
	var all_record_field []map[string]interface{}
	for i := 0; i < len(res); i++ {
		Log.Info("each_table each table======>%+v\n", res[i])
		all_record_field = append(all_record_field, res[i].Get_Fields())
	}
	Log.Info("each_table=====>length is:%+v\n", len(all_record_field))

	return all_record_field
}

// "SYS_INDEXES", "PRIMARY"
func (dh *DataDictionary) Each_Index_Recor_Field(tableName string, indexName string) []map[string]interface{} {

	res := dh.Each_Record_From_Data_Dictionary_Index(tableName, indexName)
	fmt.Printf("res %v", res)
	var all_record_field []map[string]interface{}
	for i := 0; i < len(res); i++ {
		Log.Info("each_index each index======>%+v", res[i])
		all_record_field = append(all_record_field, res[i].Get_Fields())
	}
	Log.Info("each_record_from_data_dictionary_index=====>all_record_field is:%+v", all_record_field)

	return all_record_field
}

func (dh *DataDictionary) Each_Record_From_Data_Dictionary_Index(table string, index string) []*Record {

	// root index tree
	rootindex := dh.Get_Data_Dictionary_Index_Tree(table, index)
	fmt.Printf("each_record_from_data_dictionary_index rootindex========>%+v\n", rootindex)

	records := rootindex.Each_Record(dh)
	// 对返回的每个记录进行处理

	Log.Info("each_record_from_data_dictionary_index 所有的记录数%+v\n", len(records))
	Log.Info("each_record_from_data_dictionary_index 所有的记录%+v\n", records)
	// for i := 0; i < len(records); i++ {
	// 	Log.Info("each_record_from_data_dictionary_index_page_number======>%+v\n", records[i].Get_Fields())
	// }
	return records
}

func (dh *DataDictionary) CheckNestedStruct(table_entry interface{}, table_name string, index_name string, find_table bool, find_all bool) uint64 {
	// find_table := false

	var res uint64

	e := reflect.ValueOf(table_entry).Elem()
	for i := 0; i < e.NumField(); i++ {
		field := e.Field(i)
		varName := e.Type().Field(i).Name
		//varType := e.Type().Field(i).Type
		fieldType := field.Kind()
		if find_table && (varName == index_name) {
			varValue := e.Field(i).Interface()
			find_all = true

			switch value := varValue.(type) {
			case uint64:
				res = value
				return res
			case string:
				Log.Info("list is a string and its value is %s\n", value)
			default:
				fmt.Println("list is of a different type%s", value)
			}

			//return varValue
		}
		//Log.Info("%v  %v\n", varName, fieldType)
		if fieldType == reflect.Struct {
			if varName == table_name {
				find_table = true

			} else {
				find_table = false
			}
			if !find_all {

				res = res + dh.CheckNestedStruct(field.Addr().Interface(), table_name, index_name, find_table, find_all)
			}

		}
	}

	return res

}

func (dh *DataDictionary) Is_Data_Dictionary_Table(table_name string) bool {
	if _, ok := DATA_DICTIONARY_RECORD_DESCRIBERS[table_name]; ok {
		return true
	} else {
		return false
	}
}

func (dh *DataDictionary) Is_Data_Dictionary_Index(table_name string, index_name string) bool {
	if dh.Is_Data_Dictionary_Table(table_name) {
		if _, ok := DATA_DICTIONARY_RECORD_DESCRIBERS[table_name][index_name]; ok {
			return true
		} else {
			return false
		}
	}
	return false
}

func (dh *DataDictionary) Data_Dictionary_Index_Describer(table_name string, index_name string) interface{} {
	//返回描述符，用这个 描述符创建index，这个需要明确类型
	if dh.Is_Data_Dictionary_Index(table_name, index_name) {

		class_name := DATA_DICTIONARY_RECORD_DESCRIBERS[table_name][index_name]
		fmt.Printf("data_dictionary_index_describer get index describer======>%+v\n", class_name)
		cls, _ := New(class_name)
		fmt.Println("#####", cls)
		return cls
	}
	return nil
}

// return and Index object
// 这个index_root_page 对吗？获取的level=1导致没有加 sys，导致了判断错误
func (dh *DataDictionary) Get_Data_Dictionary_Index_Tree(table_name string, index_name string) *BTreeIndex {
	var index_root_page uint64
	if table_name == "SYS_TABLES" {
		table_entry := dh.data_dictionary_indexes().SYS_TABLES
		fmt.Printf("in data_dictionary_index, table_entry ========>%+v\n", table_entry.PRIMARY)
		index_root_page = table_entry.PRIMARY
	} else if table_name == "SYS_INDEXES" {
		table_entry := dh.data_dictionary_indexes().SYS_INDEXES
		fmt.Printf("in data_dictionary_index, table_entry ========>%+v\n", table_entry.PRIMARY)
		index_root_page = table_entry.PRIMARY
	}

	//SYS_INDEXES PRIMARY root_page应该是11
	//SYS_TABLES PRIMARY root_page应该是8
	//res := dh.CheckNestedStruct(&table_entry, table_name, index_name, false, false)
	//index_root_page := res

	record_describer := dh.Data_Dictionary_Index_Describer(table_name, index_name)

	fmt.Printf("^^^^^^^^^%v", record_describer)

	switch value := record_describer.(type) {
	case *SysTablesPrimary:
		record_describer = NewSysTablesPrimary()
		// res := value
		// jsons, _ := json.Marshal(res)
		// println(jsons)
	case *SysIndexesPrimary:
		//res := record_describer.(*SysIndexesPrimary)
		record_describer = NewSysIndexesPrimary()
		// jsons, _ := json.Marshal(*res)
		// println(string(jsons))
	default:
		fmt.Println("description is of a different type%T", value)
	}
	Log.Info("data_dictionary_index_record_describer======>%+v\n", record_describer)

	return dh.system.System_Space().Get_Index_Tree(index_root_page, record_describer)

}

//table_name string
func (dh *DataDictionary) data_dictionary_indexes() Dict_Index {
	page := dh.system.System_Space().Data_Dictionary_Header_Page()
	header := NewSysDataDictionaryHeader(page)
	header.Data_Dictionary_Header()
	return header.Indexes
}

func (dh *DataDictionary) each_index_by_space_id(space_id uint64) []map[string]interface{} {
	all_record_field := dh.Each_Index_Recor_Field("SYS_INDEXES", "PRIMARY")
	var records []map[string]interface{}
	//根据上面返回的每个记录进行sapce的匹配，匹配的话输出
	Log.Info("each_index_by_space_id() space_id =======>%+v\n", space_id)

	for _, record := range all_record_field {
		Log.Info("each_index_by_space_id() =======>%+v\n", record)
		Log.Info("each_index_by_space_id() record[space]=======>%+v\n", record["SPACE"])

		space_no := uint64(record["SPACE"].(int64))
		if space_no == space_id {
			records = append(records, record)
		}
	}
	Log.Info("each_index_by_space_id() records length is======%d", len(records))
	return records

}

func Record_Describer_By_Index_Id(dh *DataDictionary, index_id uint64) interface{} {

	defer func() {
		//捕获异常
		err := recover()
		if err != nil { //条件判断，是否存在异常
			//存在异常,抛出异常
			fmt.Println(err)
		}
	}()

	dd_index := dh.Data_Dictionary_Index_Ids()[index_id]
	if dd_index != nil {
		return dh.Data_Dictionary_Index_Describer(dd_index["table"], dd_index["index"])
	} else {
		index := dh.Index_By_Id(index_id)
		table_id, _ := strconv.ParseUint(index["TABLE_ID"], 10, 64)
		table := dh.Table_By_Id(table_id)
		return dh.Record_Describer_By_Index_Name(table["NAME"], index["NAME"])
	}
}

func (dh *DataDictionary) Record_Describer_By_Index_Name(table string, index string) interface{} {
	return nil
}

func (dh *DataDictionary) Data_Dictionary_Index_Ids() map[uint64]map[string]string {
	// if dh.data_dictionary_index_ids != nil {
	// 	return dh.data_dictionary_index_ids
	// } else {
	data_dictionary_index_ids := make(map[uint64]map[string]string)
	//indexes := dh.data_dictionary_indexes()
	return data_dictionary_index_ids
	// data_dictionary_indexes.each do |table, indexes|
	//     indexes.each do |index, root_page_number|
	//       root_page = system_space.page(root_page_number)
	//       next unless root_page

	//       @data_dictionary_index_ids[root_page.index_id] = {
	//         table: table,
	//         index: index,
	//       }
	//     end
	//   end

	//   @data_dictionary_index_ids

}

func (dh *DataDictionary) Index_By_Id(index_id uint64) map[string]string {
	return dh.Object_By_Field("each_index", "ID", index_id)

}

func (dh *DataDictionary) Table_By_Id(table_id uint64) map[string]string {
	return dh.Object_By_Field("each_table", "ID", table_id)
}

func (dh *DataDictionary) Object_By_Field(method string, field string, values uint64) map[string]string {
	res := make(map[string]string)
	res["key"] = "value"
	return res
}
