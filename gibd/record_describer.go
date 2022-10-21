package gibd

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
)

// 记录描述符这应该重构下，描述符这有点混乱
//只实现了系统表systable sysindex 的description
func (index *Index) Make_Record_Description() map[string]interface{} {
	var position [1024]int
	for i := 0; i <= RECORD_MAX_N_FIELDS; i++ {
		position[i] = i
	}
	//用之前的描述符，更改下格式
	description := index.Get_Record_Describer()
	fields := make(map[string]string)

	var field_map_description map[string]interface{}

	switch description.(type) {
	case *SysTablesPrimary:

		description := description.(*SysTablesPrimary)
		fields["type"] = description.TAB_TYPE

		//转化格式，统一下，要不后续不好处理
		field_map_description = Restruct_Describer(*description)

		var counter int
		counter = 0

		var key_arr []*RecordFieldMeta
		for _, v := range field_map_description["key"].([]interface{}) {
			//key_arr = []*Recordfield{}

			value := v.(map[string]interface{})
			prop := value["type"].([]interface{})
			var properties string
			for i := 1; i < len(prop); i++ {
				properties += " " + prop[i].(string)
			}
			rf := NewRecordFieldMeta(position[counter], value["name"].(string), prop[0].(string), properties)

			fmap[counter] = "key"
			key_arr = append(key_arr, rf)
			counter = counter + 1
		}

		field_map_description["key"] = key_arr

		//叶子结点加上系统字段
		var sys_arr []*RecordFieldMeta
		if index.IsLeaf() && field_map_description["tab_type"] == "clustered" {

			DB_TRX_ID := NewRecordFieldMeta(position[counter], "DB_TRX_ID", "TRX_ID", "NOT_NULL")
			fmap[counter] = "sys"
			counter = counter + 1
			sys_arr = append(sys_arr, DB_TRX_ID)
			DB_ROLL_PTR := NewRecordFieldMeta(position[counter], "DB_ROLL_PTR", "ROLL_PTR", "NOT_NULL")
			fmap[counter] = "sys"
			counter = counter + 1
			sys_arr = append(sys_arr, DB_ROLL_PTR)

			field_map_description["sys"] = sys_arr
		}

		var row_arr []*RecordFieldMeta
		if (field_map_description["tab_type"] == "clustered") || (field_map_description["tab_type"] == "secondary") {
			for _, v := range field_map_description["row"].([]interface{}) {
				value := v.(map[string]interface{})
				name := value["name"].(string)
				prop := value["type"].([]interface{})
				var properties string
				for i := 1; i < len(prop); i++ {
					properties += " " + prop[i].(string)
				}
				row := NewRecordFieldMeta(position[counter], name, prop[0].(string), properties)

				fmap[counter] = "row"
				row_arr = append(row_arr, row)
				counter = counter + 1

			}

			field_map_description["row"] = row_arr
		}

		return field_map_description
	case *SysIndexesPrimary:
		description := description.(*SysIndexesPrimary)

		//转化格式，统一下，要不后续不好处理
		field_map_description = Restruct_Describer(*description)
		var counter int
		counter = 0

		var key_arr []*RecordFieldMeta
		for _, v := range field_map_description["key"].([]interface{}) {

			value := v.(map[string]interface{})
			prop := value["type"].([]interface{})
			var properties string
			for i := 1; i < len(prop); i++ {
				properties += " " + prop[i].(string)
			}
			rf := NewRecordFieldMeta(position[counter], value["name"].(string), prop[0].(string), properties)

			fmap[counter] = "key"
			key_arr = append(key_arr, rf)
			counter = counter + 1
		}

		field_map_description["key"] = key_arr

		var sys_arr []*RecordFieldMeta
		// 叶子结点加上回滚段和事务id的值
		if index.IsLeaf() && field_map_description["tab_type"] == "clustered" {

			DB_TRX_ID := NewRecordFieldMeta(position[counter], "DB_TRX_ID", "TRX_ID", "NOT_NULL")
			fmap[counter] = "sys"
			counter = counter + 1
			sys_arr = append(sys_arr, DB_TRX_ID)
			DB_ROLL_PTR := NewRecordFieldMeta(position[counter], "DB_ROLL_PTR", "ROLL_PTR", "NOT_NULL")
			fmap[counter] = "sys"
			counter = counter + 1
			sys_arr = append(sys_arr, DB_ROLL_PTR)

			field_map_description["sys"] = sys_arr
		}

		var row_arr []*RecordFieldMeta
		if (field_map_description["tab_type"] == "clustered") || (field_map_description["tab_type"] == "secondary") {
			for _, v := range field_map_description["row"].([]interface{}) {
				value := v.(map[string]interface{})
				name := value["name"].(string)
				prop := value["type"].([]interface{})
				var properties string
				for i := 1; i < len(prop); i++ {
					properties += " " + prop[i].(string)
				}
				row := NewRecordFieldMeta(position[counter], name, prop[0].(string), properties)
				fmap[counter] = "row"
				row_arr = append(row_arr, row)
				counter = counter + 1

			}

			field_map_description["row"] = row_arr
		}
		return field_map_description

	default:
		fmt.Printf("\n")
	}

	return field_map_description
}

//将字段转换成map 格式，并且分开，key和普通的字段
// 转换后的格式map[key:[*recordFieldMeta...],row:[*recordFieldMeta...],tab_type:""]
func Restruct_Describer(a interface{}) map[string]interface{} {

	typ := reflect.TypeOf(a)
	//获取reflect.Type类型
	val := reflect.ValueOf(a)
	//获取到a对应的类别
	kd := val.Kind()

	if kd != reflect.Struct {
		return nil
	}
	//获取到该结构体有几个字段
	num := val.NumField()
	//Log.Info("该结构体有%d个字段\n", num) //4个

	var str_type string
	var str_key string
	var str_row string
	str_key = `"key":[`
	str_row = `"row":[`
	//遍历结构体的所有字段
	for i := 0; i < num; i++ {
		//获取到struct标签，需要通过reflect.Type来获取tag标签的值
		tagVal := typ.Field(i).Tag.Get("json")
		//如果该字段有tag标签就显示，否则就不显示
		if tagVal != "" {
			if tagVal == "tab_type" {
				x := val.Field(i).Interface().(string)
				str_type = `{"tab_type":"` + x + `",`
			} else {
				fieldstr := val.Field(i).Interface().(RecordFieldMeta)

				if fieldstr.IsKey {

					str_key += `{"name":"` + fieldstr.Name + `",` + `"type":["` + fieldstr.DataType.(string) + `","` + fieldstr.Properties + `"],` + `"nullable":["` + strconv.FormatBool(fieldstr.Nullable) + `"],` + `"length":["` + strconv.Itoa(fieldstr.Length) + `"],` + `"position":["` + strconv.Itoa(fieldstr.Position) + `"],` + `"iskey":["` + strconv.FormatBool(fieldstr.IsKey) + `"]},`
				} else {

					str_row += `{"name":"` + fieldstr.Name + `",` + `"type":["` + fieldstr.DataType.(string) + `","` + fieldstr.Properties + `"],` + `"nullable":["` + strconv.FormatBool(fieldstr.Nullable) + `"],` + `"length":["` + strconv.Itoa(fieldstr.Length) + `"],` + `"position":["` + strconv.Itoa(fieldstr.Position) + `"],` + `"iskey":["` + strconv.FormatBool(fieldstr.IsKey) + `"]},`
				}
			}
		}
	}

	str_key = str_key[:len(str_key)-1] + `],`
	str_row = str_row[:len(str_row)-1] + `]}`

	m := make(map[string]interface{})

	b := []byte(str_type + str_key + str_row)
	err := json.Unmarshal(b, &m)
	if err != nil {
		fmt.Println("Umarshal failed:", err)
		return nil
	}

	return m
}
