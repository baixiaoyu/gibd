package gibd

//系统表空间
type System struct {
	config          map[string]string
	spaces          map[uint64]*Space
	orphans         []Space
	data_dictionary *DataDictionary
}

func NewSystem(filenames []string) *System {
	system := new(System)
	system.config = make(map[string]string)
	system.config["datadir"] = filenames[0]

	space := NewSpace(filenames)
	system.spaces = make(map[uint64]*Space)
	system.spaces[space.Space_id] = space
	//	system.Add_Space_File(filenames)
	system.data_dictionary = NewDataDictionary(system)
	return system
}
func (system *System) Add_Space(space *Space) {
	system.spaces[space.Space_id] = space
}

// func (system *System) Add_Space_File(space_id uint64,space_filenames []string) {
// space := NewSpace(space_filenames)
// space.Innodb_system = system
// system.Add_Space(space)

// }

func (system *System) System_Space() *Space {
	for _, value := range system.spaces {

		if value.IsSystemSpace {
			return value
		}
	}
	return nil
}

func (system *System) Each_Table_Name() []string {
	var table_names []string
	tables := system.data_dictionary.Get_Each_Table_Name()
	for _, value := range tables {
		table_names = append(table_names, value["NAME"].(string))
	}
	return table_names
}
