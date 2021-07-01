package main

type System struct {
	config          map[string]string
	spaces          map[uint64]*Space
	orphans         []Space
	data_dictionary *DataDictionary
}

func newSystem(filenames []string) *System {
	system := new(System)
	system.config = make(map[string]string)
	system.config["datadir"] = filenames[0]

	space := newSpace(filenames)
	system.spaces = make(map[uint64]*Space)
	system.spaces[space.space_id] = space
	system.add_space_file(filenames)
	system.data_dictionary = newDataDictionary(system)
	return system
}
func (system *System) add_space(space *Space) {
	system.spaces[space.space_id] = space
}
func (system *System) add_space_file(space_filenames []string) {
	space := newSpace(space_filenames)
	space.innodb_system = system
	system.add_space(space)
}

func (system *System) system_space() *Space {
	for _, value := range system.spaces {
		if value.innodb_system != nil {
			return value
		}
	}
	return nil
}
