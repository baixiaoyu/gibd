package gibd

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"unsafe"

	"github.com/astaxie/beego/logs"
)

var Log *logs.BeeLogger

func init() {

	Log = MyNewLogger("innoblock.log")
}
func IsLittleEndian() bool {
	var i int32 = 0x01020304
	u := unsafe.Pointer(&i)
	pb := (*byte)(u)
	b := *pb
	return (b == 0x04)
}

func BytesToUIntLittleEndian(b []byte) (int, error) {

	if len(b) == 3 {
		b = append([]byte{0}, b...)
	}
	bytesBuffer := bytes.NewBuffer(b)
	switch len(b) {
	case 1:
		var tmp uint8
		err := binary.Read(bytesBuffer, binary.BigEndian, &tmp)
		return int(tmp), err
	case 2:
		var tmp uint16
		err := binary.Read(bytesBuffer, binary.BigEndian, &tmp)
		return int(tmp), err
	case 4:
		var tmp uint32
		err := binary.Read(bytesBuffer, binary.BigEndian, &tmp)
		return int(tmp), err

	case 8:
		var tmp uint64
		err := binary.Read(bytesBuffer, binary.BigEndian, &tmp)
		return int(tmp), err
	default:
		return 0, fmt.Errorf("%s", "BytesToInt bytes lenth is invaild!")
	}
}

func MyNewLogger(logFile string) *logs.BeeLogger {
	return LoggerInit(logFile)
}

func LoggerInit(logFile string) (log *logs.BeeLogger) {
	log = logs.NewLogger(0)
	log.EnableFuncCallDepth(true)
	log.SetLevel(7)

	_ = log.SetLogger(
		"file", fmt.Sprintf(
			`{"filename":"%s", "level":%d, "maxlines":0,
				"maxsize":0, "daily":false, "maxdays":0}`,
			logFile, 7))

	return
}
