package gibd

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"regexp"
	"time"
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

func BytesToUIntLittleEndian1(b []byte) (int, error) {

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

func BufferReadAt(p *Page, offset int64, size int64) int {

	byteStorage := make([]byte, size)
	byteReader := bytes.NewReader(*p.Buffer)
	byteReader.ReadAt(byteStorage, offset)

	return BytesToUIntLittleEndian(byteStorage)

}

func BufferReadAtToSignInt(p *Page, offset int64, size int64) int {

	byteStorage := make([]byte, size)
	byteReader := bytes.NewReader(*p.Buffer)
	byteReader.ReadAt(byteStorage, offset)

	return BytesToIntLittleEndian(p, byteStorage)
}

func ReadBytes(p *Page, offset int64, size int64) []byte {

	byteStorage := make([]byte, size)
	byteReader := bytes.NewReader(*p.Buffer)
	byteReader.ReadAt(byteStorage, offset)

	return byteStorage
}

// func (p *Page) test(b []byte) int {

// 	if len(b) == 3 {
// 		b = append([]byte{0}, b...)
// 	}
// 	bytesBuffer := bytes.NewBuffer(b)
// 	switch len(b) {
// 	case 1:
// 		var tmp uint8
// 		binary.Read(bytesBuffer, binary.LittleEndian, &tmp)
// 		return int(tmp)
// 	case 2:
// 		var tmp uint16
// 		binary.Read(bytesBuffer, binary.LittleEndian, &tmp)
// 		return int(tmp)
// 	case 4:
// 		var tmp uint32
// 		binary.Read(bytesBuffer, binary.LittleEndian, &tmp)
// 		return int(tmp)

// 	case 8:
// 		var tmp uint64
// 		binary.Read(bytesBuffer, binary.LittleEndian, &tmp)
// 		return int(tmp)
// 	default:
// 		return 0
// 	}
// }

func BytesToUIntLittleEndian(b []byte) int {
	// 不知道这种长度是3，7，6的转化是否正确
	if len(b) == 3 || len(b) == 7 {
		b = append([]byte{0}, b...)
	}

	if len(b) == 6 {
		b = append([]byte{0}, b...)
		b = append([]byte{0}, b...)
	}

	bytesBuffer := bytes.NewBuffer(b)
	switch len(b) {
	case 1:
		var tmp uint8
		binary.Read(bytesBuffer, binary.BigEndian, &tmp)
		return int(tmp)
	case 2:
		var tmp uint16
		binary.Read(bytesBuffer, binary.BigEndian, &tmp)
		return int(tmp)
	case 4:
		var tmp uint32
		binary.Read(bytesBuffer, binary.BigEndian, &tmp)
		return int(tmp)

	case 8:
		var tmp uint64
		binary.Read(bytesBuffer, binary.BigEndian, &tmp)
		return int(tmp)
	default:
		return 0
	}
}

func BytesToIntLittleEndian(p *Page, b []byte) int {

	if len(b) == 3 {
		b = append([]byte{0}, b...)
	}
	bytesBuffer := bytes.NewBuffer(b)
	switch len(b) {
	case 1:
		var tmp int8
		binary.Read(bytesBuffer, binary.BigEndian, &tmp)
		return int(tmp)
	case 2:
		var tmp int16
		binary.Read(bytesBuffer, binary.BigEndian, &tmp)
		return int(tmp)
	case 4:
		var tmp int32
		binary.Read(bytesBuffer, binary.BigEndian, &tmp)
		return int(tmp)

	case 8:
		var tmp int64
		binary.Read(bytesBuffer, binary.BigEndian, &tmp)
		return int(tmp)
	default:
		return 0
	}
}

func BytesToBinaryString(bs []byte) string {
	buf := bytes.NewBuffer([]byte{})
	for _, v := range bs {
		buf.WriteString(fmt.Sprintf("%08b", v))
	}
	return buf.String()
}

func IntToBytes(n int) []byte {
	x := int32(n)
	bytesBuffer := bytes.NewBuffer([]byte{})
	binary.Write(bytesBuffer, binary.BigEndian, x)
	return bytesBuffer.Bytes()
}

func UintToBytes(n uint16) []byte {
	x := uint32(n)
	bytesBuffer := bytes.NewBuffer([]byte{})
	binary.Write(bytesBuffer, binary.BigEndian, x)
	return bytesBuffer.Bytes()
}

func FindTwoscomplement(inputString string) (bstring string, isMinus bool) {
	str := []byte(inputString)
	if str[0] == '0' {
		return "[" + string(str) + "]", false
	}
	n := len(str)

	// Traverse the string to get first '1' from
	// the last of string
	var i int
	for i = n - 1; i >= 0; i-- {
		if str[i] == '1' {
			break
		}

	}

	// If there exists no '1' concat 1 at the
	// starting of string
	if i == -1 {
		return "1" + string(str), true
	}

	// Continue traversal after the position of
	// first '1'
	for k := i - 1; k >= 0; k-- {
		//Just flip the values
		if str[k] == '1' {
			// str.replace(k, k+1, "0")
			// str = strings.Replace(str, "1", "0", 1)
			str[k] = '0'
		} else {
			// str.replace(k, k+1, "1")
			//str = strings.Replace(str, "0", "1", 1)
			str[k] = '1'
		}

	}
	str[0] = '1'
	result := "[" + string(str) + "]"
	// return the modified string
	return result, true
}

const (
	zero  = byte('0')
	one   = byte('1')
	lsb   = byte('[') // left square brackets
	rsb   = byte(']') // right square brackets
	space = byte(' ')
)

func init() {
	uint8arr[0] = 128
	uint8arr[1] = 64
	uint8arr[2] = 32
	uint8arr[3] = 16
	uint8arr[4] = 8
	uint8arr[5] = 4
	uint8arr[6] = 2
	uint8arr[7] = 1
}

var uint8arr [8]uint8

// ErrBadStringFormat represents a error of input string's format is illegal .
var ErrBadStringFormat = errors.New("bad string format")

// ErrEmptyString represents a error of empty input string.
var ErrEmptyString = errors.New("empty string")

// regex for delete useless string which is going to be in binary format.
var rbDel = regexp.MustCompile(`[^01]`)

// BinaryStringToBytes get the binary bytes according to the
// input string which is in binary format.
func BinaryStringToBytes(s string) (bs []byte) {
	if len(s) == 0 {
		panic(ErrEmptyString)
	}

	s = rbDel.ReplaceAllString(s, "")
	l := len(s)
	if l == 0 {
		panic(ErrBadStringFormat)
	}

	mo := l % 8
	l /= 8
	if mo != 0 {
		l++
	}
	bs = make([]byte, 0, l)
	mo = 8 - mo
	var n uint8
	for i, b := range []byte(s) {
		m := (i + mo) % 8
		switch b {
		case one:
			n += uint8arr[m]
		}
		if m == 7 {
			bs = append(bs, n)
			n = 0
		}
	}
	return
}

func ParseMySQLTimeStamp(bytes []byte) *TimeStampType {
	ts := NewTimeStampType()
	for _, b := range bytes {
		fmt.Println(byte(b))
	}
	fmt.Println(uint32(bytes[0]) << 24)
	unix := uint32((bytes[3])) + uint32(bytes[2])<<8 + uint32(bytes[1])<<16 + uint32(bytes[0])<<24
	// unix, _ := BytesToUIntLittleEndian1(bytes)

	if unix == 0 {
		ts.value = "0000-00-00 00:00:00"
		return ts
	}

	timeLayout := "2006-01-02 15:04:05"
	timeStr := time.Unix(int64(unix), 0).Format(timeLayout)
	ts.value = timeStr

	// stringTime := "2022-10-19 11:12:55"

	// loc, _ := time.LoadLocation("Local")

	// the_time, err := time.ParseInLocation("2006-01-02 15:04:05", stringTime, loc)

	// if err == nil {

	// 	unix_time := the_time.Unix() //1504082441

	// 	fmt.Println(unix_time)
	// 	bytes = IntToBytes(int(unix_time))
	// 	fmt.Println(bytes)
	// }

	return ts

}

func ParseMySQLDateTime(bytes []byte) *DateTimeType {
	dt := NewDateTimeType()
	//1位
	// var oneBit = int64(0x1)

	var seventenBits = int64(0x1ffff)
	//4位, month
	// var fourBits = int64(0xf)

	//5位, day
	var fiveBits = int64(0x1f)

	//6位, minute/second
	var sixBits = int64(0x3f)

	//24位, microseconds
	var twentyFourBits = int64(0xffffff)

	//8字节的int64
	// a := int64(2277273300067418112)
	a, _ := BytesToUIntLittleEndian1(bytes)

	year_and_month := (int64(a) >> 46) & seventenBits

	year := year_and_month / 13
	month := year_and_month % 13

	day := (int64(a) >> 41) & fiveBits
	hour := (int64(a) >> 36) & sixBits
	minute := (int64(a) >> 30) & sixBits
	second := (int64(a) >> 24) & sixBits
	microseconds := (int64(a)) & twentyFourBits

	dt.year = year
	dt.month = month
	dt.day = day
	dt.hour = hour
	dt.minute = minute
	dt.second = second
	dt.microseconds = microseconds
	// dt.hour = hour
	return dt

}
func ParseMySQLInt(index *Index, bytes []byte) int {
	var b [4]byte
	var v2 = 128
	b[0] = uint8(v2)
	b[1] = uint8(v2 >> 8)
	b[2] = uint8(v2 >> 16)
	b[3] = uint8(v2 >> 24)
	var res = make([]byte, 4)
	for i := 0; i < 4; i++ {
		res[i] = bytes[i] ^ b[i]
	}

	restring := BytesToBinaryString(res)

	resstrComp, isMinus := FindTwoscomplement(restring)

	res2 := BinaryStringToBytes(resstrComp)
	if isMinus {
		for i := 0; i < 4; i++ {
			res2[i] = res2[i] ^ b[i]
		}
	}

	finalRes := BytesToIntLittleEndian(index.Page, res2)
	if isMinus {
		finalRes = -1 * finalRes
	}

	return finalRes
}

func RemoveRepeatedElement(arr []uint64) (newArr []uint64) {
	newArr = make([]uint64, 0)
	for i := 0; i < len(arr); i++ {
		repeat := false
		for j := i + 1; j < len(arr); j++ {
			if arr[i] == arr[j] {
				repeat = true
				break
			}
		}
		if !repeat {
			newArr = append(newArr, arr[i])
		}
	}
	return
}

//获取结构体中字段的名称
// func GetFieldName(columnName string, info FspHdrXdes) uint64 {

// 	var val uint64
// 	t := reflect.TypeOf(info)
// 	if t.Kind() == reflect.Ptr {
// 		t = t.Elem()
// 	}
// 	if t.Kind() != reflect.Struct {
// 		fmt.Println("Check type error not Struct")
// 		return 0
// 	}
// 	fieldNum := t.NumField()
// 	for i := 0; i < fieldNum; i++ {
// 		if strings.ToUpper(t.Field(i).Name) == strings.ToUpper(columnName) {
// 			v := reflect.ValueOf(info)
// 			val := v.FieldByName(t.Field(i).Name).Uint()
// 			return val
// 		}
// 	}
// 	return val
// }

//  字段数组中按字段的position进行排序，这样，就能按顺序进行字段值的获取
type FiledSort []*RecordField

func (s FiledSort) Len() int {

	return len(s)
}
func (s FiledSort) Swap(i, j int) {
	//两个对象满足Less()则位置对换
	//表示执行交换数组中下标为i的数据和下标为j的数据
	s[i], s[j] = s[j], s[i]
}
func (s FiledSort) Less(i, j int) bool {
	//按字段比较大小,此处是降序排序
	//返回数组中下标为i的数据是否小于下标为j的数据
	return s[i].position < s[j].position
}

func BytesToUint32(b []byte) uint32 {
	_ = b[3] // bounds check hint to compiler; see golang.org/issue/14808
	return uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
}
