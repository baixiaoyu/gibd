package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

func getData1(f *os.File, pos int64, bsize int64) []byte {
	f.Seek(pos, 0)
	var buffer bytes.Buffer
	io.CopyN(&buffer, f, int64(bsize))
	_bytes := buffer.Bytes()

	return _bytes

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

func mainc() {
	var filePath string = "ibdata1"
	fp, err := os.Open(filePath)
	if err != nil {
		fmt.Println(err)
	}
	defer fp.Close()

	bytes := getData1(fp, 24, 2)
	res, _ := BytesToUIntLittleEndian1(bytes)
	println(res)
}
