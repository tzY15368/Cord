package diskpersister

import (
	"bytes"
	"encoding/gob"
	"os"
	"sync"

	"github.com/edsrzf/mmap-go"
)

type MMapPersister struct {
	file *os.File
	mem  mmap.MMap
	rwmu sync.RWMutex
}

func NewMMapPersister(fname string) *MMapPersister {
	file, err := os.OpenFile(fname, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		panic(err)
	}
	mem, err := mmap.Map(file, mmap.RDWR, 0)
	if err != nil {
		panic(err)
	}
	return &MMapPersister{
		file: file,
		mem:  mem,
	}
}

func (mp *MMapPersister) Close() {
	mp.rwmu.Lock()
	defer mp.rwmu.Unlock()
	mp.mem.Unmap()
	mp.file.Close()
}

func (mp *MMapPersister) write(data *[]byte) error {
	var err error
	buf := new(bytes.Buffer)
	encoder := gob.NewEncoder(buf)
	lenOfData := len(*data)
	err = encoder.Encode(lenOfData)
	if err != nil {
		return err
	}
	var lenOfLen uint8 = uint8(len(buf.Bytes()))
	actualLen := lenOfData + len(buf.Bytes()) + 1
	mp.rwmu.Lock()
	defer mp.rwmu.Unlock()
	if len(mp.mem) < actualLen {
		err = mp.mem.Unmap()
		if err != nil {
			return err
		}
		err = mp.file.Truncate(int64(actualLen))
		if err != nil {
			return err
		}
		_, err = mp.file.Seek(0, 0)
		if err != nil {
			return err
		}
		mp.mem, err = mmap.Map(mp.file, mmap.RDWR, 0)
		if err != nil {
			return err
		}
	}
	n := copy(mp.mem, *data)
	if n != len(*data) {
		panic("wrong len")
	}

	var endBytes []byte
	oldLen := buf.Len()
	buf.Write([]byte{lenOfLen})
	if buf.Len() != oldLen+1 {
		panic("invalid len")
	}
	endBytes = buf.Bytes()
	n = copy(mp.mem[len(mp.mem)-len(endBytes):], endBytes)
	if n != len(endBytes) || len(endBytes)+len(*data) < len(mp.mem) {
		panic("bad write")
	}
	return nil
}

func (mp *MMapPersister) read() (*[]byte, error) {
	mp.rwmu.Lock()
	defer mp.rwmu.Unlock()
	lenOfLen := uint8(mp.mem[len(mp.mem)-1])
	lenOfDataBytes := bytes.NewBuffer(mp.mem[len(mp.mem)-1-int(lenOfLen) : len(mp.mem)-2])
	var lenOfData int
	decoder := gob.NewDecoder(lenOfDataBytes)
	err := decoder.Decode(&lenOfData)
	if err != nil {
		return nil, err
	}

	result := make([]byte, lenOfData)
	n := copy(result, mp.mem)
	if n != lenOfData {
		panic("bad read")
	}
	return &result, nil
}
