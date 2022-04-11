package diskpersister

import (
	"os"

	"github.com/edsrzf/mmap-go"
)

type DiskPersister struct {
	file *os.File
	mem  mmap.MMap
	usem bool
}

func NewPersister(fname string, usem bool) *DiskPersister {
	file, err := os.OpenFile(fname, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		panic(err)
	}
	d := &DiskPersister{
		file: file,
		usem: usem,
	}
	if usem {
		d.mem, err = mmap.Map(file, mmap.RDWR, 0)
		if err != nil {
			panic(err)
		}
	}
	return d
}

func (mp *DiskPersister) write(data *[]byte) error {
	if mp.usem {
		return mp.writeMMap(data)
	} else {
		return mp.writeNaive(data)
	}
}

func (mp *DiskPersister) writeNaive(data *[]byte) error {
	_, err := mp.file.Seek(0, 0)
	if err != nil {
		return err
	}
	l, err := mp.file.Write(*data)
	if err != nil {
		return err
	}
	err = mp.file.Truncate(int64(l))
	return err
}

// 如果现有mmap空间不够大，重新truncate文件并重新map，
// 如果待写的数据比有的mmap空间小，则后面的需要忽略！
// 在map出来的[]byte的最后12位写实际长度？(gob encode)
func (mp *DiskPersister) writeMMap(data *[]byte) error {
	var err error
	if len(mp.mem) < len(*data) {
		err = mp.mem.Unmap()
		if err != nil {
			return err
		}
		err = mp.file.Truncate(int64(len(*data)))
		if err != nil {
			return err
		}
		mp.file.Seek(0, 0)

		mp.mem, err = mmap.Map(mp.file, mmap.RDWR, 0)
		if err != nil {
			return err
		}
	}
	copy(mp.mem, *data)
	return nil
}
