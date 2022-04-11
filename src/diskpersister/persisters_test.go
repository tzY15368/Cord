package diskpersister

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"testing"
)

func TestPersister(t *testing.T) {
	var usem = true
	persister := NewPersister("test-out", usem)
	data0 := []byte("hello")
	err := persister.write(&data0)
	if err != nil {
		panic(err)
	}
	data0 = append(data0, []byte("world222")...)

	err = persister.write(&data0)
	if err != nil {
		panic(err)
	}

	data0 = data0[2 : len(data0)-2]

	err = persister.write(&data0)
	if err != nil {
		panic(err)
	}
}

func TestRand(t *testing.T) {
	fmt.Println(genRandomBytes(550, 660))
	buf := new(bytes.Buffer)
	var en uint64 = 999999999999999
	encoder := gob.NewEncoder(buf)
	encoder.Encode(&en)
	fmt.Println(len(buf.Bytes()), buf.Bytes())
}

// 假设：log compaction按50kb为单位进行（即当log大小超过50kb时候打snapshot，snapshot大小暂定500kb）
// 按每次persist写共计约600kb计算，随即上下浮动10%，因此需要随机生成540-660kb的[]byte
func BenchmarkNaive(b *testing.B) {
	persister := NewPersister("bench-out", false)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := persister.writeNaive(genRandomBytes(2500, 4500))
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkMMAP(b *testing.B) {
	persister := NewPersister("bench-out-mmap", true)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := persister.writeMMap(genRandomBytes(540, 660))
		if err != nil {
			panic(err)
		}
	}
}
