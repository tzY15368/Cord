package diskpersister

import (
	"testing"
)

func BenchmarkMMAPP(b *testing.B) {
	mp := NewMMapPersister("mmap-bench-out-rf", "mmap-bench-out-ss", 5000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 1; j++ {
			mp.SaveStateAndSnapshot(*genRandomBytes(2500, 4500), *genRandomBytes(500, 5000))
		}
	}
}

func TestMMapPersister(t *testing.T) {
	// 5kb的log
	mp := NewMMapPersister("mmap-rf-out", "mmap-ss-out", 5000)
	s := "I'm writing a bulk ID3 tag editor in C."
	data1 := []byte(s)

	mp.SaveStateAndSnapshot(data1, data1)

	state := mp.ReadRaftState()
	snaps := mp.SnapshotSize()
	if string(state) != string(data1) {
		panic("bad state")
	}
	if snaps != len(data1) {
		panic("Bad snap")
	}
	s += "ID3 tags are usually at the beginning of an mp3 encoded file/"
	data2 := []byte(s)

	mp.SaveStateAndSnapshot(data2, data2)

	state = mp.ReadRaftState()
	snaps = mp.SnapshotSize()
	if string(state) != string(data2) {
		panic("bad state")
	}
	if snaps != len(data2) {
		panic("Bad snap")
	}

}
