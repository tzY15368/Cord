package diskpersister

import (
	"fmt"
	"testing"
)

func BenchmarkMMAPP(b *testing.B) {
	mp := NewMMapPersister("mmap-bench-out-rf2", "mmap-bench-out-ss2", 5000)
	defer mp.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		state := genRandomBytes(2500, 4500)
		snapshot := genRandomBytes(500, 5000)
		_ = state
		_ = snapshot
		mp.SaveStateAndSnapshot(*state, *snapshot)
		//fmt.Println("---")
	}
}

func Test2(t *testing.T) {
	mp := NewMMapPersister("mmap-rf-out", "mmap-ss-out-big", 5000)
	mp.Close()
}

func TestMMapPersister(t *testing.T) {
	// 5kb的log
	mp := NewMMapPersister("mmap-rf-out", "mmap-ss-out", 5000)
	s := "I'm writing a bulk ID3 tag editor in C."
	data1 := []byte(s)

	mp.SaveStateAndSnapshot(data1, data1)

	state := mp.ReadRaftState()
	snap := mp.ReadSnapshot()
	snaps := mp.SnapshotSize()
	if string(state) != string(data1) {
		panic("bad state")
	}
	if snaps != len(data1) || string(snap) != string(data1) {
		fmt.Println(string(snap))
		fmt.Println(string(data1))
		panic("Bad snap")
	}
	mp.Close()
	mp = NewMMapPersister("mmap-rf-out", "mmap-ss-out", 5000)
	snap = mp.ReadSnapshot()
	if string(snap) != string(data1) {
		fmt.Println(string(snap))
		panic("bad snap2")
	}
	return
	s += "ID3 tags are usually at the beginning of an mp3 encoded file/"
	data2 := []byte(s)
	s3 := s + "snapshot"
	data3 := []byte(s3)
	mp.SaveStateAndSnapshot(data2, data3)

	state = mp.ReadRaftState()
	snap = mp.ReadSnapshot()
	snaps = mp.SnapshotSize()
	if string(state) != string(data2) {
		panic("bad state")
	}
	if snaps != len(data3) || string(snap) != string(data3) {
		panic("Bad snap")
	}
}
