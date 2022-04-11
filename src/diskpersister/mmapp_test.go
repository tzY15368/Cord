package diskpersister

import (
	"fmt"
	"testing"
)

func TestMMapPersister(t *testing.T) {
	mp := NewMMapPersister("mmap-out")
	var err error
	s := "I'm writing a bulk ID3 tag editor in C."
	data1 := []byte(s)
	err = mp.write(&data1)
	if err != nil {
		t.Fatal(err)
	}
	var data2 *[]byte
	data2, err = mp.read()
	if err != nil {
		t.Fatal(err)
	}
	if string(*data2) != string(data1) {
		fmt.Println(data1)
		fmt.Println(*data2)
		t.Fatal("no match?")
	}

	s += "ID3 tags are usually at the beginning of an mp3 encoded file/"
	data3 := []byte(s)
	err = mp.write(&data3)
	if err != nil {
		t.Fatal(err)
	}

	var data4 *[]byte
	data4, err = mp.read()
	if err != nil {
		t.Fatal(err)
	}
	if string(data3) != string(*data4) {
		fmt.Println(data3)
		fmt.Println(*data4)
		t.Fatal("no match 2")
	}
}
