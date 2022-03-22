package common

import (
	"bytes"
	"fmt"

	"6.824/labgob"
)

func main() {
	d1 := make(map[string]string)
	d2 := make(map[int]int)
	d1["asd"] = "sdf"
	d1["bbb"] = "aaa"
	d2[2] = 3
	d2[3] = 2
	fmt.Println(d1, d2)
	buf := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buf)
	err := encoder.Encode(d1)
	if err != nil {
		panic(err)
	}
	err = encoder.Encode(d2)
	if err != nil {
		panic(err)
	}
	fmt.Println(buf.Bytes())

	decoder := labgob.NewDecoder(bytes.NewBuffer(buf.Bytes()))
	var d3 map[string]string
	var d4 map[int]int
	decoder.Decode(&d3)
	decoder.Decode(&d4)
	fmt.Println(d3)
	fmt.Println(d4)
}
