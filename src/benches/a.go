package benches

import (
	"bytes"
	"encoding/gob"
	"fmt"
)

type mt struct {
	Data string
	Vvv  int
}

func e(d interface{}) interface{} {

	buf := new(bytes.Buffer)
	encoder := gob.NewEncoder(buf)
	err := encoder.Encode(&d)
	if err != nil {
		panic(err)
	}
	b2 := buf.Bytes()
	decoder := gob.NewDecoder(bytes.NewBuffer(b2))
	var b interface{}
	err = decoder.Decode(&b)
	if err != nil {
		panic(err)
	}
	return b
}

func main() {
	gob.Register(mt{})
	m := mt{Data: "asdf", Vvv: 123}
	n := e(m)
	fmt.Println(n)
}
