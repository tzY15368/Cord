package common

import "fmt"

type b struct {
	a []string
}

func m2() {
	b1 := b{
		a: []string{"asdf", "jk"},
	}

	b2 := b{
		a: b1.a,
	}

	b2.a = append(b2.a, "bbb")
	fmt.Println(b1)
	fmt.Println(b2)
}
