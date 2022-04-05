package raft

import (
	"log"
	"time"
)

func queue_test() {
	c := make(chan ApplyMsg)
	q := NewQueue(c)
	go func() {
		for i := 0; i < 5; i++ {
			time.Sleep(1 * time.Second)
			q.put(ApplyMsg{
				Command: i,
			})
		}
		for i := 5; i < 10; i++ {
			q.put(ApplyMsg{
				Command: i,
			})

		}
	}()

	for m := range c {
		log.Println(m.Command)
		if m.Command == 9 {
			return
		}
	}
}
