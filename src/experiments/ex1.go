package main

import (
	"context"
	"fmt"
	"time"
)

var cancel context.CancelFunc

var ch1 chan string

func main() {
	ctx, cc := context.WithCancel(context.TODO())
	cancel = cc
	fmt.Println("helo")
	go task1()
	go task2(ctx)
	go func() {
		for {
			ch1 <- "nothing..."
			time.Sleep(time.Duration(300) * time.Millisecond)
		}
	}()
	select {}
}

func task1() {
	i := 0
	for {
		if i != 5 {
			i = i + 1
			fmt.Println("i=", i)
			time.Sleep(time.Duration(1) * time.Second)
		} else {
			fmt.Println("cancelling.")
			cancel()
			break
		}
	}
}

func task2(ctx context.Context) {
	for {
		done := ctx.Done()
		after := time.After(time.Duration(400) * time.Millisecond)
		select {
		case <-done:
			fmt.Println("^^^^^^^^^^^^^^done")
			return
		case <-after:
			fmt.Println("timeout!!!")
		case v := <-ch1:
			fmt.Println(v)
		}
	}
}
