package kvraft

import (
	"log"
	"sync"
	"testing"
	"time"

	"6.824/raft"
	"github.com/sirupsen/logrus"
)

func TestNewApplyHandler(t *testing.T) {
	applyCh := make(chan raft.ApplyMsg)
	go func() {
		applyCh <- raft.ApplyMsg{CommandValid: true, CommandIndex: 1}
		time.Sleep(1 * time.Second)
		applyCh <- raft.ApplyMsg{CommandValid: true, CommandIndex: 2}
		applyCh <- raft.ApplyMsg{CommandValid: true, CommandIndex: 2}
		applyCh <- raft.ApplyMsg{CommandValid: true, CommandIndex: 1}
		applyCh <- raft.ApplyMsg{CommandValid: true, CommandIndex: 3}
		time.Sleep(1 * time.Second)
		applyCh <- raft.ApplyMsg{CommandValid: true, CommandIndex: 3}
	}()

	t.Run("basic test", func(t *testing.T) {
		log.Println("started", time.Now())
		ah := NewApplyHandler(applyCh, 3, logrus.New(), 666)
		_ = ah
		var wg = new(sync.WaitGroup)
		for i := 1; i <= 3; i++ {
			wg.Add(1)
			go func(id int) {
				log.Println("started", id, time.Now())
				ok := ah.waitForMajorityOnIndex(id)
				//ok := true
				//time.Sleep(3 * time.Second)
				log.Println("end", id, time.Now(), ok)
				wg.Done()
			}(i)
		}
		wg.Wait()
	})
}
