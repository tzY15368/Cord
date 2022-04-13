package raft

import (
	"fmt"
	"time"

	"6.824/common"
	"6.824/proto"
	"github.com/sirupsen/logrus"
)

func (rf *Raft) AppendEntries(args *proto.AppendEntriesArgs, reply *proto.AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.Success = false

	if args.Term < rf.pState.CurrentTerm {
		// reject requests with stale term number
		reply.Term = int64(rf.pState.CurrentTerm)
		reply.NextTryIndex = int64(rf.getLastLogIndex() + 1)
		return
	}

	// 比如： 一个实例断开了，自己进行两次失败选举，term=3，连回来之后另外两个先变回follower重新选
	if args.Term > rf.pState.CurrentTerm {
		// become follower and update current term
		rf.state = STATE_FOLLOWER
		rf.pState.CurrentTerm = args.Term
		rf.pState.VotedFor = -1
		rf.logger.WithField("newTerm", rf.pState.CurrentTerm).Info("appendEntry: became follower due to higher client term")
		rf.dumpLog()
	}

	// confirm heartbeat to refresh timeout
	rf.chanHeartbeat <- true

	reply.Term = int64(rf.pState.CurrentTerm)

	// optimization
	if int(args.PrevLogIndex) > rf.getLastLogIndex() {
		reply.NextTryIndex = int64(rf.getLastLogIndex() + 1)
		return
	}

	// 待优化
	baseIndex := rf.getBaseLogIndex()
	// baseIndex := rf.pState.Log[0].Index
	rf.leaderID = int(args.LeaderID)
	if int(args.PrevLogIndex) >= baseIndex && int(args.PrevLogTerm) != rf.getLogTermAtOffset(int(args.PrevLogIndex)-baseIndex) {
		// if entry log[prevLogIndex] conflicts with new one, there may be conflict entries before.
		// we can bypass all entries during the problematic term to speed up.
		rf.logger.WithField("args", fmt.Sprintf("%+v", args)).WithField("logterm", rf.getLogTermAtOffset(int(args.PrevLogIndex)-baseIndex)).Info("cond failed")
		term := rf.getLogTermAtOffset(int(args.PrevLogIndex) - baseIndex)
		for i := int(args.PrevLogIndex) - 1; i >= baseIndex; i-- {
			if rf.getLogTermAtOffset(i-baseIndex) != term {
				reply.NextTryIndex = int64(i + 1)
				break
			}
		}
	} else if int(args.PrevLogIndex) >= baseIndex-1 {
		// otherwise log up to prevLogIndex are safe.
		// we can merge lcoal log and entries from leader, and apply log if commitIndex changes.
		if len(rf.pState.Log) > 0 {
			rightMargin := int(args.PrevLogIndex) - baseIndex + 1
			if rightMargin > len(rf.pState.Log) {
				rf.logger.WithFields(logrus.Fields{
					"rightMargin": rightMargin,
					"lenLog":      len(rf.pState.Log),
				}).Panic("appendEntry: invalid right margin")
			}
			rf.pState.Log = rf.pState.Log[:rightMargin]
		} else {
			// 之前是个snapshot，baseindex应该完全等于args.prevlogindex
			rf.logger.WithFields(logrus.Fields{
				"baseIndex":         baseIndex,
				"args.PrevLogIndex": args.PrevLogIndex,
			}).Warn("appendEntry: was snapshot")
		}

		rf.pState.Log = append(rf.pState.Log, args.Entries...)
		reply.Success = true
		reply.NextTryIndex = int64(int(args.PrevLogIndex) + len(args.Entries))
		rf.persist()

		if int(args.LeaderCommit) > rf.commitIndex {
			if rf.getLastLogIndex() < int(args.LeaderCommit) {
				rf.commitIndex = rf.getLastLogIndex()
			} else {
				rf.commitIndex = int(args.LeaderCommit)
			}
			//go rf.commitLog()
			rf.dumpLog()
			rf.unsafeCommitLog()
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *proto.AppendEntriesArgs, reply *proto.AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	defer rf.panicHandler()
	if !ok || rf.state != STATE_LEADER || args.Term != rf.pState.CurrentTerm {
		return ok
	}
	if reply.Term > rf.pState.CurrentTerm {
		rf.pState.CurrentTerm = reply.Term
		rf.state = STATE_FOLLOWER
		rf.pState.VotedFor = -1
		//rf.persist()
		rf.logger.WithField("newTerm", rf.pState.CurrentTerm).
			Info("appendEntry: leader became follower due to higher client term")
		return ok
	}
	if reply.Success {
		if len(args.Entries) > 0 {
			rf.nextIndex[server] = int(args.Entries[len(args.Entries)-1].Index + 1)
			rf.matchIndex[server] = rf.nextIndex[server] - 1
		}
	} else {
		rf.nextIndex[server] = common.Min(int(reply.NextTryIndex), rf.getLastLogIndex())
	}

	baseIndex := rf.getBaseLogIndex()
	// baseIndex := rf.pState.Log[0].Index

	for N := rf.getLastLogIndex(); N > rf.commitIndex && rf.pState.Log[N-baseIndex].Term == int64(rf.pState.CurrentTerm); N-- {
		// find if there exists an N to update commitIndex
		count := 1

		if rf.pState.Log[N-baseIndex].Term == int64(rf.pState.CurrentTerm) {
			for i := range rf.peers {
				if i != rf.me && rf.matchIndex[i] >= N {
					count++
				}
			}
		}

		if count > len(rf.peers)/2 {
			if rf.commitIndex < N {
				rf.commitIndex = N
				//go rf.commitLog()
				rf.unsafeCommitLog()
			}
			break
		}
	}

	return ok
}

func (rf *Raft) broadcastAppendEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.panicHandler()
	if rf.state != STATE_LEADER {
		rf.logger.Warn("broadcast: state changed, cancel broadcast")
	}
	baseIndex := rf.getBaseLogIndex()
	// baseIndex := rf.pState.Log[0].Index

	for server := range rf.peers {
		if server != rf.me && rf.state == STATE_LEADER {
			args := &proto.AppendEntriesArgs{
				Term:         int64(rf.pState.CurrentTerm),
				LeaderID:     int64(rf.me),
				PrevLogIndex: int64(rf.nextIndex[server] - 1),
			}

			if int(args.PrevLogIndex) >= baseIndex {
				args.PrevLogTerm = rf.pState.Log[int(args.PrevLogIndex)-baseIndex].Term
			} else {
				args.PrevLogTerm = int64(rf.pState.LastIncludedTerm)
			}
			lastLogIndex := rf.getLastLogIndex()
			if rf.nextIndex[server] <= lastLogIndex {
				rf.logger.WithFields(logrus.Fields{
					fmt.Sprintf("nextindex[%d]", server): rf.nextIndex[server],
					"lastLogIndex":                       lastLogIndex,
					"baseIndex":                          baseIndex,
				}).Debug("broadcast: entries diff:")
				if rf.nextIndex[server] < baseIndex {
					if baseIndex > rf.commitIndex+1 {
						rf.logger.WithFields(logrus.Fields{
							"commitindex": rf.commitIndex,
							"log":         rf.pState.Log,
							"baseIndex":   baseIndex,
						}).Panic("broadcast: baseindex > commitindex, log is lost")
					}
					// 这里如果阻塞会有问题，如果snapshot很大，用时很长，可能会心跳超时
					// 因此用发空的心跳的方式维持leader状态，下次重试
					snapshotArgs := &InstallSnapshotArgs{
						Term:              int(rf.pState.CurrentTerm),
						LeaderID:          rf.me,
						LastIncludedIndex: int(rf.pState.LastIncludedIndex),
						LastIncludedTerm:  int(rf.pState.LastIncludedTerm),
						Offset:            -1,
						Data:              rf.snapshot,
						Done:              true,
					}

					go rf.sendInstallSnapshot(server, snapshotArgs, &InstallSnapshotReply{})
					//args.ExpectSnapshot = true
					rf.logger.WithFields(logrus.Fields{
						"at":                                 time.Now(),
						fmt.Sprintf("nextIndex[%d]", server): rf.nextIndex[server],
						"baseIndex":                          baseIndex,
						"lastIncludedIndex":                  rf.pState.LastIncludedIndex,
					}).Warn(fmt.Sprintf("broadcast: nextindex[%d] < baseIndex, sending snapshot instead", server))
					return
				} else {
					args.Entries = rf.pState.Log[rf.nextIndex[server]-baseIndex:]
				}
			}
			args.LeaderCommit = int64(rf.commitIndex)

			go rf.sendAppendEntries(server, args, &proto.AppendEntriesReply{})
		}
	}
}
