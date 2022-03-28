package shardkv

// sendMigrateRPC thread safe, will block
// func (kv *ShardKV) sendMigrateRPC(groupServers []string, args *MigrateArgs, reply *MigrateReply) {
// 	// 没做groupLeader缓存
// 	for {
// 		for _, server := range groupServers {
// 		Retry:
// 			ok := kv.make_end(server).Call("ShardKV.Migrate", args, reply)
// 			if ok && reply.Err == OK {
// 				return
// 			} else if reply.Err == ErrKeyNoLock {
// 				kv.logger.Debug("skv: sendMigrate: error no lock, sleeping 75ms")
// 				time.Sleep(pollCFGInterval)
// 				goto Retry
// 			} else {
// 				kv.logger.WithField("err", reply.Err).Debug("skv: sendMigrateRPC: reply=false")
// 			}
// 		}
// 		kv.logger.WithField("reply", fmt.Sprintf("%+v", reply)).Debug("skv: sendMigrateRPC: round failed, restarting after 100ms")
// 		time.Sleep(100 * time.Millisecond)
// 	}
// }

// func (kv *ShardKV) doMigrate(delta *shardctrler.DiffCfg, newCfg *shardctrler.Config) {
// 	// pulled by others
// 	outboundShards := delta.FromMe(kv.gid)
// 	// pull from others
// 	inboundShards := delta.ToMe(kv.gid)
// 	kv.logger.WithFields(logrus.Fields{
// 		"outbound": outboundShards,
// 		"inbound":  inboundShards,
// 	}).Debug("svCFG: migrate: preparing migrates")
// 	var wg sync.WaitGroup
// 	for shardKey, targetGID := range inboundShards {
// 		args := MigrateArgs{
// 			Shard:     shardKey,
// 			ConfigNum: newCfg.Num,
// 		}
// 		wg.Add(1)
// 		go func(args *MigrateArgs, targetGid int) {
// 			servers := newCfg.Groups[targetGid]
// 			reply := &MigrateReply{Data: make(map[string]string)}

// 			kv.sendMigrateRPC(servers, args, reply)

// 			buf := new(bytes.Buffer)
// 			encoder := labgob.NewEncoder(buf)
// 			err := encoder.Encode(&reply.Data)
// 			if err != nil {
// 				panic(err)
// 			}

// 			op := Op{
// 				OP_TYPE:     OP_MIGRATE,
// 				OP_KEY:      fmt.Sprintf("%d", args.Shard),
// 				OP_VALUE:    buf.String(),
// 				RequestInfo: args.RequestInfo,
// 			}
// 			kv.proposeAndApply(op, reply)
// 			kv.logger.WithField("reply", fmt.Sprintf("%+v", reply)).
// 				Debug("skv: migratePull: result")

// 			kv.logger.WithFields(logrus.Fields{
// 				"shard": args.Shard,
// 				"len":   len(reply.Data),
// 			}).Debug("svCFG: migrate: pull ok")
// 			wg.Done()
// 		}(&args, targetGID)
// 	}
// 	wg.Add(1)
// 	go func() {
// 		for len(outboundShards) != 0 {
// 			doneShardOffset := <-kv.migrateNotify[newCfg.Num]
// 			delete(outboundShards, doneShardOffset)
// 			kv.logger.WithField("shard", doneShardOffset).Debug("svCFG: migrate: pulled by")
// 		}
// 		kv.logger.Debug("svCFG: migrate: pull by others done")
// 		wg.Done()
// 	}()
// 	wg.Wait()
// 	kv.logger.Debug("svCFG: migrate: done doMigrate")
// 	// val := atomic.AddInt32(&kv.nextMigrateIndex, 1)
// 	// kv.logger.WithField("new-nextMigrateIndex", val).Debug("svCFG: migrate: nexMigrateIndex at")
// }
