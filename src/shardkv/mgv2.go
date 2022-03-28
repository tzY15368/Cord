package shardkv

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"

	"6.824/labgob"
	"6.824/shardctrler"
	"github.com/sirupsen/logrus"
)

func (kv *ShardKV) doMigrate2(delta *shardctrler.DiffCfg, newCfg *shardctrler.Config) {
	inboundShards := delta.ToMe(kv.gid)
	var wg sync.WaitGroup
	for shardKey, targetGID := range inboundShards {
		swapped := atomic.CompareAndSwapInt32(&kv.shardLocks[shardKey], 0, 1)
		if !swapped {
			kv.logger.Panic("lock was already held", shardKey)
		}
		kv.logger.WithFields(logrus.Fields{
			"inbound": inboundShards,
		}).Debug("svCFG: migrate: preparing migrates")
		args := MigrateArgs{
			Shard:     shardKey,
			ConfigNum: newCfg.Num,
		}
		wg.Add(1)
		go func(args *MigrateArgs, gid int) {
			servers := newCfg.Groups[gid]
			reply := &MigrateReply{Data: make(map[string]string)}
			kv.sendMigrateRPC(servers, args, reply)

			buf := new(bytes.Buffer)
			encoder := labgob.NewEncoder(buf)
			err := encoder.Encode(&reply.Data)
			if err != nil {
				panic(err)
			}

			op := Op{
				OP_TYPE:     OP_MIGRATE,
				OP_KEY:      fmt.Sprintf("%d", args.Shard),
				OP_VALUE:    buf.String(),
				RequestInfo: args.RequestInfo,
			}
			kv.proposeAndApply(op, reply)
			kv.logger.WithField("reply", fmt.Sprintf("%+v", reply)).
				Debug("skv: migratePull: result")

			kv.logger.WithFields(logrus.Fields{
				"shard": args.Shard,
				"len":   len(reply.Data),
			}).Debug("svCFG: migrate: pull ok")
		}(&args, targetGID)
	}
}

/*
	！！核心是不丢数据
	v2: 直接发migrateRPC向对方group pull数据，
	1- 本地shardkey加锁
	2- 发出migraterpc
	3- 对方shardkey加锁
	4- 对方填充reply。data，
	5- 对方解锁shardkey
	5- 返回后本地导入数据，广播
	6- 广播后给对方发ack，删shardkey
	7- ack成功，本地解锁shardkey

*/
