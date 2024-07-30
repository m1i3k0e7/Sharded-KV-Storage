package shardkv


import "6.5840/labrpc"
import "6.5840/raft"
// import "sync"
import "6.5840/labgob"
import "6.5840/shardctrler"
import "time"
import "bytes"
import "fmt"
import "github.com/sasha-s/go-deadlock"

type Shard struct {
	Num int
	Gid int
	Storage map[string]string
}

type Op struct {
	Type int
	ClerkId int64
	OpId int
	Key string
	Value string
	LatestConfig shardctrler.Config
	ConfigNum int
	ShardId int
	Storage map[string]string
	MaxAppliedId map[int64]int
}

type ShardKV struct {
	mu           deadlock.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	shardDB 	  [shardctrler.NShards]Shard
	maxAppliedId  map[int64]int
	opWaitChans   map[int]chan OpReply
	persister     *raft.Persister
	snapshotIndex int
	sm 		      *shardctrler.Clerk
	config        shardctrler.Config

	s time.Time
	o string
}

func (kv *ShardKV) myLock(o string) {
	kv.mu.Lock()
	kv.s = time.Now()
	kv.o = o
}

func (kv *ShardKV) myUnlock(name string) {
	if time.Since(kv.s) > time.Duration(1*time.Second) {
            fmt.Printf("%v time out %v\n", name, time.Since(kv.s))
	}
	kv.o = ""
	kv.mu.Unlock()
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	key, id, clerk := args.Key, args.RequestId, args.ClerkId
	op := Op{
		Type: GET,
		ClerkId: clerk,
		OpId: id,
		Key: key,
	}

	reply.Err = kv.operateRequest(op).Err
	shardId := key2shard(key)
	if reply.Err == OK {
		kv.myLock("1")
		reply.Value = kv.shardDB[shardId].Storage[key]
		kv.myUnlock("1")
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	key, value, id, clerk := args.Key, args.Value, args.RequestId, args.ClerkId
	opType := PUT
	if args.Op == "Append" {
		opType = APPEND
	}

	if kv.isApplied(clerk, id) {
		reply.Err = OK
		return
	}

	op := Op{
		Type: opType,
		ClerkId: clerk,
		OpId: id,
		Key: key,
		Value: value,
	}

	reply.Err = kv.operateRequest(op).Err
}

func (kv *ShardKV) Add(args *AddArgs, reply *AddReply) {
	op := Op{
		Type: ADD,
		ClerkId: args.ClerkId,
		ConfigNum: args.ConfigNum,
		ShardId: args.ShardId,
		Storage: args.Storage,
		MaxAppliedId: args.MaxAppliedId,
	}
	reply.Err = kv.operateRequest(op).Err
}

func (kv *ShardKV) applier() {
	for applyMsg := range kv.applyCh {
		if applyMsg.CommandValid {
			index := applyMsg.CommandIndex
			op := applyMsg.Command.(Op)
			clerk, id, key, value, opType := op.ClerkId, op.OpId, op.Key, op.Value, op.Type
			shardId := key2shard(key)
			reply := OpReply{
				Err: OK,
				ClerkId: clerk,
				OpId: id,
			}
			
			if opType == GET || opType == PUT || opType == APPEND {
				if _, reply.Err = kv.inShard(shardId); reply.Err == OK {
					if !kv.isApplied(clerk, id) {
						kv.myLock("2")
						kv.maxAppliedId[clerk] = id
						if opType == PUT {
							kv.shardDB[shardId].Storage[key] = value
						} else if opType == APPEND {
							kv.shardDB[shardId].Storage[key] += value
						}
						kv.myUnlock("2")
					}
				}
			} else if opType == ADD {
				kv.myLock("3")
				shard, configNum := op.ShardId, op.ConfigNum
				if configNum == kv.shardDB[shard].Num + 1 && configNum == kv.config.Num + 1 {
					kv.shardDB[shard] = Shard{
						Num: configNum,
						Gid: int(clerk),
						Storage: copyStorage(op.Storage),
					}
					for clerk, id := range op.MaxAppliedId {
						if id2, ok := kv.maxAppliedId[clerk]; id2 < id || !ok {
							kv.maxAppliedId[clerk] = id
						}
					}
				} else if kv.config.Num + 1 < configNum || kv.shardDB[shard].Num + 1 < configNum {
					reply.Err = ErrTimeout
				}
				kv.myUnlock("3")
			} else if opType == UPDATE {
				kv.myLock("4")
				if op.LatestConfig.Num == kv.config.Num + 1 {
					kv.config = op.LatestConfig
				}
				kv.myUnlock("4")
			} else if opType == ADDUP {
				kv.myLock("5")
				shard, configNum := op.ShardId, op.ConfigNum
				// fmt.Printf("(%d, %d) addup %d %d %d\n\n", kv.gid, kv.me, shard, configNum, kv.shardDB[shard].Num)
				if ((configNum == kv.shardDB[shard].Num + 1 || kv.shardDB[shard].Num == -1) && configNum == kv.config.Num + 1) {
					kv.shardDB[shard].Num = configNum
					kv.shardDB[shard].Gid = int(op.ClerkId)
				} else if configNum == -1 {
					kv.shardDB[shard].Num = -1
				} else if configNum > kv.config.Num + 1 || (kv.shardDB[shard].Num + 1 < configNum && kv.shardDB[shard].Num != -1) {
					reply.Err = ErrTimeout
				}
				kv.myUnlock("5")
			}

			kv.myLock("6")
			waitChan, ok := kv.opWaitChans[index]
			kv.myUnlock("6")
			if ok {
				waitChan <- reply
			}
		
			if kv.maxraftstate != -1 && kv.persister.RaftStateSize() > kv.maxraftstate {
				snapshotData := kv.saveSnapshot()
				kv.rf.Snapshot(index, snapshotData)
			}
		} else if applyMsg.SnapshotValid {
			kv.myLock("7")
			kv.readSnapshot(applyMsg.Snapshot)
			kv.snapshotIndex = applyMsg.SnapshotIndex
			kv.myUnlock("7")
		}
	}
}

func (kv *ShardKV) poller() {
	for {
		updated, latestConfig := kv.getConfigUpdate()
		_, isleader := kv.rf.GetState()
		if updated && isleader {
			kv.myLock("8")
			op := Op{
				Type: UPDATE,
				ClerkId: int64(kv.gid),
				OpId: kv.me,
				ConfigNum: kv.config.Num,
				LatestConfig: latestConfig,
			}
			
			kv.operateUpdate(op)

			// fmt.Printf("(%d, %d) after update:\n%v\n\n", kv.gid, kv.me, kv.shardDB)
			updated := true
			for i := 0; i < shardctrler.NShards; i++ {
				if kv.shardDB[i].Num != latestConfig.Num {
					updated = false
					break
				}
			}
			
			if updated {
				updateOp := Op{
					Type: UPDATE,
					ClerkId: int64(kv.gid),
					OpId: kv.me,
					LatestConfig: latestConfig,
				}
				kv.myUnlock("8")
				kv.operateRequest(updateOp)
			} else {
				kv.myUnlock("8")
			}
			
			
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) Kill() {
	kv.rf.Kill()
}

func (kv *ShardKV) operateUpdate(op Op) {
	curConfig := kv.config
	latestConfig := op.LatestConfig
	// fmt.Printf("(%d, %d) start update:\n%v\n%v\n\n", kv.gid, kv.me, curConfig, latestConfig)
	if latestConfig.Num == curConfig.Num + 1 {
		for i := 0; i < shardctrler.NShards; i++ {
			if kv.shardDB[i].Num == latestConfig.Num {
				continue
			}
			if curConfig.Shards[i] == 0 {
				go func(i int) {
					kv.myLock("9")
					addOp := Op{
						Type: ADD,
						ClerkId: int64(latestConfig.Shards[i]),
						ConfigNum: latestConfig.Num,
						ShardId: i,
						Storage: make(map[string]string),
					}
					kv.myUnlock("9")
					for {
						err := kv.operateRequest(addOp).Err
						if err == OK {
							break
						}
						time.Sleep(10 * time.Millisecond)
					}
				}(i)
			} else if curConfig.Shards[i] == kv.gid && latestConfig.Shards[i] != kv.gid && kv.shardDB[i].Num == curConfig.Num {
				go func(i int, latestConfig *shardctrler.Config) {
					kv.myLock("10")
					upOp := Op{
						Type: ADDUP,
						ClerkId: int64(latestConfig.Shards[i]),
						ConfigNum: -1,
						ShardId: i,
					}
					kv.myUnlock("10")
					for {
						err := kv.operateRequest(upOp).Err
						if err == OK {
							break
						}
					}
				}(i, &latestConfig)
			} else if (curConfig.Shards[i] != kv.gid && latestConfig.Shards[i] != kv.gid) || (curConfig.Shards[i] == kv.gid && latestConfig.Shards[i] == kv.gid) {
				upOp := Op{
					Type: ADDUP,
					ClerkId: int64(latestConfig.Shards[i]),
					ConfigNum: latestConfig.Num,
					ShardId: i,
				}
				go kv.operateRequest(upOp)
			} else if curConfig.Shards[i] == kv.gid && latestConfig.Shards[i] != kv.gid && kv.shardDB[i].Num == -1 {
				go func(i int, latestConfig *shardctrler.Config) {
					kv.myLock("11")
					servers := latestConfig.Groups[latestConfig.Shards[i]]
					maxAppliedId := make(map[int64]int)
					for clerk, id := range kv.maxAppliedId {
						maxAppliedId[clerk] = id
					}
					args := AddArgs{
						ClerkId: int64(latestConfig.Shards[i]),
						ConfigNum: latestConfig.Num,
						ShardId: i,
						Storage: copyStorage(kv.shardDB[i].Storage),
						MaxAppliedId: maxAppliedId,
					}
					kv.myUnlock("11")
					for {
						for si := 0; si < len(servers); si++ {
							srv := kv.make_end(servers[si])
							reply := AddReply{}
							ok := srv.Call("ShardKV.Add", &args, &reply)
							if ok && reply.Err == OK {
								kv.myLock("12")
								upOp := Op{
									Type: ADDUP,
									ClerkId: int64(latestConfig.Shards[i]),
									ConfigNum: latestConfig.Num,
									ShardId: i,
								}
								kv.myUnlock("12")
								for {
									err := kv.operateRequest(upOp).Err
									if err == OK {
										break
									}
								}
								return
							}
						}
					}
				}(i, &latestConfig)
			}
		}
	}
}

func (kv *ShardKV) operateRequest(op Op) OpReply {
	index, isleader := kv.proposeOp(op)
	if !isleader {
		return OpReply{
			Err: ErrWrongLeader,
		}
	}

	opType, clerk, id := op.Type, op.ClerkId, op.OpId
	if opType == GET || opType == PUT || opType == APPEND {
		shardId := key2shard(op.Key)
		if in, err := kv.inShard(shardId); !in{
			return OpReply{
				Err: err,
			}
		}
	}

	waitChan := kv.createWaitChan(index)
	defer kv.deleteWaitChan(index)
	select {
		case reply := <-waitChan:
			if reply.ClerkId != clerk || reply.OpId != id {
				return OpReply{
					Err: ErrWrongLeader,
				}
			} else {
				return reply
			}
		case <-time.After(300 * time.Millisecond):
			return OpReply{
				Err: ErrTimeout,
			}
	}
}

func (kv *ShardKV) inShard(shardId int) (bool, Err) {
	kv.myLock("13")
	defer kv.myUnlock("13")
	if kv.config.Num == kv.shardDB[shardId].Num {
		if kv.config.Shards[shardId] != kv.gid || kv.shardDB[shardId].Storage == nil {
			return false, ErrWrongGroup
		} else {
			return true, OK
		}
	} else {
		return false, ErrStaleData
	}
}

func (kv *ShardKV) proposeOp(op Op) (int, bool) {
	index, _, isleader := kv.rf.Start(op)
	return index, isleader
}

func (kv *ShardKV) getConfigUpdate() (bool, shardctrler.Config) {
	kv.myLock("14")
	targetConfigNum := kv.config.Num + 1
	kv.myUnlock("14")
	latestConfig := kv.sm.Query(targetConfigNum)
	return (latestConfig.Num == targetConfigNum), latestConfig
}

func (kv *ShardKV) createWaitChan(index int) chan OpReply {
	kv.myLock("15")
	defer kv.myUnlock("15")
	kv.opWaitChans[index] = make(chan OpReply, 1)
	return kv.opWaitChans[index]
}

func (kv *ShardKV) deleteWaitChan(index int) {
	kv.myLock("16")
	defer kv.myUnlock("16")
	delete(kv.opWaitChans, index)
}

func (kv *ShardKV) isApplied(clerk int64, id int) bool {
	kv.myLock("17")
	defer kv.myUnlock("17")
	return id <= kv.maxAppliedId[clerk]
}

func copyStorage(m map[string]string) map[string]string {
	newStorage := make(map[string]string)
	for k, v := range m {
		newStorage[k] = v
	}
	return newStorage
}

func (kv *ShardKV) readSnapshot(snapshotData []byte) {
	if snapshotData == nil || len(snapshotData) == 0 {
		return
	}
	r := bytes.NewBuffer(snapshotData)
	d := labgob.NewDecoder(r)

	var shardDB [shardctrler.NShards]Shard
	var maxAppliedId map[int64]int
	var config shardctrler.Config
	var maxraftstate int
	if d.Decode(&shardDB) == nil && d.Decode(&maxAppliedId) == nil && d.Decode(&config) == nil && d.Decode(&maxraftstate) == nil {
		kv.shardDB = shardDB
		kv.maxAppliedId = maxAppliedId
		kv.config = config
		kv.maxraftstate = maxraftstate
	} else {
		fmt.Printf("[Server(%v)] failed to decode snapshot", kv.me)
	}
}

func (kv *ShardKV) saveSnapshot() []byte {
	kv.myLock("18")
	defer kv.myUnlock("18")
	// tmpShardDB := [shardctrler.NShards]Shard{}
	// for i := 0; i < shardctrler.NShards; i++ {
	// 	tmpShardDB[i] = Shard{
	// 		Num: kv.shardDB[i].Num,
	// 		Gid: kv.shardDB[i].Gid,
	// 		Storage: make(map[string]string),
	// 	}
	// 	for k, v := range kv.shardDB[i].Storage {
	// 		tmpShardDB[i].Storage[k] = v
	// 	}
	// }

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.shardDB)
	e.Encode(kv.maxAppliedId)
	e.Encode(kv.config)
	e.Encode(kv.maxraftstate)
	data := w.Bytes()
	return data
}
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.shardDB = [shardctrler.NShards]Shard{}
	kv.maxAppliedId = make(map[int64]int)
	kv.opWaitChans = make(map[int]chan OpReply)
	kv.persister = persister
	
	kv.sm = shardctrler.MakeClerk(ctrlers)
	kv.readSnapshot(kv.persister.ReadSnapshot())

	go kv.applier()
	go kv.poller()

	return kv
}
