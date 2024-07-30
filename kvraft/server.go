package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
	"bytes"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type int
	ClerkId int64
	OpId int
	Key string
	Value string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	// Your definitions here.
	maxAppliedId map[int64]int
	db map[string]string
	opWaitChans map[int]chan Op
	persister *raft.Persister
	snapshotIndex int
}

func (kv *KVServer) proposeOp(op Op) (int, bool) {
	index, _, isleader := kv.rf.Start(op)
	return index, isleader
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	clerk, id, key := args.ClerkId, args.OpId, args.Key
	op := Op{
		Type: GET,
		ClerkId: clerk,
		OpId: id,
		Key: key,
	}

	index, isleader := kv.proposeOp(op)
	if !isleader {
		reply.Err = ErrWrongLeader
		return
	}
	waitChan := kv.createWaitChan(index)
	defer func() {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		delete(kv.opWaitChans, index)
	}()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	select {
		case appliedOp := <-waitChan:
			if appliedOp.ClerkId != clerk || appliedOp.OpId != id {
				reply.Err = ErrWrongLeader
			} else {
				kv.mu.Lock()
				v, ok := kv.db[key]
				kv.mu.Unlock()
				if !ok {
					reply.Err = ErrNoKey
				} else {
					reply.Err = OK
					reply.Value = v
				}
			}
			return
		case <-ticker.C:
			reply.Err = ErrTimeout
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	clerk, id, key, value := args.ClerkId, args.OpId, args.Key, args.Value
	if kv.isApplied(clerk, id) {
		reply.Err = OK
		return
	}

	op := Op{
		Type: PUT,
		ClerkId: clerk,
		OpId: id,
		Key: key,
		Value: value,
	}

	index, isleader := kv.proposeOp(op)
	if !isleader {
		reply.Err = ErrWrongLeader
		return
	}

	waitChan := kv.createWaitChan(index)
	defer func() {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		delete(kv.opWaitChans, index)
	}()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	select {
		case appliedOp := <-waitChan:
			if appliedOp.ClerkId != clerk || appliedOp.OpId != id {
				reply.Err = ErrWrongLeader
			} else {
				reply.Err = OK
			}
		case <-ticker.C:
			reply.Err = ErrTimeout
	}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	clerk, id, key, value := args.ClerkId, args.OpId, args.Key, args.Value
	if kv.isApplied(clerk, id) {
		reply.Err = OK
		return
	}

	op := Op{
		Type: APPEND,
		ClerkId: clerk,
		OpId: id,
		Key: key,
		Value: value,
	}

	index, isleader := kv.proposeOp(op)
	if !isleader {
		reply.Err = ErrWrongLeader
		return
	}

	waitChan := kv.createWaitChan(index)
	defer func() {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		delete(kv.opWaitChans, index)
	}()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	select {
		case appliedOp := <-waitChan:
			if appliedOp.ClerkId != clerk || appliedOp.OpId != id {
				reply.Err = ErrWrongLeader
			} else {
				reply.Err = OK
			}
		case <-ticker.C:
			reply.Err = ErrTimeout
	}
}

func (kv *KVServer) applier() {
	for !kv.killed() {
		applyMsg := <-kv.applyCh
		if applyMsg.CommandValid {
			index := applyMsg.CommandIndex
			if index <= kv.snapshotIndex {
				return 
			}

			op := applyMsg.Command.(Op)
			clerk, id, key, value, opType := op.ClerkId, op.OpId, op.Key, op.Value, op.Type
			if !kv.isApplied(clerk, id) {
				kv.mu.Lock()
				if opType == PUT {
					kv.db[key] = value
				} else if op.Type == APPEND {
					kv.db[key] += value
				}
				kv.maxAppliedId[clerk] = id
				kv.mu.Unlock()
			}	

			if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= kv.maxraftstate {
				snapshotData := kv.saveSnapshot()
				kv.rf.Snapshot(index, snapshotData)
			}

			kv.mu.Lock()
			opChan, ok := kv.opWaitChans[index]
			kv.mu.Unlock()
			if ok {
				opChan <- op
			}
		} else if applyMsg.SnapshotValid {
			kv.mu.Lock()
			kv.readSnapshot(applyMsg.Snapshot)
			kv.snapshotIndex = applyMsg.SnapshotIndex
			kv.mu.Unlock()
		}
	}
}

func (kv *KVServer) createWaitChan(index int) chan Op {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.opWaitChans[index] = make(chan Op, 1)
	return kv.opWaitChans[index]
}

func (kv *KVServer) isApplied(clerk int64, id int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return id <= kv.maxAppliedId[clerk]
}

func (kv *KVServer) snapshoter() {
	for !kv.killed() {
		if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= kv.maxraftstate {
			index := kv.rf.GetLastLogIndex() + 1
			snapshotData := kv.saveSnapshot()
			kv.rf.Snapshot(index, snapshotData)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (kv *KVServer) saveSnapshot() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.db)
	e.Encode(kv.maxAppliedId)
	data := w.Bytes()
	return data
}

func (kv *KVServer) readSnapshot(snapshotData []byte) {
	if snapshotData == nil || len(snapshotData) == 0 {
		return
	}
	r := bytes.NewBuffer(snapshotData)
	d := labgob.NewDecoder(r)

	var db map[string]string
	var maxAppliedId map[int64]int

	if d.Decode(&db) == nil && d.Decode(&maxAppliedId) == nil {
		kv.db = db
		kv.maxAppliedId = maxAppliedId
	} else {
		log.Printf("[Server(%v)] failed to decode snapshot", kv.me)
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.maxAppliedId = make(map[int64]int)
	kv.db = make(map[string]string)
	kv.opWaitChans = make(map[int]chan Op)
	kv.persister = persister
	
	kv.readSnapshot(kv.persister.ReadSnapshot())

	go kv.applier()
	// go kv.snapshoter()

	return kv
}
