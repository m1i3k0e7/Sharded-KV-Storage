package kvsrv

import (
	"log"
	"sync"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


type KVServer struct {
	mu sync.Mutex
	// Your definitions here.
	db map[string]string
	history map[int64]map[int]string
	maxAppliedOp map[int64]int
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Value = kv.db[args.Key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	key, value, clerk, id := args.Key, args.Value, args.ClerkId, args.OpId
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.maxAppliedOp[clerk] != id {
		if kv.maxAppliedOp[clerk] < id {
			reply.Status = ERR
		}
		return
	}
	kv.maxAppliedOp[clerk] = id + 1
	kv.db[key] = value
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	key, value, clerk, id := args.Key, args.Value, args.ClerkId, args.OpId
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if args.Type == REPORT {
		delete(kv.history[clerk], id)
		return
	}
	if kv.maxAppliedOp[clerk] != id {
		if kv.maxAppliedOp[clerk] > id {
			reply.Value = kv.history[clerk][id]
		} else {
			reply.Status = ERR
		}
		return
	}
	reply.Value = kv.db[key]
	if _, ok := kv.history[clerk]; !ok {
		kv.history[clerk] = make(map[int]string)
	}
	kv.history[clerk][id] = reply.Value
	kv.db[key] += value
	kv.maxAppliedOp[clerk] = id + 1
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.db = make(map[string]string)
	kv.history = make(map[int64]map[int]string)
	kv.maxAppliedOp = make(map[int64]int)
	return kv
}
