package kvraft

import "6.5840/labrpc"
import "crypto/rand"
import "math/big"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	requestId int
	clerkId int64
	leader int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clerkId = nrand()
	ck.requestId = 1
	return ck
}

func (ck *Clerk) Get(key string) string {
	args := GetArgs{
		Key: key,
		ClerkId: ck.clerkId,
		OpId: ck.getRequestId(),
	}
	leader := ck.leader
	// DPrintf("Get ready, %d, key: %v, clerk: %d", args.OpId, key, ck.clerkId)
	for {
		reply := GetReply{}
		if ck.servers[leader].Call("KVServer.Get", &args, &reply) {
			switch reply.Err {
				case OK:
					ck.leader = leader
					// DPrintf("Get end, %d, value: %v, clerk: %d", args.OpId, reply.Value, ck.clerkId)
					return reply.Value
				case ErrNoKey:
					ck.leader = leader
					// DPrintf("Get end, %d, clerk: %d", args.OpId, ck.clerkId)
					return ""
				case ErrWrongLeader:
					leader = (leader + 1) % len(ck.servers)
					continue
				case ErrTimeout:
					continue
			}
		}
		leader = (leader + 1) % len(ck.servers)
	}
	
}

func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{
		Key: key,
		Value: value,
		ClerkId: ck.clerkId,
		OpId: ck.getRequestId(),
	}
	leader := ck.leader
	// DPrintf("%v ready, %d, key: %v, value: %v, clerk: %d", op, args.OpId, key, value, ck.clerkId)
	for {
		reply := PutAppendReply{}
		if ck.servers[leader].Call("KVServer."+op, &args, &reply) {
			switch reply.Err {
				case OK:
					ck.leader = leader
					// DPrintf("%v end, %d, clerk: %d", op, args.OpId, ck.clerkId)
					return
				case ErrWrongLeader:
					leader = (leader + 1) % len(ck.servers)
					continue
				case ErrTimeout:
					continue
			}
		}
		leader = (leader + 1) % len(ck.servers)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

func (ck *Clerk) getRequestId() int {
	id := ck.requestId
	ck.requestId++
	return id
}