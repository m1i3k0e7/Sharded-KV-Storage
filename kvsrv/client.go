package kvsrv

import "6.5840/labrpc"
import "crypto/rand"
import "math/big"


type Clerk struct {
	server *labrpc.ClientEnd
	// You will have to modify this struct.
	clerkId int64
	requestId int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server
	// You'll have to add code here.
	ck.requestId = 0
	ck.clerkId = nrand()
	return ck
}

func (ck *Clerk) Get(key string) string {
	args := GetArgs{
		Key: key,
	}
	reply := GetReply{}
	for {
		ok := ck.server.Call("KVServer.Get", &args, &reply)
		if ok {
			return reply.Value
		}
	}
}

func (ck *Clerk) PutAppend(key string, value string, op string) string {
	reply := PutAppendReply{}
	id := ck.getRequestId()
	for {
		args := PutAppendArgs{
			Key: key,
			Value: value,
			ClerkId: ck.clerkId,
			OpId: id,
		}
		for !ck.server.Call("KVServer."+op, &args, &reply) || reply.Status != OK {}
		args = PutAppendArgs{
			ClerkId: ck.clerkId,
			Type: REPORT,
			OpId: id,
		}
		for !ck.server.Call("KVServer."+op, &args, &reply) {}
		return reply.Value
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}

func (ck *Clerk) getRequestId() int {
	id := ck.requestId
	ck.requestId++
	return id
}