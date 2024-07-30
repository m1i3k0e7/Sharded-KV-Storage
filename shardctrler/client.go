package shardctrler

//
// Shardctrler clerk.
//

import "6.5840/labrpc"
// import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
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
	// Your code here.
	ck.clerkId = nrand()
	ck.requestId = 1
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{
		Num: num,
		ClerkId: ck.clerkId,
		OpId: ck.getRequestId(),
	}
	leader := ck.leader
	for {
		reply := QueryReply{}
		if ck.servers[leader].Call("ShardCtrler.Query", args, &reply) {
			if reply.Err == OK && reply.WrongLeader == false {
				ck.leader = leader
				return reply.Config
			} else if reply.WrongLeader == false {
				ck.leader = leader
				continue
			}
		}
		leader = (leader + 1) % len(ck.servers)
		// time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{
		Servers: servers,
		ClerkId: ck.clerkId,
		OpId: ck.getRequestId(),
	}
	leader := ck.leader
	for {
		reply := JoinReply{}
		if ck.servers[leader].Call("ShardCtrler.Join", args, &reply) {
			if reply.Err == OK && reply.WrongLeader == false {
				ck.leader = leader
				return
			} else if reply.WrongLeader == false {
				ck.leader = leader
				continue
			}
		}
		leader = (leader + 1) % len(ck.servers)
		// time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{
		GIDs: gids,
		ClerkId: ck.clerkId,
		OpId: ck.getRequestId(),
	}
	leader := ck.leader
	for {
		reply := LeaveReply{}
		if ck.servers[leader].Call("ShardCtrler.Leave", args, &reply) {
			if reply.Err == OK && reply.WrongLeader == false {
				ck.leader = leader
				return
			} else if reply.WrongLeader == false {
				ck.leader = leader
				continue
			}
		}
		leader = (leader + 1) % len(ck.servers)
		// time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{
		Shard: shard,
		GID: gid,
		ClerkId: ck.clerkId,
		OpId: ck.getRequestId(),
	}
	leader := ck.leader
	for {
		reply := MoveReply{}
		if ck.servers[leader].Call("ShardCtrler.Move", args, &reply) {
			if reply.Err == OK && reply.WrongLeader == false {
				ck.leader = leader
				return
			} else if reply.WrongLeader == false {
				ck.leader = leader
				continue
			}
		}
		leader = (leader + 1) % len(ck.servers)
		// time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) getRequestId() int {
	id := ck.requestId
	ck.requestId++
	return id
}