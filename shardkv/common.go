package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
	ErrStaleData   = "ErrStaleData"
)

const (
	GET = iota
	PUT
	APPEND
	UPDATE
	ADD
	ADDUP
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	RequestId int
	ClerkId int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	RequestId int
	ClerkId int64
}

type GetReply struct {
	Err   Err
	Value string
}

type OpReply struct {
	Err Err
	ClerkId int64
	OpId int
	Shard Shard
	Storage map[string]string
	Num int
}

type AddArgs struct {
	RequestId int
	ClerkId int64
	ConfigNum int
	ShardId int
	Storage map[string]string
	MaxAppliedId map[int64]int
}

type AddReply struct {
	Err Err
}