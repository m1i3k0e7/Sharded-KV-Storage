package kvsrv

const (
	MODIFY = 0
	REPORT = 1
)

const (
	OK = 0
	ERR = 1
)
// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClerkId int64
	OpId int
	Type int
}

type PutAppendReply struct {
	Value string
	Status int
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Value string
}
