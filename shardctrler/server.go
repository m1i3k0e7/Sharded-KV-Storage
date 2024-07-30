package shardctrler


import "6.5840/raft"
import "6.5840/labrpc"
// import "sync"
import "6.5840/labgob"
import "time"
import "sort"
import "github.com/sasha-s/go-deadlock"
// import "fmt"

type ShardCtrler struct {
	mu      deadlock.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	
	// Your data here.

	configs []Config // indexed by config num
	maxAppliedId map[int64]int
	opWaitChans map[int]chan Op
	persister *raft.Persister
	snapshotIndex int
}


type Op struct {
	Type int
	OpId int
	ClerkId int64
	Servers map[int][]string
	GIDs []int
	Shard int
	GID   int
	Num int
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	num, clerk, id := args.Num, args.ClerkId, args.OpId
	op := Op{
		Type: QUERY,
		OpId: id,
		ClerkId: clerk,
		Num: num,
	}
	index, isleader := sc.proposeOp(op)
	if !isleader {
		reply.WrongLeader = true
		return
	}

	waitChan := sc.createWaitChan(index)
	defer sc.deleteWaitChan(index)

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	
	select {
		case appliedOp := <-waitChan:
			if appliedOp.OpId != id || appliedOp.ClerkId != clerk {
				reply.WrongLeader = true
			} else {
				sc.mu.Lock()
				tmpConfig := sc.configs[len(sc.configs) - 1]
				if num > -1 && num < len(sc.configs) {
					tmpConfig = sc.configs[num]
				}
				newConfig := Config{
					Num: tmpConfig.Num,
					Shards: [NShards]int{},
					Groups: make(map[int][]string),
				}
				for i := 0; i < NShards; i++ {
					newConfig.Shards[i] = tmpConfig.Shards[i]
				}
				for k, v := range tmpConfig.Groups {
					newConfig.Groups[k] = []string{}
					for i := 0; i < len(v); i++ {
						newConfig.Groups[k] = append(newConfig.Groups[k], v[i])
					}
				}
				reply.Config = newConfig
				sc.mu.Unlock()
				reply.Err = OK
			}
			return
		case <-ticker.C:
			reply.Err = ErrTimeout
	}
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	servers, clerk, id := args.Servers, args.ClerkId, args.OpId
	if sc.isApplied(clerk, id) {
		reply.Err = OK
		return
	}

	op := Op{
		Type: JOIN,
		ClerkId: clerk,
		OpId: id,
		Servers: servers,
	}
	index, isleader := sc.proposeOp(op)
	if !isleader {
		reply.WrongLeader = true
		return
	}

	waitChan := sc.createWaitChan(index)
	defer sc.deleteWaitChan(index)

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	select {
		case appliedOp := <-waitChan:
			if appliedOp.OpId != id || appliedOp.ClerkId != clerk {
				reply.WrongLeader = true
			} else {
				// fmt.Printf("new join: %v\n%v\n\n", servers, sc.configs[len(sc.configs) - 1])
				reply.Err = OK
			}
		case <-ticker.C:
			reply.Err = ErrTimeout
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	gids, clerk, id := args.GIDs, args.ClerkId, args.OpId
	if sc.isApplied(clerk, id) {
		reply.Err = OK
		return
	}

	op := Op{
		Type: LEAVE,
		GIDs: gids,
		ClerkId: clerk,
		OpId: id,
	}
	index, isleader := sc.proposeOp(op)
	if !isleader {
		reply.WrongLeader = true
		return
	}

	waitChan := sc.createWaitChan(index)
	defer sc.deleteWaitChan(index)

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	select {
		case appliedOp := <-waitChan:
			if appliedOp.ClerkId != clerk || appliedOp.OpId != id {
				reply.WrongLeader = true
			} else {
				// fmt.Printf("new leave: %v\n%v\n\n", gids, sc.configs[len(sc.configs) - 1])
				reply.Err = OK
			}
		case <-ticker.C:
			reply.Err = ErrTimeout
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	shard, gid, clerk, id := args.Shard, args.GID, args.ClerkId, args.OpId
	if sc.isApplied(clerk, id) {
		reply.Err = OK
		return
	}

	op := Op{
		Type: MOVE,
		Shard: shard,
		GID: gid,
		ClerkId: clerk,
		OpId: id,
	}
	index, isleader := sc.proposeOp(op)
	if !isleader {
		reply.WrongLeader = true
		return
	}

	waitChan := sc.createWaitChan(index)
	defer sc.deleteWaitChan(index)

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	select {
		case appliedOp := <-waitChan:
			if appliedOp.ClerkId != clerk || appliedOp.OpId != id {
				reply.WrongLeader = true
			} else {
				reply.Err = OK
			}
		case <-ticker.C:
			reply.Err = ErrTimeout
	}
}

func (sc *ShardCtrler) applier() {
	for {
		applyMsg := <-sc.applyCh
		if applyMsg.CommandValid {
			index := applyMsg.CommandIndex
			op := applyMsg.Command.(Op)
			clerk, id := op.ClerkId, op.OpId
			if !sc.isApplied(clerk, id) {
				sc.mu.Lock()
				if op.Type == JOIN {
					sc.operateJoin(op.Servers)
				} else if op.Type == LEAVE {
					sc.operateLeave(op.GIDs)
				} else if op.Type == MOVE {
					sc.operateMove(op.Shard, op.GID)
				}
				sc.maxAppliedId[clerk] = id
				sc.mu.Unlock()
			}

			sc.mu.Lock()
			waitChan, ok := sc.opWaitChans[index]
			sc.mu.Unlock()
			if ok {
				waitChan <- op
			}
		}
	}
}

func (sc *ShardCtrler) operateJoin(servers map[int][]string) {
	latestConfig := sc.configs[len(sc.configs) - 1]
	newConfig := Config{
		Num: latestConfig.Num + 1,
		Shards: [NShards]int{},
		Groups: make(map[int][]string),
	}

	for i := 0; i < NShards; i++ {
		newConfig.Shards[i] = latestConfig.Shards[i]
	}

	newServers := make(map[int][]string)
	for gid, server := range latestConfig.Groups {
		newConfig.Groups[gid] = server
	}
	for gid, server := range servers {
		if _, ok := newConfig.Groups[gid]; !ok {
			newServers[gid] = server
		}
		newConfig.Groups[gid] = server
	}

	if len(latestConfig.Groups) < NShards {
		cnt := make(map[int]int)
		shardNum := [][]int{}
		for gid := range newConfig.Groups {
			cnt[gid] = 0
		}
		for i := 0; i < NShards; i++ {
			if newConfig.Shards[i] != 0 {
				cnt[newConfig.Shards[i]]++
			}
		}
		for gid, num := range cnt {
			shardNum = append(shardNum, []int{num, gid})
		}
		sort.Slice(shardNum, func(a, b int) bool {
			if shardNum[a][0] == shardNum[b][0] {
				return shardNum[a][1] < shardNum[b][1]
			}
			return shardNum[a][0] > shardNum[b][0]
		})

		numServer := len(newConfig.Groups)
		s1 := NShards / numServer
		s2 := NShards % numServer
		allocation := make(map[int]int)
		for i := 0; i < len(shardNum); i++ {
			gid := shardNum[i][1]
			allocation[gid] = s1 + Max(0, Min(1, s2))
			s2--
		}

		rebalance := []int{}
		cnt = make(map[int]int)
		for i := 0; i < NShards; i++ {
			gid := latestConfig.Shards[i]
			if gid != 0 && cnt[gid] < allocation[gid] {
				newConfig.Shards[i] = gid
				cnt[gid]++
			} else {
				rebalance = append(rebalance, i)
			}
		}

		rebalanceId := 0
		for i := 0; i < len(shardNum); i++ {
			gid := shardNum[i][1]
			for cnt[gid] < allocation[gid] {
				newConfig.Shards[rebalance[rebalanceId]] = gid
				cnt[gid]++
				rebalanceId++
			}
		}
	}
	sc.configs = append(sc.configs, newConfig)
}

func (sc *ShardCtrler) operateLeave(gids []int) {
	latestConfig := sc.configs[len(sc.configs) - 1]
	newConfig := Config{
		Num: latestConfig.Num + 1,
		Shards: [NShards]int{},
		Groups: make(map[int][]string),
	}

	for i := 0; i < NShards; i++ {
		newConfig.Shards[i] = latestConfig.Shards[i]
	}

	for gid, server := range latestConfig.Groups {
		newConfig.Groups[gid] = server
	}

	rebalance := []int{}
	for _, gid := range gids {
		for i := 0; i < NShards; i++ {
			if newConfig.Shards[i] == gid {
				rebalance = append(rebalance, i)
				newConfig.Shards[i] = 0
			}
		}
		delete(newConfig.Groups, gid)
	}

	numServer := len(newConfig.Groups)
	if numServer > 0 {
		cnt := make(map[int]int)
		shardNum := [][]int{}
		for gid := range newConfig.Groups {
			cnt[gid] = 0
		}
		for i := 0; i < NShards; i++ {
			if newConfig.Shards[i] != 0 {
				cnt[newConfig.Shards[i]]++
			}
		}
		for gid, num := range cnt {
			shardNum = append(shardNum, []int{num, gid})
		}
		sort.Slice(shardNum, func(a, b int) bool {
			if shardNum[a][0] == shardNum[b][0] {
				return shardNum[a][1] < shardNum[b][1]
			}
			return shardNum[a][0] > shardNum[b][0]
		})

		s1 := NShards / numServer
		s2 := NShards % numServer
		allocation := make(map[int]int)
		for i := 0; i < len(shardNum); i++ {
			gid := shardNum[i][1]
			allocation[gid] = s1 + Max(0, Min(1, s2))
			s2--
		}

		rebalanceId := 0
		for i := 0; i < len(shardNum); i++ {
			gid := shardNum[i][1]
			for cnt[gid] < allocation[gid] {
				newConfig.Shards[rebalance[rebalanceId]] = gid
				cnt[gid]++
				rebalanceId++
			}
		}
	}
	sc.configs = append(sc.configs, newConfig)
}

func (sc *ShardCtrler) operateMove(shard int, gid int) {
	latestConfig := sc.configs[len(sc.configs) - 1]
	newConfig := Config{
		Num: latestConfig.Num + 1,
		Shards: [NShards]int{},
		Groups: make(map[int][]string),
	}

	for i := 0; i < NShards; i++ {
		newConfig.Shards[i] = latestConfig.Shards[i]
	}

	for g, server := range latestConfig.Groups {
		newConfig.Groups[g] = server
	}
	for i := 0; i < NShards; i++ {
		newConfig.Shards[i] = latestConfig.Shards[i]
		if i == shard {
			_, ok := newConfig.Groups[gid]
			if ok {
				newConfig.Shards[i] = gid
			}
		}
	}
	sc.configs = append(sc.configs, newConfig)
}

func (sc *ShardCtrler) createWaitChan(index int) chan Op {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.opWaitChans[index] = make(chan Op, 1)
	return sc.opWaitChans[index]
}

func (sc *ShardCtrler) deleteWaitChan(index int) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	delete(sc.opWaitChans, index)
}

func (sc *ShardCtrler) proposeOp(op Op) (int, bool) {
	index, _, isleader := sc.Raft().Start(op)
	return index, isleader
}

func (sc *ShardCtrler) isApplied(clerk int64, id int) bool {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.maxAppliedId[clerk] >= id
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.maxAppliedId = make(map[int64]int)
	sc.opWaitChans = make(map[int]chan Op)
	sc.persister = persister
	go sc.applier()

	return sc
}
