package raft

import (
	"bytes"
	"math/rand"
	// "sync"
	"sync/atomic"
	"time"
	"sort"
	"log"
	"6.5840/labgob"
	"6.5840/labrpc"
	"github.com/sasha-s/go-deadlock"
)

const (
	LEADER    = 0
	CANDIDATE = 1
	FOLLOWER  = 2
)

const (
	MinInterval = 200
	MaxInterval = 300
	HeartbeatInterval = 100
	tickerTimeoutBase = 10
	tickerTimeoutInterval = 30
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        deadlock.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	CurrentTerm int
	VotedFor int
	Log []LogEntry
	
	CommitIndex int
	LastApplied int

	NextIndex []int
	MatchIndex []int

	LastHeartBeat time.Time
	State int // 0: Leader, 1: Candidate, 2: Follower

	TimeoutInterval int
	CommitIdUpdate int
	ApplyCh chan ApplyMsg

	LastIncludedIndex int
	LastIncludedTerm int
}

type LogEntry struct {
	Index int
	Term int
	Command interface{}
}

type RequestVoteArgs struct {
	Term int
	CandidateID int
	LastLogIndex int
	LastLogTerm int
}

type RequestVoteReply struct {
	Term int
	VoteGranted bool
}

type InstallSnapshotArgs struct {
	Term int
	LeaderID int
	LastIncludedIndex int
	LastIncludedTerm int
	Offset int
	Data []byte
	Done bool
}

type InstallSnapshotReply struct {
	Term int
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	ConflictTerm int
	ConflictIndex int
	FollowerLen int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.CurrentTerm
	isleader = (rf.State == LEADER)
	return term, isleader 
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Log)
	e.Encode(rf.LastIncludedIndex)
	e.Encode(rf.LastIncludedTerm)
	raftstate := w.Bytes()
	rf.persister.SaveRaftState(raftstate)
}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var CurrentTerm int
	var VotedFor int
	var Log []LogEntry
	var LastIncludedIndex int
	var LastIncludedTerm int
	if d.Decode(&CurrentTerm) != nil || d.Decode(&VotedFor) != nil || d.Decode(&Log) != nil || d.Decode(&LastIncludedIndex) != nil || d.Decode(&LastIncludedTerm) != nil {
		log.Println("Persister decode error")
	} else {
	  rf.CurrentTerm = CurrentTerm
	  rf.VotedFor = VotedFor
	  rf.Log = Log
	  rf.LastIncludedIndex = LastIncludedIndex
	  rf.LastIncludedTerm = LastIncludedTerm
	}
}

func (rf *Raft)sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	id := index - 1
	if rf.LastIncludedIndex >= id {
		return
	}
	
	// if id - rf.LastIncludedIndex - 1 < len(rf.Log) {
	rf.LastIncludedTerm = rf.Log[id - rf.LastIncludedIndex - 1].Term
	rf.Log = rf.Log[id - rf.LastIncludedIndex:]
	rf.LastIncludedIndex = id
	// }

	rf.CommitIndex = Max(rf.CommitIndex, rf.LastIncludedIndex)
	rf.LastApplied = Max(rf.LastApplied, rf.LastIncludedIndex)
	rf.persist()
	rf.persister.SaveSnapshot(snapshot)
}

func (rf *Raft)InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()

	if rf.CurrentTerm > args.Term {
		reply.Term = rf.CurrentTerm
		rf.mu.Unlock()
		return
	}

	reply.Term = args.Term
	rf.convertToFollower(args.Term, true)
	if rf.LastIncludedIndex >= args.LastIncludedIndex {
		rf.mu.Unlock()
		return
	}

	// DPrintf("leader %d send snapshot to %d, (%d, %d)", args.LeaderID, rf.me, args.LastIncludedIndex, rf.LastIncludedIndex)

	index := Min(args.LastIncludedIndex - rf.LastIncludedIndex, len(rf.Log) - 1)
	if index > 0 {
		rf.Log = rf.Log[index:]
	}
	rf.LastIncludedIndex = args.LastIncludedIndex
	rf.LastIncludedTerm = args.LastIncludedTerm

	rf.persist()
	
	rf.CommitIndex = Max(rf.CommitIndex, args.LastIncludedIndex)
	rf.LastApplied = Max(rf.LastApplied, args.LastIncludedIndex)
	rf.persister.SaveSnapshot(args.Data)

	applyMsg := ApplyMsg{
		SnapshotValid: true,
		Snapshot: args.Data,
		SnapshotTerm: rf.LastIncludedTerm, 
		SnapshotIndex: rf.LastIncludedIndex + 1,
	}
	rf.mu.Unlock()
	rf.ApplyCh <- applyMsg
}

func (rf *Raft) sendSnapShot(i int) {
	rf.mu.Lock()
	args := InstallSnapshotArgs{
									Term: rf.CurrentTerm,
									LeaderID: rf.me,
									LastIncludedIndex: rf.LastIncludedIndex,
									LastIncludedTerm: rf.LastIncludedTerm,
									Data: rf.persister.ReadSnapshot(),
								}
	rf.mu.Unlock()
	
	// DPrintf("leader %d send snapshot to %d, last id: %d", rf.me, i, args.LastIncludedIndex)
	reply := InstallSnapshotReply{}
	ok := rf.sendInstallSnapshot(i, &args, &reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok && rf.State == LEADER && rf.CurrentTerm == args.Term {
		if reply.Term > rf.CurrentTerm {
			rf.convertToFollower(reply.Term, true)
			return
		}
		rf.MatchIndex[i] = rf.LastIncludedIndex
		rf.NextIndex[i] = rf.LastIncludedIndex + 1
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.CurrentTerm
	reply.Success = true
	if args.Term < reply.Term {
		reply.Success = false
		return 
	} else {
		rf.convertToFollower(args.Term, true)
	}
	
	if args.PrevLogIndex < rf.LastIncludedIndex {
		reply.Success = false
		reply.ConflictIndex = rf.LastIncludedIndex + 1
		reply.ConflictTerm = -1
		return
	}

	lastLog := rf.getLastLog()
	prevLog := LogEntry{Index: rf.LastIncludedIndex, Term: rf.LastIncludedTerm,}
	if args.PrevLogIndex > rf.LastIncludedIndex && args.PrevLogIndex - rf.LastIncludedIndex - 1 <= len(rf.Log) - 1 {
		prevLog = rf.Log[args.PrevLogIndex - rf.LastIncludedIndex - 1]
	}
	if args.PrevLogIndex >= rf.LastIncludedIndex + 1 && (lastLog.Index < args.PrevLogIndex || prevLog.Term != args.PrevLogTerm) {
		reply.Success = false
		if lastLog.Index < args.PrevLogIndex {
			reply.ConflictIndex = lastLog.Index + 1
			reply.ConflictTerm = -1
		} else {
			reply.ConflictTerm = prevLog.Term
			reply.ConflictIndex = sort.Search(len(rf.Log), func(id int) bool {
				return rf.Log[id].Term >= reply.ConflictTerm
			}) + rf.LastIncludedIndex + 1
			reply.FollowerLen = rf.LastIncludedIndex + len(rf.Log) + 1
		}
		return 
	} else if len(args.Entries) > 0 {
		nextLogIndex := args.PrevLogIndex + 1
		if lastLog.Index > args.PrevLogIndex {
			for i := args.PrevLogIndex + 1; i <= lastLog.Index; i++ {
				if i - args.PrevLogIndex - 1 >= len(args.Entries) {
					break
				}
				if rf.Log[i - rf.LastIncludedIndex - 1].Term != args.Entries[i - args.PrevLogIndex - 1].Term {
					break
				}
				nextLogIndex = i + 1
			}
		}
		if nextLogIndex - args.PrevLogIndex - 1 >= len(args.Entries) {
			reply.Success = false
			reply.ConflictIndex = lastLog.Index + 1
			reply.ConflictTerm = -1
			return
		}
		rf.Log = append(rf.Log[:nextLogIndex - rf.LastIncludedIndex - 1], args.Entries[nextLogIndex - args.PrevLogIndex - 1:]...)
		rf.persist()
	}

	if args.LeaderCommit > rf.CommitIndex && len(rf.Log) > 0 {
		rf.CommitIndex = Min(args.LeaderCommit, rf.Log[len(rf.Log) - 1].Index)
	}
}

func (rf *Raft) sendAppendEntry(i int) {
	rf.mu.Lock()
	if rf.State != LEADER {
		rf.mu.Unlock()
		return
	}
	args := AppendEntriesArgs{
								Term: rf.CurrentTerm,
								LeaderID: rf.me,
								PrevLogIndex: rf.NextIndex[i] - 1,
								PrevLogTerm: 0,
								Entries: nil,
								LeaderCommit: rf.CommitIndex,
							}
	// args.prevLogIndex >= rf.LastIncludedIndex
	// if args.PrevLogIndex > lastLog.Index {
	// 	args.PrevLogIndex = lastLog.Index
	// 	args.PrevLogTerm = lastLog.Term
	// 	rf.NextIndex[i] = args.PrevLogIndex + 1
	// } else 
	if args.PrevLogIndex == rf.LastIncludedIndex {
		args.PrevLogTerm = rf.LastIncludedTerm
	} else if args.PrevLogIndex > rf.LastIncludedIndex {
		args.PrevLogTerm = rf.Log[args.PrevLogIndex - rf.LastIncludedIndex - 1].Term
	} else {
		rf.mu.Unlock()
		return
	}
	lastLogIndex := rf.LastIncludedIndex + len(rf.Log)
	if lastLogIndex >= rf.NextIndex[i] {
		newEntries := rf.Log[rf.NextIndex[i] - rf.LastIncludedIndex - 1:]
		args.Entries = make([]LogEntry, len(newEntries))
		copy(args.Entries, newEntries)
	}
	rf.mu.Unlock()
	
	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(i, &args, &reply)
	
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok && args.Term == rf.CurrentTerm && rf.State == LEADER{
		if reply.Term > rf.CurrentTerm {
			rf.convertToFollower(reply.Term, true)
			return
		}

		if reply.Success && args.Entries != nil {
			rf.NextIndex[i] = args.Entries[len(args.Entries) - 1].Index + 1
			rf.MatchIndex[i] = rf.NextIndex[i] - 1
			if rf.MatchIndex[i] > rf.CommitIndex {
				rf.CommitIdUpdate++
			}
		} else if !reply.Success {
			if reply.ConflictTerm == -1 {
				rf.NextIndex[i] = reply.ConflictIndex
			} else {
				index := sort.Search(len(rf.Log), func(id int) bool {
					return rf.Log[id].Term > reply.ConflictTerm
				}) - 1
				if index >= 0 && rf.Log[index].Term == reply.ConflictTerm {
					rf.NextIndex[i] = Min(reply.FollowerLen, index + rf.LastIncludedIndex + 1)
				} else {
					rf.NextIndex[i] = reply.ConflictIndex
				}
			}
			
			if args.Entries != nil {
				go rf.sendAppendEntry(i)
			}
		}
		if rf.CommitIdUpdate > len(rf.peers) / 2 {
			matchIndex := make([]int, len(rf.MatchIndex))
			copy(matchIndex, rf.MatchIndex)
			sort.Ints(matchIndex)
			N := matchIndex[len(matchIndex) / 2 + len(matchIndex) % 2]
			if N >= rf.LastIncludedIndex + 1 && rf.Log[N - rf.LastIncludedIndex - 1].Term == rf.CurrentTerm && N > rf.CommitIndex{
				rf.CommitIndex = N
			}
			rf.CommitIdUpdate = 1
		}
	}
}

func (rf *Raft) operateLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.LastHeartBeat = time.Now()
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		if rf.LastIncludedIndex <= rf.NextIndex[i] - 1 {
			go rf.sendAppendEntry(i)
		} else {
			go rf.sendSnapShot(i)
		}
	}
	// go rf.applyLogEntries()
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader := (rf.State == LEADER)
	if isLeader {
		term = rf.CurrentTerm
		index = len(rf.Log) + rf.LastIncludedIndex + 1
		newEntry := LogEntry{Index: index, Term: term, Command: command,}
		rf.Log = append(rf.Log, newEntry)
		rf.persist()
		// go rf.operateLeader()
	}
	return index + 1, term, isLeader
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) applyLogEntries() {
	rf.mu.Lock()
	lastLogIndex := rf.LastIncludedIndex + len(rf.Log)
	
	if rf.CommitIndex > lastLogIndex {
		rf.CommitIndex = lastLogIndex
	}

	if rf.CommitIndex <= rf.LastApplied {
		rf.LastApplied = rf.CommitIndex
		rf.mu.Unlock()
		return
	}

	if len(rf.Log) > 0 && rf.LastApplied + 1 < rf.Log[0].Index {
		rf.LastApplied = rf.Log[0].Index - 1
	}

	commands := []ApplyMsg{}
	for i := rf.LastApplied + 1; i <= rf.CommitIndex; i++ {
		commands = append(commands, ApplyMsg{CommandValid: true, Command: rf.Log[i - rf.LastIncludedIndex - 1].Command, CommandIndex: i + 1,})
	}
	rf.LastApplied = rf.CommitIndex
	rf.mu.Unlock()

	for _, command := range commands {
		rf.ApplyCh <- command
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.CurrentTerm
	if rf.CurrentTerm > args.Term {
		rf.VotedFor = -1
		rf.persist()
		reply.VoteGranted = false
		return
	} else if (args.Term > rf.CurrentTerm) {
		rf.convertToFollower(args.Term, true)
	}

	if rf.VotedFor == -1 || args.CandidateID == rf.VotedFor {
		lastLog := rf.getLastLog()
		if lastLog.Term > args.LastLogTerm || (lastLog.Term == args.LastLogTerm && lastLog.Index > args.LastLogIndex) {
			rf.VotedFor = -1
			rf.persist()
			reply.VoteGranted = false
			return ;
		}
		rf.convertToFollower(args.Term, true)
		rf.VotedFor = args.CandidateID
		reply.VoteGranted = true
	} else {
		rf.VotedFor = -1
		reply.VoteGranted = false
	}
	rf.persist()
}

func (rf *Raft) kickoffElection() {
	rf.convertToCandidate()
	voteNum := 1
	doneNum := 1
	totalVote := len(rf.peers)
	for i := 0; i < totalVote; i++ {
		if i == rf.me {
			continue
		}
		go func(i int) {
			rf.mu.Lock()
			if rf.State != CANDIDATE {
				rf.mu.Unlock()
				return 
			}
			args := RequestVoteArgs{  Term: rf.CurrentTerm,
									  CandidateID: rf.me,
									  LastLogIndex: rf.getLastLog().Index,
									  LastLogTerm: rf.getLastLog().Term,
									}
			rf.mu.Unlock()

			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(i, &args, &reply)

			rf.mu.Lock()
			defer rf.mu.Unlock()
			doneNum++
			//////////////////////////////
			if rf.CurrentTerm != args.Term {
				return
			}
			//////////////////////////////
			if ok && reply.VoteGranted {
				voteNum++
				tmpVoteNum := voteNum
				if rf.State == CANDIDATE && tmpVoteNum > totalVote / 2 {
					rf.convertToLeader()
					go rf.operateLeader()
					return 
				}
			}
			vetoNum := doneNum - voteNum
			if rf.State != FOLLOWER && (reply.Term > rf.CurrentTerm || (vetoNum > totalVote / 2)){
				rf.convertToFollower(reply.Term, true)
			}
		}(i)
	}
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		rf.applyLogEntries()
		rf.mu.Lock()		
		if time.Since(rf.LastHeartBeat) >= time.Duration(rf.TimeoutInterval) * time.Millisecond {
			if rf.State == FOLLOWER {
				rf.mu.Unlock()
				rf.kickoffElection()
			} else if rf.State == LEADER {
				rf.mu.Unlock()
				rf.operateLeader()
			} else {
				rf.mu.Unlock()
				rf.kickoffElection()
			}
		} else {
			rf.mu.Unlock()
		}
		// rf.mu.Unlock()
		ms := tickerTimeoutBase + (rand.Int63() % tickerTimeoutInterval)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) GetLastLogIndex() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.LastIncludedIndex + len(rf.Log)
}

func (rf *Raft) getLastLog() LogEntry {
	if len(rf.Log) == 0 {
		return LogEntry{Index: rf.LastIncludedIndex, Term: rf.LastIncludedTerm}
	}
	return rf.Log[len(rf.Log) - 1]
}

func (rf *Raft) getLastHBTime() time.Time {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.LastHeartBeat
}

func (rf *Raft) convertToFollower(newTerm int, persist bool) {
	rand.Seed(time.Now().UnixNano())
	rf.TimeoutInterval = rand.Intn(MaxInterval - MinInterval + 1) + MinInterval
	rf.LastHeartBeat = time.Now()
	rf.State = FOLLOWER
	rf.VotedFor = -1
	rf.CurrentTerm = newTerm
	if(persist) {
		rf.persist()
	}
}

func (rf *Raft) convertToCandidate() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rand.Seed(time.Now().UnixNano())
	rf.TimeoutInterval = rand.Intn(MaxInterval - MinInterval + 1) + MinInterval
	rf.LastHeartBeat = time.Now()
	rf.CurrentTerm++
	rf.State = CANDIDATE
	rf.VotedFor = rf.me
	rf.persist()
}

func (rf *Raft) convertToLeader() {
	// DPrintf("raft %d become new leader", rf.me)
	rf.TimeoutInterval = HeartbeatInterval
	rf.State = LEADER
	rf.CommitIdUpdate = 1
	nextIndex := rf.LastIncludedIndex + 1
	if len(rf.Log) > 0 {
		nextIndex = rf.Log[len(rf.Log) - 1].Index + 1
	}
	for i := range rf.NextIndex {
		rf.NextIndex[i] = nextIndex
	}
	for i := range rf.MatchIndex {
		rf.MatchIndex[i] = -1
	}
}

func (rf *Raft) GetTerm() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.CurrentTerm
}

func (rf *Raft) GetServerState() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.State
}

func (rf *Raft) ConvertID(i int) int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return i - rf.LastIncludedIndex - 1
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.Log = []LogEntry{}
	rf.CommitIndex = -1
	rf.LastApplied = -1
	rf.NextIndex = make([]int, len(peers))
	rf.MatchIndex = make([]int, len(peers))
	rf.ApplyCh = applyCh
	rf.LastIncludedIndex = -1
	rf.LastIncludedTerm = 0
	// Your initialization code here (3A, 3B, 3C).
	rf.convertToFollower(0, false)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.LastApplied = Max(rf.LastApplied, rf.LastIncludedIndex)
	// start ticker goroutine to start elections
	go rf.ticker()
	return rf
}
