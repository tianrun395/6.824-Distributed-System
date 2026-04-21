package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"main/labgob"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

// import "bytes"
// import "../labgob"

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Command interface{}
	Term    int
}

const (
	Follower  = 0
	Candidate = 1
	Leader    = 2
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	log         []LogEntry
	commitIndex int
	lastApplied int
	applyCh     chan ApplyMsg

	// candidate state
	state int // 0 for follower, 1 for candidate, 2 for leader

	// leader state
	nextIndex  []int
	matchIndex []int

	// election timer
	lastResetElectionTime time.Time
	electionTimeout       time.Duration
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		// error...
		DPrintf("Decoding persisted state error")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) resetElectionTimer() {
	rf.lastResetElectionTime = time.Now()
	// random election timeout in [500ms, 1000ms)
	rf.electionTimeout = time.Duration(500+rand.Intn(500)) * time.Millisecond
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
		rf.state = Follower
	}
	reply.Term = rf.currentTerm
	currentServerLogIndex := len(rf.log) - 1
	currentServerLogTerm := rf.log[currentServerLogIndex].Term
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		(args.LastLogTerm > currentServerLogTerm || (args.LastLogTerm == currentServerLogTerm && args.LastLogIndex >= currentServerLogIndex)) {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.state = Follower
		rf.persist()
		rf.resetElectionTimer()
	} else {
		reply.VoteGranted = false
	}
	return
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int // leader's term
	LeaderId     int // so follower can redirect clients
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int // currentTerm, for leader to update itself
	Success bool
}

// AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	// reset election timer
	rf.resetElectionTimer()
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1 // only reset votedFor when term increases to avoid voting for multiple candidates in the same term
		rf.persist()
	}
	rf.state = Follower
	reply.Term = rf.currentTerm
	if args.PrevLogIndex >= len(rf.log) || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		return
	}

	for i, entry := range args.Entries {
		if len(rf.log) > args.PrevLogIndex+1+i {
			if rf.log[args.PrevLogIndex+1+i].Term == entry.Term {
				continue
			} else {
				rf.log = rf.log[:args.PrevLogIndex+1+i]
				rf.log = append(rf.log, entry)
				rf.persist()
			}
		} else {
			rf.log = append(rf.log, entry)
			rf.persist()
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
	}
	reply.Success = true
	return
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1

	// Your code here (2B).
	rf.mu.Lock()
	term := rf.currentTerm
	if rf.state != Leader {
		rf.mu.Unlock()
		return index, term, false
	}
	rf.log = append(rf.log, LogEntry{Command: command, Term: rf.currentTerm})
	rf.persist()
	index = len(rf.log) - 1
	rf.matchIndex[rf.me] = index
	rf.nextIndex[rf.me] = index + 1
	rf.mu.Unlock()
	go func() {
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			go rf.replicateToPeer(i)
		}
	}()
	return index, term, true
}

func (rf *Raft) replicateToPeer(i int) {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state != Leader || rf.nextIndex[i] >= len(rf.log) {
			rf.mu.Unlock()
			return
		}
		term := rf.currentTerm
		prevIndex := rf.nextIndex[i] - 1
		prevTerm := rf.log[prevIndex].Term
		entries := make([]LogEntry, len(rf.log)-rf.nextIndex[i])
		copy(entries, rf.log[rf.nextIndex[i]:])
		commitIndex := rf.commitIndex
		rf.mu.Unlock()
		reply := &AppendEntriesReply{}
		ok := rf.sendAppendEntries(i, &AppendEntriesArgs{
			Term:         term,
			LeaderId:     rf.me,
			PrevLogIndex: prevIndex,
			PrevLogTerm:  prevTerm,
			Entries:      entries,
			LeaderCommit: commitIndex,
		}, reply)
		if !ok {
			continue
		}
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = Follower
			rf.votedFor = -1
			rf.persist()
			rf.resetElectionTimer()
			rf.mu.Unlock()
			return
		}

		if reply.Success {
			rf.matchIndex[i] = prevIndex + len(entries)
			rf.nextIndex[i] = rf.matchIndex[i] + 1
			rf.mu.Unlock()
			break
		} else {
			rf.nextIndex[i] = max(1, rf.nextIndex[i]-1)
			rf.mu.Unlock()
		}
	}
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) startElection(shouldStartHeartbeat chan bool) {
	// start election
	rf.mu.Lock()
	if rf.state == Leader {
		rf.mu.Unlock()
		return
	}
	rf.currentTerm++
	termWhenStartElection := rf.currentTerm
	rf.votedFor = rf.me
	rf.persist()
	rf.state = Candidate
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term
	rf.mu.Unlock()
	voteReceived := 1
	finished := 1
	cond := sync.NewCond(&rf.mu)
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(termWhenStartElection int, i int) {
			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(i, &RequestVoteArgs{
				Term:         termWhenStartElection,
				CandidateId:  rf.me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}, reply)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			defer cond.Broadcast()
			finished++
			if !ok {
				return
			}

			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.state = Follower
				rf.persist()
				rf.resetElectionTimer()
			}
			if reply.VoteGranted && rf.state == Candidate && rf.currentTerm == termWhenStartElection {
				voteReceived++
			}
		}(termWhenStartElection, i)
	}
	rf.mu.Lock()
	for voteReceived <= len(rf.peers)/2 && finished < len(rf.peers) &&
		rf.state == Candidate && rf.currentTerm == termWhenStartElection {
		cond.Wait()
	}
	if voteReceived > len(rf.peers)/2 && rf.state == Candidate && rf.currentTerm == termWhenStartElection {
		// become leader
		rf.state = Leader
		// reinitialize nextIndex and matchIndex after election
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = len(rf.log) // rf.log indexed from 1
			rf.matchIndex[i] = 0
		}
		rf.mu.Unlock()
		shouldStartHeartbeat <- true
	} else {
		rf.mu.Unlock()
	}
}

func (rf *Raft) electionLoop(shouldStartHeartbeat chan bool) {
	for !rf.killed() {
		rf.mu.Lock()
		state := rf.state
		last := rf.lastResetElectionTime
		timeout := rf.electionTimeout
		rf.mu.Unlock()
		if state == Leader || time.Since(last) < timeout {
			// not time for election
			time.Sleep(20 * time.Millisecond)
			continue
		}
		rf.mu.Lock()
		rf.resetElectionTimer()
		rf.mu.Unlock()
		go rf.startElection(shouldStartHeartbeat)
	}
}

func (rf *Raft) heartbeatLoop(shouldStartHeartbeat chan bool) {
	for !rf.killed() {
		<-shouldStartHeartbeat
		for !rf.killed() { // & not idle
			rf.mu.Lock()
			if rf.state != Leader {
				rf.mu.Unlock()
				break
			}
			term := rf.currentTerm
			prevIndex := len(rf.log) - 1
			prevTerm := rf.log[prevIndex].Term
			commitIndex := rf.commitIndex
			rf.mu.Unlock()
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				go func(i int) {
					reply := &AppendEntriesReply{}
					rf.sendAppendEntries(i, &AppendEntriesArgs{
						Term:         term,
						LeaderId:     rf.me,
						PrevLogIndex: prevIndex,
						PrevLogTerm:  prevTerm,
						Entries:      make([]LogEntry, 0),
						LeaderCommit: commitIndex,
					}, reply)
					rf.mu.Lock()
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.state = Follower
						rf.votedFor = -1
						rf.persist()
						rf.resetElectionTimer()
					}
					rf.mu.Unlock()
				}(i)
			}
			time.Sleep(150 * time.Millisecond)
		}
	}
}

func (rf *Raft) applyEntriesToStateMachine() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			lastApplied := rf.lastApplied
			command := rf.log[lastApplied].Command
			rf.mu.Unlock()
			rf.applyCh <- ApplyMsg{
				Command:      command,
				CommandValid: true,
				CommandIndex: lastApplied,
			}
		} else {
			rf.mu.Unlock()
			time.Sleep(20 * time.Millisecond)
		}
	}
}

func (rf *Raft) updateLeaderCommitIndex() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state == Leader {
			for i := len(rf.log) - 1; i > rf.commitIndex; i-- {
				count := 1
				for j := 0; j < len(rf.peers); j++ {
					if j != rf.me && rf.matchIndex[j] >= i {
						count++
					}
				}
				if count > len(rf.peers)/2 && rf.log[i].Term == rf.currentTerm {
					rf.commitIndex = i
					break
				}
			}
		}
		rf.mu.Unlock()
		time.Sleep(20 * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCh = applyCh
	rf.log = make([]LogEntry, 0)
	rf.log = append(rf.log, LogEntry{Term: 0})
	rf.matchIndex = make([]int, len(peers))
	rf.nextIndex = make([]int, len(peers))
	rf.state = Follower
	rf.votedFor = -1
	rf.mu.Lock()
	rf.resetElectionTimer()
	rf.mu.Unlock()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	shouldStartHeartbeat := make(chan bool)
	// start a goroutine for election
	go rf.electionLoop(shouldStartHeartbeat)

	// start a goroutine for heartbeat
	go rf.heartbeatLoop(shouldStartHeartbeat)

	// start a goroutine for applying committed log entries to state machine
	go rf.applyEntriesToStateMachine()

	// start a goroutine to check whether to update leader commitIndex
	go rf.updateLeaderCommitIndex()

	return rf
}
