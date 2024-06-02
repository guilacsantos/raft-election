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
//   should send an ApplyMsg to the service (or tster)
//   in the same server.
//

import (
	"fmt"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "encoding/gob"

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type LogEntry struct {
	LogTerm int
	Index   int
	Data    int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu             sync.Mutex          // Lock to protect shared access to this peer's state
	peers          []*labrpc.ClientEnd // RPC end points of all peers
	persister      *Persister          // Object to hold this peer's persisted state
	alive          bool
	me             int // this peer's index into peers[]
	state          string
	log            []LogEntry
	currentTerm    int
	termLeader     int
	votedFor       []int
	votesReceived  []int
	timeout        *time.Timer
	heartbeatTimer *time.Timer
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var isLeader bool
	if rf.state == "leader" {
		isLeader = true
	}

	// Your code here (2A).
	return rf.currentTerm, isLeader
}

func (rf *Raft) restartTimer() {
	//fmt.Println("Timer restarted on node ", rf.me)
	//rf.timeout.Stop()
	rf.timeout.Reset(time.Duration(300+rand.Int31n(350)) * time.Millisecond)
	//fmt.Println("a ")

}

func (rf *Raft) startElection() {
	fmt.Println("Election started on node ", rf.me)

	//Vota em si mesmo, se torna candidato para um novo termo
	rf.currentTerm = rf.currentTerm + 1
	rf.state = "candidate"
	rf.votedFor = nil
	rf.votesReceived = nil
	rf.votedFor = append(rf.votedFor, rf.me)
	rf.votesReceived = append(rf.votesReceived, rf.me)

	//Como log não está implementado nesta versão, sempre manda mensagem de eleiçãp com lastLogTerm e lastLogIndex = 0
	var lastLogTerm int
	var lastLogIndex int

	lastLogTerm = len(rf.log)
	lastLogIndex = 0

	//Starts election timer
	rf.restartTimer()

	//Inicia eleição:
	args := RequestVoteArgs{
		Term:          rf.currentTerm,
		NodeId:        rf.me,
		LastTermIndex: lastLogIndex,
		LastLogTerm:   lastLogTerm,
	}

	for i := 0; i < len(rf.peers); i++ {
		if !(i == rf.me) && rf.state == "candidate" {
			var peer = rf.peers[i]
			var reply RequestVoteReply
			fmt.Println("Candidate", rf.me, "sending request to node", i, "for vote in term", rf.currentTerm)
			var granted = peer.Call("Raft.RequestVote", &args, &reply)
			if granted {
				rf.votesReceived = append(rf.votesReceived, i)
				fmt.Println("Node", i, "voted for", rf.me, "in term", rf.currentTerm)
			}
			rf.checkIfWonElection()

		}
	}
}

func (rf *Raft) checkIfWonElection() {
	if rf.state == "candidate" && len(rf.votesReceived) > len(rf.peers)/2 {
		fmt.Println("Node elected leader of term", rf.currentTerm, ": ", rf.me, ", by quorum", rf.votesReceived)
		rf.mu.Lock()
		rf.state = "leader"
		rf.heartbeatTimer.Reset(259 * time.Millisecond)
		rf.restartTimer()
		rf.mu.Unlock()
		rf.sendAppendEntries()
	}
}
func (rf *Raft) awaitEvents() {
	for rf.alive {
		select {
		case <-rf.timeout.C:
			if rf.state != "leader" {
				fmt.Println("Election timeout reached on node ", rf.me)
				rf.startElection()
			}
			if rf.state == "leader" {
				rf.restartTimer()
			}
		case <-rf.heartbeatTimer.C:
			if rf.state == "leader" {
				fmt.Println("Leader", rf.me, "of term", rf.currentTerm, "sending heartbeats... ")
				rf.heartbeatTimer.Reset(250 * time.Millisecond)
				rf.sendAppendEntries()
			}
		}

	}
}

type AppendEntriesArgs struct {
	Term         int
	Leader       int
	LastLogIndex int
	LastLogTerm  int
	Data         []LogEntry
}

type AppendEntriesReply struct {
	Term int
	Ok   int
}

func (rf *Raft) sendAppendEntries() {
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		Leader:       rf.me,
		LastLogIndex: 0,
		LastLogTerm:  0,
		Data:         rf.log,
	}

	for peer := 0; peer < len(rf.peers); peer++ {
		reply := AppendEntriesReply{}
		go func(peerId int) {
			if peerId == rf.me {
				return
			}
			//fmt.Println("Node", rf.me, "appending entries on", peerId)
			rf.peers[peerId].Call("Raft.AppendEntries", &args, &reply)
		}(peer)
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		fmt.Println("Error in append entries. Node: ", rf.me)
		reply.Ok = 0
	} else {
		fmt.Println("Heartbeat from", args.Leader, "received on", rf.me)
		rf.restartTimer()
		reply.Ok = 1
		rf.state = "follower"
		rf.currentTerm = args.Term
		rf.termLeader = args.Leader
	}
}

func (rf *Raft) persist() {
}

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	NodeId        int
	Term          int
	LastTermIndex int
	LastLogTerm   int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Granted bool
	Term    int
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	fmt.Println("Vote requested from node", rf.me, "by candidate", args.NodeId, "in term", args.Term)
	reply.Term = rf.currentTerm
	reply.Granted = false

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.state = "follower"
		rf.votedFor = nil
		rf.termLeader = -1
		rf.currentTerm = args.Term
	}

	if len(rf.votedFor) < 1 || rf.votedFor[0] == args.NodeId {
		ownLastLogTerm := 0
		if len(rf.log) > 0 {
			ownLastLogTerm = rf.log[len(rf.log)-1].LogTerm
		}

		//Na pratica, como nao ha insercao em log, voto sempre deve ser granted se votedFor for nulo
		if args.LastLogTerm > ownLastLogTerm || args.LastLogTerm == ownLastLogTerm && len(rf.log) <= args.LastLogTerm {
			reply.Granted = true
			rf.votedFor = append(rf.votedFor, args.NodeId)
			return
		}

	}

}

// example RequestVote RPC handler.

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
	fmt.Println("sendRequestVote on node ", rf.me)

	fmt.Println(args)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	fmt.Println("Started node ", rf.me)

	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (rf *Raft) Kill() {
	fmt.Println("Killed", rf.state, "node", rf.me)
	rf.alive = false
	// Your code here, if desired.
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
	fmt.Println("Making node ", me)
	//fmt.Println(peers)

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = "follower"
	rf.currentTerm = 0
	rf.votedFor = nil
	rf.votesReceived = nil
	rf.termLeader = -1

	//Começa timer
	rf.timeout = time.NewTimer(time.Duration(500+rand.Int31n(350)) * time.Millisecond)
	rf.heartbeatTimer = time.NewTimer(250 * time.Millisecond)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	//função que faz o nó escutar e responder para eventos como fins de timeout de eleição ou de envio de heartbeat
	rf.alive = true
	go rf.awaitEvents()
	//fmt.Println("Finished making node ", me)

	return rf
}
