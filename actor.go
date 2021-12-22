package main

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"log"
	"math/rand"
	"strings"
	"sync"
	"time"
)

type ActorConfig struct {
	NumPeers       int
	CommitIdx      int
	Peers          []string //all servers' port number
	CurrentTerm    int
	Logs           []Log
	Timeout        int
	Role           string
	ID             string // current server's index in Servers []
	NextIndices    []int
	LastLogIndices []int
}

type RoleType int

const (
	Leader RoleType = iota
	Follower
	Candidate
)

type Actor struct {
	// tracks the index of the latest appended log from current leader to each follower
	LastLogIndicies map[string]int

	// tracks the index of the next should-be-appended log for each follower
	NextIndicies map[string]int

	// number of all other peers in system (excluding itself)
	NumPeers int

	// counts the number of times a log has been successfully appended to a follower
	//  used for deciding when to commit at where
	AppendCounter []int

	// track commit status
	CommitIdx int

	// port numbers in string form for each peer in system
	Peers []string

	mu          sync.RWMutex
	CurrentTerm int
	Logs        []Log

	// timeout for receving heartbeat messages and election
	Timeout int
	Role    RoleType

	// port number of the actor, assuming this is unique in system
	ID        string
	VotedTerm int

	counter    int
	lastHBtime time.Time
	lastVRtime time.Time
}

func (this *Actor) Init(id string, num_peers int, peers []string) {
	// TODO: init peers, logs, appendcounter

	this.ID = id
	this.CurrentTerm = 0
	this.Role = Follower
	this.NumPeers = num_peers
	this.NextIndicies = make(map[string]int)
	this.LastLogIndicies = make(map[string]int)
	this.AppendCounter = make([]int, 0)

	this.Logs = make([]Log, 0)
	this.Logs = append(this.Logs, Log{
		Term:    0,
		Index:   1,
		Command: "Term 0 Idx 1",
	})
	this.VotedTerm = -1

	for i := 0; i < this.NumPeers; i++ {
		this.NextIndicies[peers[i]] = 1
		this.LastLogIndicies[peers[i]] = 0
	}

	this.CommitIdx = 0

	var min, max int = 3, 15
	this.Timeout = rand.Intn(max-min) + min

	this.Peers = make([]string, len(peers))
	for i := 0; i < len(peers); i++ {
		this.Peers[i] = peers[i]
	}

	this.lastHBtime = time.Now()
	this.lastVRtime = time.Now()
	this.counter = 1
}

func (this *Actor) InitFromConfigFile(filename string) {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		panic(err)
	}
	payload := ActorConfig{}
	err = json.Unmarshal(data, &payload)
	if err != nil {
		panic(err)
	}

	this.NumPeers = payload.NumPeers
	this.CommitIdx = payload.CommitIdx
	this.Peers = make([]string, len(payload.Peers))
	this.ID = payload.ID

	for i := 0; i < len(payload.Peers); i++ {
		this.Peers[i] = payload.Peers[i]
	}

	this.CurrentTerm = payload.CurrentTerm
	this.Logs = DeepCopyLogs(payload.Logs)
	this.Timeout = payload.Timeout

	if strings.Compare(payload.Role, "leader") == 0 {
		this.Role = Leader
	} else if strings.Compare(payload.Role, "candidate") == 0 {
		this.Role = Candidate
	} else {
		this.Role = Follower
	}

	this.VotedTerm = -1

	this.NextIndicies = make(map[string]int)
	this.LastLogIndicies = make(map[string]int)

	for i := 0; i < len(payload.Peers); i++ {
		this.NextIndicies[payload.Peers[i]] = payload.NextIndices[i]
		this.LastLogIndicies[payload.Peers[i]] = payload.LastLogIndices[i]

	}

	this.AppendCounter = make([]int, len(this.Logs))

	this.lastHBtime = time.Now()
	this.lastVRtime = time.Now()
	this.counter = 1
}

func (this *Actor) CheckPrev(index, term int) bool {

	if this.Role != Follower {
		panic(errors.New("trying to check prev as non-follower"))
	}

	if index > len(this.Logs) {
		return false
	} else if index == 0 {
		return len(this.Logs) == 0
	} else if len(this.Logs) == 0 {
		return true
	}

	log := this.Logs[index-1]
	return log.Term == term

}

func (this *Actor) PrintLeaderState() {
	log.Println("Leader maintained next indices:")
	for k, v := range this.NextIndicies {
		log.Printf("[%s, %d]\n", k, v)
	}

	log.Println("Leader maintained last log indices:")
	for k, v := range this.LastLogIndicies {
		log.Printf("[%s, %d]\n", k, v)
	}

}

func (this *Actor) PrintLogs() {
	this.mu.Lock()
	defer this.mu.Unlock()
	for i := 0; i < len(this.Logs); i++ {
		log.Println(this.Logs[i].ToStr())
	}
}

func (this *Actor) HandleAppendEntriesRPC(rpc *AppendEntriesRPC) AppendResp {
	this.mu.Lock()
	defer this.mu.Unlock()

	// validity check ?
	if this.Role == Candidate {
		log.Printf("Server %s: received append entry rpc, candidate -> follower\n", this.ID)
		this.Role = Follower
	} else if this.Role == Leader {
		panic(errors.New("trying to append entry as leader"))
	}

	// reset heart beat timer
	this.lastHBtime = time.Now()

	if rpc.Term < this.CurrentTerm {
		log.Println("rpc term less than current term, refuse")
		return AppendResp{this.CurrentTerm, false}
	}

	if len(rpc.Entries) == 0 {
		log.Println("heartbeat received")
		return AppendResp{-1, false}
	}

	// return failure if log does not contain an entry at prevLogIndex whose
	//  term matches prevLogTerm
	if !this.CheckPrev(rpc.PrevLogIndex, rpc.PrevLogTerm) {

		log.Println("prev index does not match, refuse")
		return AppendResp{this.CurrentTerm, false}
	}

	if rpc.Term >= this.CurrentTerm {
		this.lastHBtime = time.Now()
		this.CurrentTerm = rpc.Term
		this.VotedTerm = 0
	}

	if rpc.PrevLogIndex < len(this.Logs)-1 {
		this.Logs = this.Logs[:rpc.PrevLogIndex]
	}
	this.Logs = append(this.Logs, DeepCopyLogs(rpc.Entries)...)

	log.Println("append rpc success")
	return AppendResp{this.CurrentTerm, true}

}

// leader reaction to follower responses for append rpcs
func (this *Actor) HandleResp(newPrevIdx int, resp *RespPayload) bool {

	this.mu.Lock()
	defer this.mu.Unlock()

	if this.Role != Leader {
		panic(errors.New("Trying to handle resp as non-leader"))
	}

	// special value -1 for responses to heartbeat messages
	if resp.Body.Term == -1 {
		return true
	}

	if resp.Body.Success == false {
		this.NextIndicies[resp.PeerID] -= 1
		return false
	}

	return true
}

// receive client command string and append to own log
func (this *Actor) ReceiveClientRequest(req string) {
	this.mu.Lock()
	defer this.mu.Unlock()

	if this.Role != Leader {
		panic(errors.New("trying to process client command as non-leader"))
	}

	new_log := Log{this.CurrentTerm, len(this.Logs) + 1, req}
	this.Logs = append(this.Logs, new_log)
}

func (this *Actor) HandleVoteReq(rpc *VoteReqRPC) VoteRsp {
	this.mu.Lock()
	defer this.mu.Unlock()

	if this.Role != Follower {
		log.Printf("Server %s: not follower so do not give vote\n", this.ID)
		return VoteRsp{this.VotedTerm, false}
	}

	if this.VotedTerm < rpc.Voteterm {
		this.VotedTerm = rpc.Voteterm
		this.lastVRtime = time.Now()
	} else {
		return VoteRsp{this.VotedTerm, false}
	}

	if this.CurrentTerm > rpc.Term {
		return VoteRsp{this.VotedTerm, false}
	} else {
		this.lastVRtime = time.Now()
	}

	if len(this.Logs) <= rpc.LastLogIndex+1 {
		return VoteRsp{this.VotedTerm, true}
	} else {
		return VoteRsp{this.VotedTerm, false}
	}

}
