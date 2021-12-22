package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
)

type AppendEntriesRPC struct {
	Term         int
	LeaderId     string
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	CommitIndex  int
}

type VoteReqRPC struct {
	CandidateID  string
	Term         int
	Voteterm     int
	LastLogIndex int
	LastLogTerm  int
}

type VoteRsp struct {
	Term        int
	VoteGranted bool
}

type ClientRequest struct {
	Req string
}

type AppendResp struct {
	Term    int
	Success bool
}

type RespPayload struct {
	Body   AppendResp
	PeerID string
}

type Responses struct {
	Resps []AppendResp
}

type AppendReqs struct {
	Rpcs []AppendEntriesRPC
}

type Log struct {
	Term    int
	Index   int
	Command string
}

type LeaderLog struct {
	Rpcs  []AppendEntriesRPC
	Resps []AppendResp
}

func (this *AppendEntriesRPC) Print() {
	fmt.Printf("Term: %d, LID: %s, prevIdx: %d, prevLogTerm: %d, commitIdx: %d\n", this.Term, this.LeaderId, this.PrevLogIndex, this.PrevLogTerm, this.CommitIndex)
}

func (this *AppendEntriesRPC) PrintEntries() {
	for i := range this.Entries {
		fmt.Println(this.Entries[i].ToStr())
	}
}

func (this *Log) ToStr() string {
	return fmt.Sprintf("[Term: %d, Index: %d, command: %s]", this.Term, this.Index, this.Command)
}

func DeepCopyLogs(logs []Log) []Log {
	result := make([]Log, len(logs))
	for i := range logs {
		result[i] = Log{}
		result[i].Index = logs[i].Index
		result[i].Term = logs[i].Term
		result[i].Command = logs[i].Command
	}

	return result
}

// Unmarshal byte array into struct holding all the requests, propagate error
func ParseAppendReqFromFile(filename string) (AppendReqs, error) {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return AppendReqs{}, err
	}

	requests := AppendReqs{}
	json.Unmarshal(data, &requests)
	return requests, nil
}

//parse missing leader log
func ParseRPCAndRespFromFile(filename string) (LeaderLog, error) {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return LeaderLog{}, err
	}

	payload := LeaderLog{}
	json.Unmarshal(data, &payload)
	return payload, nil
}

func PrintAppendReqs(data *AppendReqs) {
	for i := 0; i < len(data.Rpcs); i++ {
		req := data.Rpcs[i]
		fmt.Printf("term: %d, leaderId: %s, prevLogIndex: %d, prevLogTerm: %d, commitIndex: %d, entries:\n", req.Term, req.LeaderId, req.PrevLogIndex, req.PrevLogTerm, req.CommitIndex)
		req.PrintEntries()
	}
}

func PrintResps(responses *Responses) {
	for i := range responses.Resps {
		fmt.Printf("{Term: %d, Success: %t}\n", responses.Resps[i].Term, responses.Resps[i].Success)
	}
}
