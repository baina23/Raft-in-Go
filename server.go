package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"log"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"runtime"
	"time"

	"github.com/go-co-op/gocron"
)

// global variable that holds all information about the state machine
//  see actor.go for implementation details
var server Actor

const HB_EXPIRED_TIME = 4
const VTERM_EXPIRED_TIME = 4

// this function implements the follower behavior to AppendEntriesRPC
func handleAppendRPC(w http.ResponseWriter, req *http.Request) {

	log.Println("Trying to handle RPC")

	// extract rpc struct from json payload in request body
	//  assuming the leader sends a post request
	var rpc AppendEntriesRPC
	err := json.NewDecoder(req.Body).Decode(&rpc)

	log.Println("Append RPC received:")
	rpc.Print()
	rpc.PrintEntries()

	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application-json")
	w.WriteHeader(http.StatusCreated)

	// advance state machine and get response
	resp := server.HandleAppendEntriesRPC(&rpc)

	log.Println("Follower log after rpc:")
	server.PrintLogs()
	log.Printf("%d, %t\n", resp.Term, resp.Success)

	// return response to leader as json
	json.NewEncoder(w).Encode(resp)
}

// this route is used by leader to get client request in the form of string
//  and append to its own logs. These will be appended to follower afterwards.
//   See AppendRpcTask for more detail
func handleClientReq(w http.ResponseWriter, req *http.Request) {
	log.Println("Trying to handle client request")
	var creq ClientRequest

	err := json.NewDecoder(req.Body).Decode(&creq)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	server.ReceiveClientRequest(creq.Req)
	w.WriteHeader(http.StatusOK)

	resp_json := make(map[string]string)
	resp_json["message"] = "Success"
	http_resp, err := json.Marshal(resp_json)
	if err != nil {
		log.Println("json marshal error")
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.Write(http_resp)
}

func handleVoteReq(w http.ResponseWriter, req *http.Request) {
	log.Println("Trying to handle Vote Request from Follower side")

	var vreq VoteReqRPC

	err := json.NewDecoder(req.Body).Decode(&vreq)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	log.Printf("Server ID %s: Received Vote Request from server: %s\n", server.ID, vreq.CandidateID)

	w.Header().Set("Content-Type", "application-json")
	w.WriteHeader(http.StatusCreated)

	resp := server.HandleVoteReq(&vreq)
	log.Printf("Server ID %s: Vote result: %t\n", server.ID, resp.VoteGranted)
	// return response to leader as json
	json.NewEncoder(w).Encode(resp)
}

func FollowerTask(server *Actor) {
	server.mu.Lock()
	defer server.mu.Unlock()
	// Transition from follower to candidate (if not heartbeat received and no vote term started at current term)

	if server.Role != Follower {
		return
	}

	HBtimeout := time.Since(server.lastHBtime).Seconds()
	VRtimeout := time.Since(server.lastVRtime).Seconds()
	if HBtimeout > float64(server.Timeout) {
		if server.VotedTerm == 0 || VRtimeout > VTERM_EXPIRED_TIME {

			log.Printf("Server ID %s: Follower -> Candidate\n", server.ID)
			server.Role = Candidate
			server.VotedTerm++
		}
	}
}

func CandidateTask(server *Actor) {
	// lock to avoid dirty data
	server.mu.Lock()
	defer server.mu.Unlock()
	// only candidate confirm if it is selected
	if server.Role != Candidate {
		return
	}
	http_client := &http.Client{Timeout: 1 * time.Second}

	// broadcast request vote rpc
	for i := 0; i < server.NumPeers; i++ {
		peer_id := server.Peers[i]

		req := VoteReqRPC{
			CandidateID:  server.ID,
			Term:         server.CurrentTerm,
			Voteterm:     server.VotedTerm + 1,
			LastLogIndex: server.Logs[len(server.Logs)-1].Index,
			LastLogTerm:  server.Logs[len(server.Logs)-1].Term}

		req_json, er := json.Marshal(req)
		if er != nil {
			log.Println("marshal failed")
		}
		r, http_err := http_client.Post("http://localhost:"+peer_id+"/request-vote", "application/json", bytes.NewBuffer(req_json))

		if http_err == nil {
			var vote_rsp VoteRsp
			err := json.NewDecoder(r.Body).Decode(&vote_rsp)

			if err != nil {
				log.Println("json parse error for response body")
				return
			}
			if vote_rsp.VoteGranted == true {
				server.counter++
				if server.counter > server.NumPeers/2 {
					break
				}
			} else {
				if vote_rsp.Term >= server.VotedTerm { // my voteterm is not up-to-date
					break
				}
			}
			r.Body.Close()
		}
	}
	log.Printf("Server %s: counter: %d\n", server.ID, server.counter)
	// now the candidate becomes leader, need to send out heartbeat messages
	if server.counter > server.NumPeers/2 {

		// since it is now elected as leader, need to increase term by 1
		server.Role = Leader
		server.CurrentTerm++
		server.VotedTerm = 0

		log.Printf("Server ID %s is elected leader of term %d\n", server.ID, server.CurrentTerm)
	} else {

		// not elected as leader, fall back to follower and re-compute a timeout value
		log.Printf("Server ID %s: Candidate -> Follower\n", server.ID)
		server.Role = Follower
		server.Timeout = rand.Intn(15-3) + 3
		log.Printf("Server %s: new timeout value: %d\n", server.ID, server.Timeout)
	}
}

// async task run by the scheduler for every fixed amount of seconds.
//  The interval can be adjusted in main function
// This task implements the process of leader appending logs to its
//  followers
func LeaderTask(server *Actor) {

	// lock to avoid dirty data
	server.mu.Lock()
	defer server.mu.Unlock()

	// this is used for sending post requests to followers
	http_client := &http.Client{Timeout: 10 * time.Second}

	// follower and candidate should not use this
	if server.Role != Leader {
		return
	}

	// for each peer (follower), try to append entries
	for i := 0; i < server.NumPeers; i++ {
		peer_id := server.Peers[i]

		if server.NextIndicies[peer_id] > server.LastLogIndicies[peer_id] && server.NextIndicies[peer_id]-1 < len(server.Logs) {
			log.Println("trying to append entry to follower...")
			var last_term int
			if server.NextIndicies[peer_id]-1 <= 0 {
				last_term = 0
			} else {
				last_term = server.Logs[server.NextIndicies[peer_id]-2].Term
			}
			// build payload and send post request
			append_rpc := AppendEntriesRPC{
				Term:         server.Logs[server.NextIndicies[peer_id]-1].Term,
				LeaderId:     server.ID,
				PrevLogIndex: server.NextIndicies[peer_id] - 1,
				PrevLogTerm:  last_term,
				Entries:      []Log{server.Logs[server.NextIndicies[peer_id]-1]},
				CommitIndex:  server.CommitIdx}

			// TODO: add error handling
			rpc_json, er := json.Marshal(append_rpc)
			if er != nil {
				log.Println("marshal failed")
			}
			r, http_err := http_client.Post("http://localhost:"+peer_id+"/append-entry-rpc", "application/json", bytes.NewBuffer(rpc_json))

			if http_err == nil && r != nil && r.Body != nil {

				var follower_resp AppendResp
				err := json.NewDecoder(r.Body).Decode(&follower_resp)

				if err != nil {
					log.Println("json parse error for response body")
					return
				}

				if !follower_resp.Success {
					log.Println("append failed, decrease next index by one")

					// if append fails, we roll back next index for that peer by one so that
					//  next time the leader will try to append the previous log
					server.NextIndicies[peer_id] = MaxInt(0, server.NextIndicies[peer_id]-1)
				} else {
					log.Println("append success")

					// if append succeeded, update next index and last log index accordingly
					server.LastLogIndicies[peer_id] = server.NextIndicies[peer_id]
					server.NextIndicies[peer_id] += 1

					// TODO: handle commit index here
				}
				r.Body.Close()
				server.PrintLeaderState()
			} else {
				log.Printf("Server ID %s: http request for AppendEntryRPC to follower %s failed\n", server.ID, peer_id)
			}
		} else {
			heartbeat := AppendEntriesRPC{
				Term:         server.CurrentTerm,
				LeaderId:     server.ID,
				PrevLogIndex: server.NextIndicies[peer_id] - 1,
				PrevLogTerm:  server.CurrentTerm - 1,
				Entries:      []Log{},
				CommitIndex:  server.CommitIdx}

			heartbeat_json, er := json.Marshal(heartbeat)
			if er != nil {
				log.Println("marshal failed")
			}
			log.Printf("Server ID %s: trying to send heartbeat to server %s\n", server.ID, peer_id)
			r, http_err := http_client.Post("http://localhost:"+peer_id+"/append-entry-rpc", "application/json", bytes.NewBuffer(heartbeat_json))

			if http_err == nil && r != nil && r.Body != nil {
				log.Printf("Server ID %s: heartbeat to server %s success\n", server.ID, peer_id)
				r.Body.Close()
			} else {
				log.Printf("Server ID %s: heartbeat to server %s failed\n", server.ID, peer_id)
			}
		}
	}

}

// check if a flag is set
// https://stackoverflow.com/questions/35809252/check-if-flag-was-provided-in-go
func isFlagPassed(name string) bool {
	found := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == name {
			found = true
		}
	})
	return found
}

func main() {

	// for blocking time profiling
	runtime.SetBlockProfileRate(1)

	// command line argument parsing
	port := flag.String("p", "8090", "port number")
	num_peers := flag.Int("n", 0, "#peers")
	config_file := flag.String("f", "actor_config/follower1.json", "config file name")
	flag.Parse()
	peers := flag.Args()

	if isFlagPassed("f") {
		server.InitFromConfigFile(*config_file)
	} else {
		if !isFlagPassed("p") || !isFlagPassed("n") || *num_peers != len(peers) {
			log.Fatal("Command argument format error")
		}
		server.Init(*port, *num_peers, peers)
	}

	log.Printf("Starting server at port %s with %d peers\n", server.ID, server.NumPeers)
	if server.Role == Leader {
		server.PrintLeaderState()
	}

	// register http handlers
	http.HandleFunc("/client-api", handleClientReq)
	http.HandleFunc("/append-entry-rpc", handleAppendRPC)
	http.HandleFunc("/request-vote", handleVoteReq)

	// use scheduler for sending rpcs
	scheduler := gocron.NewScheduler(time.UTC)

	// change the number here to modify the interval
	scheduler.Every(3).Seconds().Do(LeaderTask, &server)
	scheduler.Every(3).Seconds().Do(FollowerTask, &server)
	scheduler.Every(3).Seconds().Do(CandidateTask, &server)
	scheduler.StartAsync()

	http.ListenAndServe(":"+server.ID, nil)
}
