// بِسْمِ ٱللَّٰهِ ٱلرَّحْمَٰنِ ٱلرَّحِيمِ

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
	"labrpc"
	"math"
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
	UseSnapshot bool   // ignore for Assignment2; only used in Assignment3
	Snapshot    []byte // ignore for Assignment2; only used in Assignment3
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // --> used for locking & unlocking for data structures that will be using it alot BECAREFUL FOR THIS
	peers     []*labrpc.ClientEnd // --> array containing nodes via sockets to identify which server is in our positions
	persister *Persister
	me        int // index into peers[]
	apply_msg chan ApplyMsg

	// Your data here.
	state                 string
	vote_count            int
	term                  int
	voted_for             int
	Logs                  []Log_array
	commit_index          int
	last_applied          int
	send_chan             chan ApplyMsg
	send_follower         chan ApplyMsg
	return_lead_apply     chan string
	return_follower_apply chan string
	// Leader's index :

	nextindex   map[int]int
	match_index map[int]int

	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// create your own channels

	append_to_election chan string // --> used to communicate between append go routine to election go routine
	vote_to_election   chan string // --> used for communication b/w vote_request and election go routine

}

// return currentTerm and whether this server
// believes it is the leader. we will query the servers to get its state during the program
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.
	isleader = false
	rf.mu.Lock()
	if rf.state == "leader" {
		isleader = true
	}
	term = rf.term
	rf.mu.Unlock()

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// ////////rf.persister.SaveRaftState(data)

	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.term)
	// e.Encode(rf.voted_for)
	// e.Encode(rf.Logs)

	// data := w.Bytes()
	//////rf.persister.SaveRaftState(data)

}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)

	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// rf.mu.Lock()
	// current_term := rf.term
	// my_logs := rf.Logs
	// my_votes := rf.voted_for

	// rf.mu.Unlock()
	// d.Decode(&current_term)
	// d.Decode(&my_logs)
	// d.Decode(&my_votes)

}

// example RequestVote RPC arguments structure.
type RequestVoteArgs struct {
	// Your data here.
	Sender_term int
	Sender_id   int
	Candidate   []Log_array
}

// example RequestVote RPC reply structure.
type RequestVoteReply struct {
	// Your data here.
	Reciever_term         int
	Reciever_vote_granted bool
	Candidate_log         []Log_array
}

type Append_call struct {
	Leader_term    int
	Prev_log_index int
	Prev_log_term  int
	Entries        []Log_array
	Leader_commit  int
}

type Append_call_response struct {
	Reciever_term int
}

type Log_array struct {
	Index   int
	Term    int
	Command interface{}
}

// example RequestVote RPC handler. all the handling will be done at this , just need to call it.
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {

	// Your code here.
	rf.mu.Lock()
	if args.Sender_term >= rf.term {
		if (len(rf.Logs) == 0 && len(args.Candidate) == 0) || (len(rf.Logs) == 0 && len(args.Candidate) > 0) || (len(args.Candidate) > 0 && (args.Candidate[len(args.Candidate)-1].Term > rf.Logs[len(rf.Logs)-1].Term)) || (len(args.Candidate) > 0 && (args.Candidate[len(args.Candidate)-1].Term == rf.Logs[len(rf.Logs)-1].Term) && len(args.Candidate) >= len(rf.Logs)) {

			rf.state = "follower"
			rf.vote_count = 0
			rf.voted_for = args.Sender_id
			rf.term = args.Sender_term
			rf.vote_to_election <- "reset"
			reply.Reciever_term = rf.term
			reply.Reciever_vote_granted = true
			// //////rf.persist()
		} else {
			reply.Reciever_term = rf.term
			reply.Reciever_vote_granted = false
		}
	} else {
		reply.Reciever_term = rf.term
		reply.Reciever_vote_granted = false
	}

	rf.mu.Unlock()
	//////rf.persist()

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
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {

	rf.mu.Lock()
	peer := rf.peers[server]
	rf.mu.Unlock()
	ok := peer.Call("Raft.RequestVote", args, reply)

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
	index := -1
	term := -1
	isLeader := true

	rf.mu.Lock()
	if rf.state == "leader" {

		// fmt.Println("COMMAND FOR THIS ENTRY IS : ", command, " with the index : ", len(rf.Logs)+1, rf.me)
		// fmt.Println("My commit id is : ", rf.commit_index)
		// fmt.Println("my name is : ", rf.me)
		index = len(rf.Logs) + 1
		term = rf.term
		rf.Logs = append(rf.Logs, Log_array{index, term, command})

	} else {
		isLeader = false
	}
	rf.mu.Unlock()
	//////rf.persist()

	return index, term, isLeader
}

// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (rf *Raft) Kill() {
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

// we make server
// we initialize it with whatever id we are giving
// similar to startmodel we will exit and let other go routines handle
// create relevant channels
// error is handling

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	//////rf.persister = persister
	rf.me = me
	rf.apply_msg = applyCh
	rf.send_chan = make(chan ApplyMsg, 2)
	rf.send_follower = make(chan ApplyMsg, 2)
	rf.return_lead_apply = make(chan string)
	rf.return_follower_apply = make(chan string)
	// Your initialization code here.

	rf.state = "follower"
	rf.vote_count = 0
	rf.term = 0
	rf.voted_for = -1
	rf.append_to_election = make(chan string)
	rf.vote_to_election = make(chan string)
	rf.Logs = []Log_array{}
	rf.nextindex = make(map[int]int)
	rf.match_index = make(map[int]int)
	rf.commit_index = -1
	rf.last_applied = -1

	for i := 0; i < len(rf.peers); i++ {
		rf.nextindex[i] = -1
		rf.match_index[i] = -1
	}

	// Initializing both next and match index :

	// assigmnment 3.1 structs :

	// LAUNCHING OUR GO-ROUTINES HERE :
	go rf.electionTimer()
	go rf.AppendEntryCaller()
	go rf.send_msg()
	go rf.send_follower_msg()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

// 1) Election timeout go-routine : used for deciding when to have elections by a server :
func (rf *Raft) electionTimer() {

	// creating a random timer
	// then using append and vote channels to reset it when needed

	election_tmr := time.NewTimer(rf.ranTimeGenerator())

	for {

		select {

		case <-rf.append_to_election:
			election_tmr.Reset(rf.ranTimeGenerator())

		case <-rf.vote_to_election:
			election_tmr.Reset(rf.ranTimeGenerator())

		case <-election_tmr.C:
			election_tmr.Reset(rf.ranTimeGenerator())

			rf.mu.Lock()
			temp_state := rf.state
			rf.mu.Unlock()
			if temp_state != "leader" {
				go rf.elections()
			}

		}

	}

}

// 2) Generates a random time b/w 250 and 399

func (rf *Raft) ranTimeGenerator() time.Duration {

	ran_time := (rand.Intn(400-250) + 350)
	return time.Duration(ran_time) * time.Millisecond

}

// 3) Generates an election function :

func (rf *Raft) elections() {

	rf.mu.Lock()
	rf.term = rf.term + 1
	rf.state = "candidate"
	rf.vote_count = 0
	rf.vote_count = rf.vote_count + 1
	rf.voted_for = rf.me
	temp_my_term := rf.term
	temp_my_state := rf.state
	my_id := rf.me

	rf.mu.Unlock()
	//////rf.persist()

	// send votes to everyone other than yourself.

	for i := 0; i < len(rf.peers); i++ {

		if i != my_id {
			go rf.sendVoteToPeer(i, temp_my_term, temp_my_state)
		}

	}

}

// 4) Sends & handles vote request to each peer other than us

func (rf *Raft) sendVoteToPeer(peer_id int, temp_my_term int, temp_my_state string) {

	vote_args := RequestVoteArgs{}
	vote_reply := RequestVoteReply{}

	rf.mu.Lock()
	vote_args.Sender_id = rf.me
	vote_args.Candidate = rf.Logs
	rf.mu.Unlock()
	vote_args.Sender_term = temp_my_term

	vote_reply.Reciever_term = -1
	vote_reply.Reciever_vote_granted = false

	//fmt.Println("send vote req to ", peer_id, "requestvote args: ", vote_args)
	flag := rf.sendRequestVote(peer_id, vote_args, &vote_reply)

	if flag {

		rf.mu.Lock()
		// defer //////rf.persist()
		defer rf.mu.Unlock()

		if rf.term < vote_reply.Reciever_term {
			rf.term = vote_reply.Reciever_term
			rf.state = "follower"
			rf.vote_count = 0
			rf.voted_for = -1
			// //////rf.persist()
			return
		}

		if rf.state == "follower" || rf.state == "leader" {
			return

			// if my term has incremented to higher lvl than that of reply then ignore
		} else if rf.term > vote_reply.Reciever_term {
			return

		} else {
			// now deciding the course of actions to according to the reply.

			//1)
			if vote_reply.Reciever_term > rf.term {
				rf.term = vote_reply.Reciever_term
				rf.state = "follower"
				rf.vote_count = 0
				rf.voted_for = -1
				// //////rf.persist()

			} else if vote_reply.Reciever_term == rf.term {

				if vote_reply.Reciever_vote_granted == true {

					rf.vote_count = rf.vote_count + 1
					if rf.vote_count >= int(math.Ceil(float64(len(rf.peers))/float64(2))) {
						rf.state = "leader"
						rf.vote_count = 0
						rf.voted_for = -1
						// //////rf.persist()
						// initializing the next and map index :

						for i := 1; i < len(rf.peers); i++ {

							rf.nextindex[i] = len(rf.Logs)
							rf.match_index[i] = 0

						}

					}
				}

			}

		}
		return
	}

}

// 5) Processes the execution of append_entry_call by a leader :

func (rf *Raft) AppendEntryHandler(args Append_call, reply *Append_call_response) {

	rf.mu.Lock()
	second_flag := false
	temp_commit := rf.commit_index
	temp_app := rf.last_applied
	if args.Leader_term >= rf.term && ((len(rf.Logs) == 0 && len(args.Entries) == 0) || (len(rf.Logs) == 0 && len(args.Entries) > 0) || (len(args.Entries) > 0 && (args.Entries[len(args.Entries)-1].Term >= rf.Logs[len(rf.Logs)-1].Term)) || (len(args.Entries) > 0 && ((args.Entries[len(args.Entries)-1].Term == rf.Logs[len(rf.Logs)-1].Term) && len(args.Entries) >= len(rf.Logs)))) {

		rf.state = "follower"
		rf.term = args.Leader_term
		rf.vote_count = 0
		rf.voted_for = -1
		rf.append_to_election <- "reset"
		reply.Reciever_term = rf.term
		rf.commit_index = args.Leader_commit
		rf.Logs = args.Entries
		temp_commit = rf.commit_index
		// //////rf.persist()
		// CHECKS FOR ASSIGNMENT 3.1 BEGINS :

		// 1) CHECKING IF THE PREV INDEX ISNT "-1" :

		rf.commit_index = args.Leader_commit

		if len(args.Entries) > 0 {

			// fmt.Println("TARGET COMMIT IS : ", target_commit, las_app_indc)

			second_flag = true
			temp_app = rf.last_applied
			if (temp_app + 1) <= temp_commit {
				rf.last_applied = temp_commit
			}
		}
	} else {
		reply.Reciever_term = rf.term
	}
	rf.mu.Unlock()

	if second_flag {

		for i := (temp_app + 1); i <= temp_commit; i++ {

			new_apply_msg := ApplyMsg{}
			new_apply_msg.Index = args.Entries[i].Index
			new_apply_msg.Command = args.Entries[i].Command
			rf.send_follower <- new_apply_msg
			select {
			case <-rf.return_follower_apply:
				continue
			}

		}
		////////rf.persist()
	}
}

// 6) Performs heart beats to each peer other than yourself periodically :

func (rf *Raft) AppendEntryCaller() {

	heart_beat_tmr := time.NewTimer(rf.heartBeatTimer())

	for {

		select {

		case <-heart_beat_tmr.C:
			heart_beat_tmr.Reset(rf.heartBeatTimer())

			rf.mu.Lock()
			temp_state := rf.state
			rf.mu.Unlock()
			if temp_state == "leader" {
				go rf.conductHeartBeats()
			}

		}
	}

}

// 7) Heart_beat's timer :

func (rf *Raft) heartBeatTimer() time.Duration {

	ran_time := (rand.Intn(130-0) + 0)
	return time.Duration(ran_time) * time.Millisecond

}

// 8) sends heart_beats to every peer :

func (rf *Raft) conductHeartBeats() {

	// creating channel to recieve the votes back :

	rf.mu.Lock()

	temp_my_term := rf.term
	my_id := rf.me

	entries := rf.Logs
	leader_commit := rf.commit_index
	Prev_log_index := len(rf.Logs) - 1
	Prev_log_term := -1

	if Prev_log_index < 0 {

		Prev_log_term = -1

	} else {

		Prev_log_term = rf.Logs[Prev_log_index].Term

	}

	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {

		if i != my_id {
			go rf.sendAndManageHeartBeat(i, temp_my_term, entries, leader_commit, Prev_log_index, Prev_log_term)
		}

	}

}

// 9) truly sends heart beat to a single server and processes it :

func (rf *Raft) sendAndManageHeartBeat(id int, leader_term int, entries []Log_array, leader_commit int, Prev_log_index int, Prev_log_term int) {

	append_args := Append_call{}
	append_reply := Append_call_response{}
	second_flag := false
	// third_flag := false
	append_args.Leader_term = leader_term
	append_reply.Reciever_term = -1
	append_args.Leader_commit = leader_commit
	append_args.Prev_log_index = Prev_log_index
	append_args.Prev_log_term = Prev_log_term
	append_args.Entries = entries

	flag := rf.sendHeartBeat(id, append_args, &append_reply)

	if flag {

		rf.mu.Lock()

		target_commit := rf.commit_index
		las_app_indc := rf.last_applied

		if append_reply.Reciever_term > rf.term {

			rf.state = "follower"
			rf.term = append_reply.Reciever_term
			rf.vote_count = 0
			rf.voted_for = -1
			// ////////rf.persist()

		} else if rf.state == "leader" {

			// ASSIGNMENT 3.1 => UPDATING THE NEXT AND MATCH HASHTABLES :

			if rf.match_index[id] < len(entries) && len(entries) != 0 {

				rf.match_index[id] = len(entries) - 1
				rf.match_index[rf.me] = len(entries) - 1
				rf.nextindex[id] = len(entries)

				// calling update commit index function : if commit index < leader's length of log send by the leader to servers :

				if rf.commit_index <= len(entries)-1 {

					// fmt.Println("YESSS HERE IS THE CLUE : ")
					target_commit = rf.updateCommit()

					if target_commit != -1 && rf.Logs[target_commit].Term == rf.term {
						// fmt.Println("TARGET COMMIT IS : ", target_commit, las_app_indc)
						rf.commit_index = target_commit
						// ////////rf.persist()
						second_flag = true
						las_app_indc = rf.last_applied
						if (las_app_indc + 1) <= target_commit {
							rf.last_applied = target_commit
						}
					}

				}
			}
		}
		rf.mu.Unlock()

		if second_flag == true {

			for i := (las_app_indc + 1); i <= target_commit; i++ {
				new_apply_msg := ApplyMsg{}
				new_apply_msg.Index = entries[i].Index
				new_apply_msg.Command = entries[i].Command
				rf.send_chan <- new_apply_msg
				select {

				case <-rf.return_lead_apply:
					continue
				}
			}
			////////rf.persist()
		}
	}
}

func (rf *Raft) sendHeartBeat(server int, args Append_call, reply *Append_call_response) bool {
	rf.mu.Lock()
	peer := rf.peers[server]
	rf.mu.Unlock()

	ok := peer.Call("Raft.AppendEntryHandler", args, reply)
	return ok
}

func (rf *Raft) send_msg() {

	for {
		select {

		case sample, _ := <-rf.send_chan:
			// fmt.Println("CMD TO APPLY CHAN : ", sample.Command, " Index is : ", sample.Index, rf.me, " Apply INDEX IS : ", rf.last_applied)
			rf.apply_msg <- sample
			rf.return_lead_apply <- "ok"
			// ////////rf.persist()
		}
		// sample, _ := <-rf.send_chan
		// fmt.Println("CMD TO APPLY CHAN : ", sample.Command, " Index is : ", sample.Index, rf.me)
		// rf.apply_msg <- sample
	}

}

func (rf *Raft) send_follower_msg() {

	for {
		select {

		case sample, _ := <-rf.send_follower:

			rf.apply_msg <- sample
			rf.return_follower_apply <- "ok"
			// //////rf.persist()

		}
		// sample, _ := <-rf.send_chan
		// fmt.Println("CMD TO APPLY CHAN : ", sample.Command, " Index is : ", sample.Index, rf.me)
		// rf.apply_msg <- sample
	}

}

func (rf *Raft) updateCommit() int {

	// fmt.Println("YESSS HEEHEHHEEEHEHEHE", (len(rf.peers))/2)
	// checked_index := make(map[int]bool)
	count := 0
	new_commit := -1

	// for i := 0; i < (len(rf.peers)); i++ {
	// 	fmt.Println("Match index of i is ", i, " with the val : ", rf.match_index[i])

	// }
	// fmt.Printf("________")

	// fmt.Println("condition is : ", len(rf.peers)/2)

	for i := 0; i < (len(rf.peers)); i++ {

		count = 0
		// fmt.Println(("wowowowow"))
		// if checked_index[rf.match_index[i]] {

		// 	continue

		// } else {

		target_index := rf.match_index[i]

		for j := 0; j < len(rf.peers); j++ {
			// fmt.Println("condition is match index : ", rf.match_index, " : ", target_index, "^^^^^^^^^^")
			if rf.match_index[j] == target_index {
				count = count + 1

			}
		}

		if count >= ((len(rf.peers))/2)+1 {
			// fmt.Println("Count gained for the prev commit id is : ", count, "   :  ", rf.commit_index, "target index is : ", target_index)

			//fmt.Println("________________ value is ", target_index, " : ", count, "_____________")
			new_commit = target_index
			// fmt.Println("____NEW COMMIT IS : _____", new_commit, " length of log is : ", len(rf.Logs))
			// // fmt.Println("NEW COMMIT IS : ", new_commit, " length of log is : ", len(rf.Logs), " the numb of counts : ", count)
			// if new_commit > rf.commit_index {
			// 	fmt.Println("____NEW COMMIT IS : _____", new_commit, " length of log is : ", len(rf.Logs))
			// }
			break

		}

	}

	return new_commit

}
