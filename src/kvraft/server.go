// بِسْمِ ٱللَّٰهِ ٱلرَّحْمَٰنِ ٱلرَّحِيمِ

package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	Key   string
	Value string
	Type  string // to find the type of msg
	Index int    // using to send msgs -> to all channels
	// Term      int // just in case
	Client_id int // to update data structure in all servers
	Txn_id    int // "" "" ""
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

	// Structure 1 : structure : {	client id : txn #	}
	client_txn_map map[int]int

	// Structure 2 : structure : {	index	: [channel 1 , channel 2 , channel 3 .... ]		}
	indx_channels_map map[int][]chan Op

	// Structure 3 : structure : { key : value }
	kv_store map[string]string
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	// Checking if whether I am leader or not :
	_, status := kv.rf.GetState()
	apply_flag := false

	if status == false {
		reply.WrongLeader = true
	} else {

		// Checking if the Client ID exists in the map or not & finding the value :

		kv.mu.Lock()

		txn_id, ok := kv.client_txn_map[args.Client_id]

		if !ok {

			apply_flag = true

		} else {

			// checking whether transaction is repeated or not :

			if txn_id < args.Txn_id {

				apply_flag = true

			} else {

				apply_flag = false
				reply.Err = "Dup_req"
				reply.Updated_txn_id = args.Txn_id + 1
				value_present, ok := kv.kv_store[args.Key]

				if ok {
					reply.Value = value_present
					reply.WrongLeader = false

				} else {

					reply.Value = ""
					reply.WrongLeader = false

				}

			}
		}
		kv.mu.Unlock()
	}

	// Creating OP structure , channel , and select statement for apply :

	if apply_flag {

		// Creating OP structure :

		op_command := Op{}
		op_command.Client_id = args.Client_id
		op_command.Txn_id = args.Txn_id
		op_command.Key = args.Key
		op_command.Type = "Get"
		op_command.Value = ""

		// creating personal channel :

		local_chan := make(chan Op)

		target_index, _, _ := kv.rf.Start(op_command)

		// storing the channel to the map :

		kv.mu.Lock()

		kv.indx_channels_map[target_index] = append(kv.indx_channels_map[target_index], local_chan)

		kv.mu.Unlock()

		// Waiting for the reply ... :

		select {

		case output_command := <-local_chan:

			if output_command.Client_id == op_command.Client_id && output_command.Txn_id == op_command.Txn_id && output_command.Key == op_command.Key {

				kv.mu.Lock()

				reply.Updated_txn_id = args.Txn_id + 1

				value_present, ok := kv.kv_store[args.Key]

				if ok {
					reply.Value = value_present
					reply.Err = OK
					reply.WrongLeader = false

				} else {

					reply.Value = ""
					reply.Err = ErrNoKey
					reply.WrongLeader = false

				}

				kv.mu.Unlock()

			} else {

				reply.Updated_txn_id = args.Txn_id
				reply.WrongLeader = true

			}

		}

	}

}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	// Checking if whether I am leader or not :
	_, status := kv.rf.GetState()
	apply_flag := false

	if status == false {
		reply.WrongLeader = true
	} else {

		// Checking if the Client ID exists in the map or not & finding the value :

		kv.mu.Lock()

		txn_id, ok := kv.client_txn_map[args.Client_id]

		if !ok {

			apply_flag = true

		} else {

			// checking whether transaction is repeated or not :

			if txn_id < args.Txn_id {

				apply_flag = true
				// reply.Updated_txn_id = args.Txn_id + 1

			} else {

				apply_flag = false
				reply.Err = "Dup_req"
				reply.Updated_txn_id = args.Txn_id + 1

			}
		}
		kv.mu.Unlock()
	}

	// Creating OP structure , channel , and select statement for apply :

	if apply_flag {

		// Creating OP structure :

		op_command := Op{}
		op_command.Client_id = args.Client_id
		op_command.Txn_id = args.Txn_id
		op_command.Key = args.Key
		op_command.Type = args.Op
		op_command.Value = args.Value

		// creating personal channel :

		local_chan := make(chan Op)

		// sending OP STRUCTURE to start/ state machine

		target_index, _, _ := kv.rf.Start(op_command)

		// storing the channel to the map :

		kv.mu.Lock()

		kv.indx_channels_map[target_index] = append(kv.indx_channels_map[target_index], local_chan)

		kv.mu.Unlock()

		// Waiting for the reply ... :

		select {

		case output_command := <-local_chan:

			if output_command.Client_id == op_command.Client_id && output_command.Txn_id == op_command.Txn_id && output_command.Key == op_command.Key && output_command.Value == op_command.Value {

				// fmt.Printf("m leader")

				// extracting the value from MAP :

				// storing the new txn number for client into the MAP :

				// kv.client_txn_map[args.Client_id] = args.Txn_id

				reply.Updated_txn_id = args.Txn_id + 1
				reply.Err = OK
				// fmt.Printf("heree")
				reply.WrongLeader = false
				// fmt.Printf("yes")
				// checking what type of OP was given out to us  :
				// fmt.Println("lmaoooooo")

			} else { // meaning some other cmd was used for this case

				reply.Updated_txn_id = args.Txn_id
				reply.WrongLeader = true

			}

		}

	}

}

func (kv *RaftKV) masterGoRoutine() {

	// Objective : loops forever and extracts values out of append index and conveys the msg over to the respective channels
	for {

		select {

		case apply_struct := <-kv.applyCh:

			// getting the index out of the struct and forwarding stuff to the specific channels :

			// fmt.Println("RECIEVED MSG ON SERVER : ", apply_struct)

			kv.mu.Lock()

			target_index := apply_struct.Index

			// Updating the KV store and client structure map on every node :

			op_cmd := apply_struct.Command.(Op)

			// fmt.Println("Output command : ", op_cmd.Type, " with index : ", op_cmd.Index, " my name is : ", kv.me, " with the log index of : ", target_index)

			kv.client_txn_map[op_cmd.Client_id] = op_cmd.Txn_id

			if op_cmd.Type == "Put" {

				// fmt.Println()
				// fmt.Println("TYPE CMD : ", op_cmd.Type)
				// fmt.Println("MY NAME IS : ", kv.me)
				// fmt.Println("YES KEY IS CALLED !")

				kv.kv_store[op_cmd.Key] = op_cmd.Value
				// fmt.Println("YOUR VALUE FOR PUT KEY : ", op_cmd.Key, " : VALUE IS : ", kv.kv_store[op_cmd.Key], "YOUR ORIGINAL VALUE IS : ", op_cmd.Value)
				// fmt.Println("YOUR ORIGINAL VALUE IS : ", op_cmd.Value)

			} else if op_cmd.Type == "Append" {

				// checking if the value exists or not in the kv store :

				value_present, ok := kv.kv_store[op_cmd.Key]

				if ok { // meaning it exists
					// fmt.Println()
					// fmt.Println("TYPE CMD : ", op_cmd.Type)
					// fmt.Println("MY NAME IS : ", kv.me)
					// fmt.Println("YES APPEND IS CALLED !")
					kv.kv_store[op_cmd.Key] = value_present + op_cmd.Value

					// fmt.Println("YOUR VALUE FOR APPEND KEY : ", op_cmd.Key, " : VALUE IS : ", kv.kv_store[op_cmd.Key], "YOUR ORIGINAL VALUE IS : ", op_cmd.Value)
					//fmt.Println("YOUR VALUE FOR APPEND KEY : ", op_cmd.Key, " : VALUE IS : ", kv.kv_store[op_cmd.Key], " my name is : ", kv.me)

				} else {

					kv.kv_store[op_cmd.Key] = op_cmd.Value

				}

			}

			// passing the cmd to every channel :

			list_of_chans, ok := kv.indx_channels_map[target_index]
			kv.mu.Unlock()
			if ok { // means there was a channel :

				// sending stuff to every client in OP format

				for i_index := 0; i_index < len(list_of_chans); i_index++ {

					// fmt.Printf("yesss")

					list_of_chans[i_index] <- op_cmd
				}
				// fmt.Printf("out")

			}

			// Updating the user's txn #

			// Updating the kvstore for put and get req :

		}

	}

}

// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// Your initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// Making Map functions :
	kv.indx_channels_map = make(map[int][]chan Op)
	kv.client_txn_map = make(map[int]int)
	kv.kv_store = make(map[string]string)

	go kv.masterGoRoutine()

	return kv
}
