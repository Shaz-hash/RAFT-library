// بِسْمِ ٱللَّٰهِ ٱلرَّحْمَٰنِ ٱلرَّحِيمِ
package raftkv

import (
	"crypto/rand"
	"labrpc"
	"math/big"
	"sync"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.

	mu             sync.Mutex
	clerk_id       int // client's id
	current_leader int
	txn_id         int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.

	// getting a random identifier for the client
	ck.clerk_id = int(nrand())

	// initializing current leader & txn id :
	// ck.current_leader = -1
	ck.txn_id = 0

	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	// fmt.Println(" _____GET COMMAND IS CALLED_____FOR_______", key)
	answer := ""
	flag := false

	// 1)  looping over all the kv servers till i get a response :

	index := 0

	for flag == false {

		// 2) Creating arguments

		get_args := GetArgs{}
		get_reply := GetReply{}
		// ck.mu.Lock()
		selected_server := ck.servers[index]
		length := len(ck.servers)
		get_args.Client_id = ck.clerk_id
		get_args.Key = key
		get_args.Txn_id = ck.txn_id
		// ck.mu.Unlock()

		// 3) waiting for the reply :

		ok := selected_server.Call("RaftKV.Get", &get_args, &get_reply)

		// 4) checking if the leader has msged me or not :

		if ok {

			// Checking if the reciever was a leader or not :

			if get_reply.WrongLeader == true {

				index = index + 1

			} else {

				// checking if i recieved the reply or not :
				flag = true

				if get_reply.Err == OK {

					// ck.mu.Lock()
					ck.txn_id = get_reply.Updated_txn_id
					// ck.mu.Unlock()
					answer = get_reply.Value
					// fmt.Println("REPLY FOR THE KEY : ", get_args.Key, " : ", get_reply.Value)

				} else if get_reply.Err == ErrNoKey {

					// ck.mu.Lock()
					ck.txn_id = get_reply.Updated_txn_id
					// ck.mu.Unlock()
					answer = ""

				} else if get_reply.Err == "Dup_req" {

					ck.txn_id = get_reply.Updated_txn_id

					answer = get_reply.Value

				}

			}
		} else {
			index = index + 1
		}

		if index >= length {
			index = 0
		}

	}

	return answer
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.

	// fmt.Println("_______________________Your key is : ", key, " value is : ", value, " Command is : ", op)
	index := 0
	flag := false

	// ck.mu.Unlock()

	for flag == false {
		put_args := PutAppendArgs{}
		put_reply := PutAppendReply{}

		// ck.mu.Lock()

		put_args.Op = op
		put_args.Key = key
		put_args.Value = value
		put_args.Client_id = ck.clerk_id
		put_args.Txn_id = ck.txn_id
		length := len(ck.servers)

		// ck.mu.Lock()

		target_server := ck.servers[index]

		// ck.mu.Unlock()

		ok := target_server.Call("RaftKV.PutAppend", &put_args, &put_reply)

		// fmt.Println(put_reply.WrongLeader)

		if ok {

			// fmt.Printf("i m here")
			if put_reply.WrongLeader == true {

				index = index + 1
				// fmt.Printf("i m the star")

			} else {

				flag = true

				if put_reply.Err == OK {

					// fmt.Println("_______YES I AM DONE FINALLY_________")
					// ck.mu.Lock()
					ck.txn_id = put_reply.Updated_txn_id
					// ck.mu.Unlock()

				} else if put_reply.Err == "Dup_req" {

					ck.txn_id = put_reply.Updated_txn_id

				}
			}
		} else {

			index = index + 1
		}

		if index >= length {
			// time.Sleep(500 * time.Millisecond)
			index = 0
		}

	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

/*

STUFF TO CHECK :

-- MAKE GET SIMILAR TO PUT IN CLIENT
-- ADD DUPLICACY IN CLIENTS
-- CHECK THE FLOW

-- REMOVE ADDITIONS TO THE KV STRUCTURES
-- CHECK REPLY CONDITIONS
-- CHECK THE FLOW

*/
