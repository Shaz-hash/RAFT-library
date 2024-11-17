package raftkv

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	// My additions :
	Txn_id    int
	Client_id int
}

type PutAppendReply struct {
	WrongLeader    bool
	Err            Err
	Updated_txn_id int
}

type GetArgs struct {
	Key       string
	Txn_id    int
	Client_id int

	// You'll have to add definitions here.

}

type GetReply struct {
	WrongLeader    bool
	Err            Err
	Value          string
	Updated_txn_id int
}
