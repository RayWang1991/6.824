package raftkv

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	ClId  int // client id
	ReqId int // request id
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	ServId      int
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	ClId  int // client id
	ReqId int // request id
	Key   string
	// You'll have to add definitions here.
}

type GetReply struct {
	ServId      int
	WrongLeader bool
	Err         Err
	Value       string
}
