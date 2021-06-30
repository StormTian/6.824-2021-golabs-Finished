package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrFail = "ErrFail"
)

type Err string

const (
	GetOp    = "Get"
	PutOp    = "Put"
	AppendOp = "Append"
)

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientID int64
	SeqNum   int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientID int64
}

type GetReply struct {
	Err   Err
	Value string
}

type TransDataArgs struct {
	Shard int
	CurCfgNum int // current config num
}

type TransDataReply struct {
	Prepared bool // data has been prepared or not
	Shard int
	CurCfgNum int
	Database map[string]string
	DupDetect map[int64]int64
	Gid int // gid of lose servers
}

type ClearLoseDataArgs struct {
	Shard int
	CurCfgNum int
}

type ClearLoseDataReply struct {}