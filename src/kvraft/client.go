package kvraft

import (
	"6.824/labrpc"
	"sync"
	"time"
)
import "crypto/rand"
import "math/big"

const retryInterval = 30 * time.Millisecond

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	nums     int   // number of servers
	leader   int   // last leader
	clientID int64 // created by nrand
	seqNum   int64 // sequence number of reqs from this client
	mu       sync.Mutex
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
	ck.nums = len(ck.servers)
	ck.leader = 0
	ck.clientID = nrand()
	ck.seqNum = ck.clientID // init to clientID
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	ck.mu.Lock()
	leader := ck.leader
	seq := ck.seqNum
	ck.seqNum++
	ck.mu.Unlock()
	args := GetArgs{
		Key:      key,
		ClientID: ck.clientID,
		SeqNum:   seq,
	}
	for {
		reply := GetReply{}
		ok := ck.servers[leader].Call("KVServer.Get", &args, &reply)
		if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
			ck.mu.Lock()
			ck.leader = leader
			ck.mu.Unlock()
			DPrintf("%v get[%v] = %v", ck.clientID, args.Key, reply.Value)
			return reply.Value
		}
		leader = (leader + 1) % ck.nums
		if leader == 0 {
			time.Sleep(retryInterval)
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.mu.Lock()
	leader := ck.leader
	seq := ck.seqNum
	ck.seqNum++
	ck.mu.Unlock()
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClientID: ck.clientID,
		SeqNum:   seq,
	}
	for {
		reply := PutAppendReply{}
		ok := ck.servers[leader].Call("KVServer.PutAppend", &args, &reply)
		if ok && reply.Err == OK {
			ck.mu.Lock()
			ck.leader = leader
			ck.mu.Unlock()
			return
		}
		leader = (leader + 1) % ck.nums
		if leader == 0 {
			time.Sleep(retryInterval)
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
