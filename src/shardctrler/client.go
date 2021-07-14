package shardctrler

//
// Shardctrler clerk.
//

import (
	"6.824/labrpc"
	"sync"
)
import "time"
import "crypto/rand"
import "math/big"

const retryInterval = 30 * time.Millisecond

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	nums     int // number of servers
	leader   int // last leader
	clientID int64
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
	// Your code here.
	ck.nums = len(ck.servers)
	ck.leader = 0
	ck.clientID = nrand()
	ck.seqNum = ck.clientID // init to clientID
	return ck
}

func (ck *Clerk) Query(num int) Config {
	ck.mu.Lock()
	leader := ck.leader
	ck.mu.Unlock()
	args := &QueryArgs{}
	// Your code here.
	args.Num = num
	args.ClientID = ck.clientID
	for {
		var reply QueryReply
		ok := ck.servers[leader].Call("ShardCtrler.Query", args, &reply)
		if ok && reply.WrongLeader == false {
			ck.mu.Lock()
			ck.leader = leader
			ck.mu.Unlock()
			return reply.Config
		}
		leader = (leader + 1) % ck.nums
		if leader == 0 {
			time.Sleep(retryInterval)
		}
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	ck.mu.Lock()
	leader := ck.leader
	seq := ck.seqNum
	ck.seqNum++
	ck.mu.Unlock()
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers
	args.ClientID = ck.clientID
	args.SeqNum = seq
	for {
		var reply JoinReply
		ok := ck.servers[leader].Call("ShardCtrler.Join", args, &reply)
		if ok && reply.WrongLeader == false {
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

func (ck *Clerk) Leave(gids []int) {
	ck.mu.Lock()
	leader := ck.leader
	seq := ck.seqNum
	ck.seqNum++
	ck.mu.Unlock()
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids
	args.ClientID = ck.clientID
	args.SeqNum = seq
	for {
		var reply LeaveReply
		ok := ck.servers[leader].Call("ShardCtrler.Leave", args, &reply)
		if ok && reply.WrongLeader == false {
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

func (ck *Clerk) Move(shard int, gid int) {
	ck.mu.Lock()
	leader := ck.leader
	seq := ck.seqNum
	ck.seqNum++
	ck.mu.Unlock()
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	args.ClientID = ck.clientID
	args.SeqNum = seq
	for {
		var reply MoveReply
		ok := ck.servers[leader].Call("ShardCtrler.Move", args, &reply)
		if ok && reply.WrongLeader == false {
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
