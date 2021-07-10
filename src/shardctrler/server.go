package shardctrler

import (
	"6.824/labgob"
	"6.824/raft"
	"log"
	"sort"
	"time"
)
import "6.824/labrpc"
import "sync"

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf("sc | "+format, a...)
	}
	return
}

type res struct {
	clientID int64
	seqNum   int64
	err      Err
	cfg      Config
}

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	configs   []Config         // indexed by config num
	dupDetect map[int64]int64  // clientID -> latest seq num
	resChan   map[int]chan res // index -> channel
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	sc.lock(sc.me, "Join")
	index, _, isLeader := sc.rf.Start(args)
	if !isLeader {
		reply.WrongLeader = true
		sc.unlock(sc.me, "Join")
		return
	}
	reply.WrongLeader = false
	c := make(chan res, 1)
	sc.resChan[index] = c
	sc.unlock(sc.me, "Join")

	DPrintf("%d waiting for op %v index %d", sc.me, args, index)
	r := sc.bePoked(c)
	if r.clientID == -1 || r.clientID != args.ClientID || r.seqNum != args.SeqNum {
		// different req appears at the index, leader has changed
		reply.Err = ErrFail
		return
	}
	reply.Err = r.err
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	sc.lock(sc.me, "Leave")
	index, _, isLeader := sc.rf.Start(args)
	if !isLeader {
		reply.WrongLeader = true
		sc.unlock(sc.me, "Leave")
		return
	}
	reply.WrongLeader = false
	c := make(chan res, 1)
	sc.resChan[index] = c
	sc.unlock(sc.me, "Leave")

	DPrintf("%d waiting for op %v index %d", sc.me, args, index)
	r := sc.bePoked(c)
	if r.clientID == -1 || r.clientID != args.ClientID || r.seqNum != args.SeqNum {
		// different req appears at the index, leader has changed
		reply.Err = ErrFail
		return
	}
	reply.Err = r.err
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	sc.lock(sc.me, "Move")
	index, _, isLeader := sc.rf.Start(args)
	if !isLeader {
		reply.WrongLeader = true
		sc.unlock(sc.me, "Move")
		return
	}
	reply.WrongLeader = false
	c := make(chan res, 1)
	sc.resChan[index] = c
	sc.unlock(sc.me, "Move")

	DPrintf("%d waiting for op %v index %d", sc.me, args, index)
	r := sc.bePoked(c)
	if r.clientID == -1 || r.clientID != args.ClientID || r.seqNum != args.SeqNum {
		// different req appears at the index, leader has changed
		reply.Err = ErrFail
		return
	}
	reply.Err = r.err
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	sc.lock(sc.me, "Query")
	index, _, isLeader := sc.rf.Start(args)
	if !isLeader {
		reply.WrongLeader = true
		sc.unlock(sc.me, "Query")
		return
	}
	reply.WrongLeader = false
	c := make(chan res, 1)
	sc.resChan[index] = c
	sc.unlock(sc.me, "Query")

	DPrintf("%d waiting for op %v index %d", sc.me, args, index)
	r := sc.bePoked(c)
	if r.clientID == -1 || r.clientID != args.ClientID {
		// different req appears at the index, leader has changed
		reply.Err = ErrFail
		return
	}
	reply.Err = r.err
	reply.Config = r.cfg
}

func (sc *ShardCtrler) bePoked(c chan res) res {
	ticker := time.NewTicker(500 * time.Millisecond)
	r := res{
		clientID: -1,
	}
	select {
	case r = <-c:
		return r
	case <-ticker.C:
		return r
	}
}

// applier reads message from apply ch and execute it
func (sc *ShardCtrler) applier() {
	for {
		m := <-sc.applyCh
		index := m.CommandIndex
		var r res
		if op, ok := m.Command.(*QueryArgs); ok {
			// query op
			DPrintf("%d recv query op %v index %d", sc.me, op, index)
			sc.lock(sc.me, "execute")
			var cfg Config
			if op.Num == -1 || op.Num >= len(sc.configs) {
				cfg = sc.configs[len(sc.configs)-1]
			} else {
				cfg = sc.configs[op.Num]
			}
			DPrintf("cfg %d:\n%v", op.Num, cfg)
			sc.unlock(sc.me, "execute")
			r.clientID = op.ClientID
			r.cfg = cfg
			r.err = OK
		} else if op, ok := m.Command.(*JoinArgs); ok {
			// join op
			DPrintf("%d recv join op %v index %d", sc.me, op, index)
			sc.lock(sc.me, "execute")
			latestSeqNum, ok := sc.dupDetect[op.ClientID]
			if !ok || op.SeqNum > latestSeqNum {
				// execute
				sc.dupDetect[op.ClientID] = op.SeqNum
				// init new config
				lastCfg := sc.configs[len(sc.configs)-1]
				DPrintf("last cfg:\n%v", lastCfg)
				newCfg := Config{}
				newCfg.Num = lastCfg.Num + 1
				newCfg.Groups = make(map[int][]string)
				for gid, servers := range lastCfg.Groups {
					newCfg.Groups[gid] = servers // copy old groups
				}
				// get all new groups, sort them by gid
				newGids := []int{}
				for gid, _ := range op.Servers {
					newGids = append(newGids, gid)
				}
				sort.Ints(newGids)
				DPrintf("new gids: %v", newGids)
				// get old arrangement, gid -> shards list
				curArr := getArrangement(lastCfg)
				DPrintf("current arrangement: %v", curArr)
				// deal with new groups one by one
				for i := 0; i < len(newGids); i++ {
					newGid := newGids[i]
					// add this group to Groups
					newCfg.Groups[newGid] = op.Servers[newGid]
					DPrintf("add group %d", newGid)
					// re-config
					if len(newCfg.Groups) == 1 {
						// the first group
						curArr[newGid] = []int{}
						for i := 0; i < NShards; i++ {
							curArr[newGid] = append(curArr[newGid], i)
						}
						delete(curArr, 0)
						DPrintf("current arrangement: %v", curArr)
						continue
					}
					curArr[newGid] = []int{}
					aimShardsNum := NShards / len(newCfg.Groups)
					DPrintf("aimShardsNum %d", aimShardsNum)
					for k := 0; k < aimShardsNum; k++ {
						aimGid := getLargestGroup(curArr, newGid)
						aimShard := curArr[aimGid][0]
						curArr[aimGid] = curArr[aimGid][1:]
						curArr[newGid] = append(curArr[newGid], aimShard)
						DPrintf("current arrangement: %v", curArr)
					}
				}
				// append new config
				newCfg.Shards = getShards(curArr)
				sc.configs = append(sc.configs, newCfg)
				DPrintf("new cfg:\n%v", newCfg)
			}
			sc.unlock(sc.me, "execute")
			r.clientID = op.ClientID
			r.seqNum = op.SeqNum
			r.err = OK
		} else if op, ok := m.Command.(*LeaveArgs); ok {
			// leave op
			DPrintf("%d recv leave op %v index %d", sc.me, op, index)
			sc.lock(sc.me, "execute")
			latestSeqNum, ok := sc.dupDetect[op.ClientID]
			if !ok || op.SeqNum > latestSeqNum {
				// execute
				sc.dupDetect[op.ClientID] = op.SeqNum
				// init new config
				lastCfg := sc.configs[len(sc.configs)-1]
				DPrintf("last cfg:\n%v", lastCfg)
				newCfg := Config{}
				newCfg.Num = lastCfg.Num + 1
				newCfg.Groups = make(map[int][]string)
				for gid, servers := range lastCfg.Groups {
					newCfg.Groups[gid] = servers // copy old groups
				}
				// sort all leaving groups by gid
				leavingGids := op.GIDs
				sort.Ints(leavingGids)
				DPrintf("leaving gids: %v", leavingGids)
				// get old arrangement, gid -> shards list
				curArr := getArrangement(lastCfg)
				DPrintf("current arrangement: %v", curArr)
				// deal with new groups one by one
				for i := 0; i < len(leavingGids); i++ {
					leaveGid := leavingGids[i]
					delete(newCfg.Groups, leaveGid)
					DPrintf("delete group %d", leaveGid)
					// re-config
					toReArrShards := curArr[leaveGid]
					for _, toArrShard := range toReArrShards {
						aimGid := getSmallestGroup(curArr, leaveGid)
						curArr[aimGid] = append(curArr[aimGid], toArrShard)
					}
					delete(curArr, leaveGid)
					DPrintf("current arrangement: %v", curArr)
				}
				// append new config
				newCfg.Shards = getShards(curArr)
				sc.configs = append(sc.configs, newCfg)
				DPrintf("new cfg:\n%v", newCfg)
			}
			sc.unlock(sc.me, "execute")
			r.clientID = op.ClientID
			r.seqNum = op.SeqNum
			r.err = OK
		} else if op, ok := m.Command.(*MoveArgs); ok {
			// move op
			DPrintf("%d recv move op %v index %d", sc.me, op, index)
			sc.lock(sc.me, "execute")
			latestSeqNum, ok := sc.dupDetect[op.ClientID]
			if !ok || op.SeqNum > latestSeqNum {
				// execute
				sc.dupDetect[op.ClientID] = op.SeqNum
				// init new config
				lastCfg := sc.configs[len(sc.configs)-1]
				DPrintf("last cfg:\n%v", lastCfg)
				newCfg := Config{}
				newCfg.Num = lastCfg.Num + 1
				newCfg.Groups = make(map[int][]string)
				for gid, servers := range lastCfg.Groups {
					newCfg.Groups[gid] = servers // copy old groups
				}
				newCfg.Shards = [NShards]int{}
				for shard, gid := range lastCfg.Shards {
					newCfg.Shards[shard] = gid
				}
				newCfg.Shards[op.Shard] = op.GID // move
				sc.configs = append(sc.configs, newCfg)
				DPrintf("new cfg:\n%v", newCfg)
			}
			sc.unlock(sc.me, "execute")
			r.clientID = op.ClientID
			r.seqNum = op.SeqNum
			r.err = OK
		}
		sc.lock(sc.me, "resChan")
		c, ok := sc.resChan[index]
		sc.unlock(sc.me, "resChan")
		if ok {
			c <- r
		}
	}
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{} // Num and Shards are tolerant to be 0

	// labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	// for decoding op
	labgob.Register(&QueryArgs{})
	labgob.Register(&JoinArgs{})
	labgob.Register(&LeaveArgs{})
	labgob.Register(&MoveArgs{})

	sc.dupDetect = make(map[int64]int64)
	sc.resChan = make(map[int]chan res)

	go sc.applier()

	return sc
}

func getArrangement(cfg Config) (curArr map[int][]int) {
	curArr = make(map[int][]int)
	for gid, _ := range cfg.Groups {
		// register the group charged for no shards also
		curArr[gid] = []int{}
	}
	shards := cfg.Shards
	for shard, gid := range shards {
		curArr[gid] = append(curArr[gid], shard) // shards list is sorted
	}
	return curArr
}

// find the group charged for most shards with smallest gid
func getLargestGroup(curArr map[int][]int, extraGid int) int {
	maxShards := 0
	aimGid := 0
	for gid, shards := range curArr {
		if gid == extraGid {
			continue
		}
		if len(shards) > maxShards {
			maxShards = len(shards)
			aimGid = gid
		} else if len(shards) == maxShards && gid < aimGid {
			aimGid = gid
		}
	}
	return aimGid
}

func getSmallestGroup(curArr map[int][]int, extraGid int) int {
	minShards := NShards + 1
	aimGid := 0
	for gid, shards := range curArr {
		if gid == extraGid {
			continue
		}
		if len(shards) < minShards {
			minShards = len(shards)
			aimGid = gid
		} else if len(shards) == minShards && gid < aimGid {
			aimGid = gid
		}
	}
	return aimGid
}

func getShards(curArr map[int][]int) (res [NShards]int) {
	res = [NShards]int{}
	for gid, shards := range curArr {
		for _, shard := range shards {
			res[shard] = gid
		}
	}
	return res
}

func (sc *ShardCtrler) lock(i int, msg string) {
	sc.mu.Lock()
	DPrintf("sc lock: %d %v", i, msg)
}

func (sc *ShardCtrler) unlock(i int, msg string) {
	sc.mu.Unlock()
	DPrintf("sc unlock: %d %v", i, msg)
}
