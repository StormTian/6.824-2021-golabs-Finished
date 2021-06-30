package shardkv

import (
	"6.824/labrpc"
	"6.824/shardctrler"
	"bytes"
	"log"
	"time"
)
import "6.824/raft"
import "sync"
import "6.824/labgob"

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf("shardkv | "+format, a...)
	}
	return
}

const threshold = 100
const (
	fetchLatestCfgInterval = 50 * time.Millisecond
	retryInterval          = 80 * time.Millisecond
	waitForLastChange      = 40 * time.Millisecond
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op       string // "Get" "Put" or "Append"
	Key      string
	Value    string // for PutAppend
	ClientID int64
	SeqNum   int64 // for PutAppend
}

type res struct {
	clientID int64
	seqNum   int64
	err      Err
	value    string
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	mck           *shardctrler.Clerk
	cfg           shardctrler.Config
	db            map[string]string
	dupDetect     map[int64]int64  // clientID -> latest seq num
	resChan       map[int]chan res // index -> channel
	persister     *raft.Persister
	cfgNumInLog   int
	nextCfg       shardctrler.Config // to change
	wantShardsNum int
	getShardsNum  int
	loseData      map[int]map[int]map[string]string // curCfgNum -> shard -> data
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		Op:       GetOp,
		Key:      args.Key,
		ClientID: args.ClientID,
	}
	kv.lock(kv.me, "Get")
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.unlock(kv.me, "Get")
		return
	}
	c := make(chan res, 1)
	kv.resChan[index] = c
	kv.unlock(kv.me, "Get")

	DPrintf("%d-%d waiting for op %v index %d", kv.gid, kv.me, op, index)
	r := kv.bePoked(c)
	if r.clientID == -1 || r.clientID != op.ClientID || r.seqNum != op.SeqNum {
		// different req appears at the index, leader has changed
		reply.Err = ErrFail
		DPrintf("op %v index %d fail", op, index)
		return
	}
	reply.Err = r.err
	reply.Value = r.value
	DPrintf("op %v index %d succeed", op, index)
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		Op:       args.Op,
		Key:      args.Key,
		Value:    args.Value,
		ClientID: args.ClientID,
		SeqNum:   args.SeqNum,
	}
	kv.lock(kv.me, "PutAppend")
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.unlock(kv.me, "PutAppend")
		return
	}
	c := make(chan res, 1)
	kv.resChan[index] = c
	kv.unlock(kv.me, "PutAppend")

	DPrintf("%d-%d waiting for op %v index %d", kv.gid, kv.me, op, index)
	// r := <-kv.resChan[index]
	r := kv.bePoked(c)
	if r.clientID == -1 || r.clientID != op.ClientID || r.seqNum != op.SeqNum {
		reply.Err = ErrFail
		DPrintf("op %v index %d fail", op, index)
		return
	}
	reply.Err = r.err
	DPrintf("op %v index %d succeed", op, index)
}

func (kv *ShardKV) bePoked(c chan res) res {
	ticker := time.NewTicker(time.Second)
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
func (kv *ShardKV) applier() {
	for {
		m := <-kv.applyCh
		if m.CommandValid {
			index := m.CommandIndex
			DPrintf("%d-%d recv entry index %d", kv.gid, kv.me, index)
			if op, ok := m.Command.(Op); ok {
				// Get, Put or Append
				DPrintf("%d-%d recv op %v", kv.gid, kv.me, op, index)
				r := res{
					clientID: op.ClientID,
					seqNum:   op.SeqNum,
				}
				shard := key2shard(op.Key)
				aimGid := kv.cfg.Shards[shard]
				if aimGid != kv.gid {
					r.err = ErrWrongGroup
					DPrintf("%d-%d wrong group for op %v index %d", kv.gid, kv.me, op, index)
				} else {
					switch op.Op {
					case GetOp:
						{
							kv.lock(kv.me, "execute")
							v, ok := kv.db[op.Key]
							DPrintf("%d-%d db:\n%v", kv.gid, kv.me, kv.db)
							kv.unlock(kv.me, "execute")
							if ok {
								r.value = v
								r.err = OK
							} else {
								r.value = ""
								r.err = ErrNoKey
							}
						}
					case PutOp:
						{
							kv.lock(kv.me, "execute")
							latestSeqNum, ok := kv.dupDetect[op.ClientID]
							if !ok || op.SeqNum > latestSeqNum {
								kv.db[op.Key] = op.Value
								DPrintf("%d-%d db:\n%v", kv.gid, kv.me, kv.db)
								kv.dupDetect[op.ClientID] = op.SeqNum
							}
							kv.unlock(kv.me, "execute")
							r.err = OK
						}
					case AppendOp:
						{
							kv.lock(kv.me, "execute")
							latestSeqNum, ok := kv.dupDetect[op.ClientID]
							if !ok || op.SeqNum > latestSeqNum {
								v, ok := kv.db[op.Key]
								if ok {
									newV := v + op.Value
									kv.db[op.Key] = newV
								} else {
									kv.db[op.Key] = op.Value
								}
								DPrintf("%d-%d db:\n%v", kv.gid, kv.me, kv.db)
								kv.dupDetect[op.ClientID] = op.SeqNum
							}
							kv.unlock(kv.me, "execute")
							r.err = OK
						}
					}
				}
				kv.lock(kv.me, "resChan")
				c, ok := kv.resChan[index]
				kv.unlock(kv.me, "resChan")
				if ok {
					c <- r
				}
			} else if newCfg, ok := m.Command.(shardctrler.Config); ok {
				// new config
				kv.dealWithNewCfg(newCfg)
			} else if reply, ok := m.Command.(*TransDataReply); ok {
				kv.lock(kv.me, "combineShard")
				if kv.cfg.Num == reply.CurCfgNum &&
					kv.cfg.Shards[reply.Shard] != kv.gid {
					kv.combineShard(reply)
				}
				kv.unlock(kv.me, "combineShard")
			}
			if kv.maxraftstate != -1 {
				if kv.maxraftstate-kv.persister.RaftStateSize() < threshold {
					kv.doSnapshot(index)
				}
			}
		}
		if m.SnapshotValid {
			// snapshot msg
			kv.readSnapshot(m.Snapshot)
		}
	}
}

func (kv *ShardKV) dealWithNewCfg(newCfg shardctrler.Config) {
	for {
		kv.lock(kv.me, "dealWithNewCfg")
		if newCfg.Num != kv.cfg.Num+1 {
			// Process re-configurations one at a time, in order.
			kv.unlock(kv.me, "dealWithNewCfg")
			if _, _, isLeader := kv.rf.Start(newCfg); isLeader {
				DPrintf("%d-%d put new cfg %d into log again", kv.gid, kv.me, newCfg.Num)
			}
			return
		}
		curCfg := kv.cfg
		kv.nextCfg = newCfg
		wantShards := []int{}
		loseShards := []int{}
		for shard, gid := range newCfg.Shards {
			if gid == kv.gid && curCfg.Shards[shard] != kv.gid {
				wantShards = append(wantShards, shard)
			}
			if curCfg.Shards[shard] == kv.gid && gid != kv.gid {
				loseShards = append(loseShards, shard)
			}
		}
		DPrintf("%d-%d stop service %v\nwant data %v", kv.gid, kv.me, loseShards, wantShards)
		// lose data
		curLoseData := make(map[int]map[string]string)
		for _, shard := range loseShards {
			curCfg.Shards[shard] = -1 // stop service immediately
			thisShardData := make(map[string]string)
			for k, v := range kv.db {
				if key2shard(k) == shard {
					thisShardData[k] = v
				}
			}
			curLoseData[shard] = thisShardData
		}
		kv.loseData[kv.cfg.Num] = curLoseData

		// ask for data
		kv.wantShardsNum = len(wantShards)
		if kv.wantShardsNum == 0 {
			kv.changeCfg(newCfg)
			kv.unlock(kv.me, "dealWithNewCfg")
			return
		}
		for _, shard := range wantShards {
			aimGid := curCfg.Shards[shard]
			if aimGid == 0 {
				kv.startAService(shard)
				continue
			}
			go kv.askForAShard(shard, aimGid)
		}
		kv.unlock(kv.me, "dealWithNewCfg")
		return
	}
}

// change config.
// locking
func (kv *ShardKV) changeCfg(newCfg shardctrler.Config) {
	kv.cfg = newCfg
	kv.wantShardsNum = 0
	kv.getShardsNum = 0
	DPrintf("%d-%d change cfg to %v", kv.gid, kv.me, kv.cfg)
}

// RaftStateSize is too large, do a snapshot
func (kv *ShardKV) doSnapshot(index int) {
	DPrintf("%d-%d before snapshot: %d", kv.gid, kv.me, kv.persister.RaftStateSize())
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	kv.lock(kv.me, "doSnapshot")
	e.Encode(kv.db)
	e.Encode(kv.dupDetect)
	e.Encode(kv.cfg)
	e.Encode(kv.cfgNumInLog)
	e.Encode(kv.nextCfg)
	e.Encode(kv.wantShardsNum)
	e.Encode(kv.getShardsNum)
	e.Encode(kv.loseData)
	kv.unlock(kv.me, "doSnapshot")
	snapshot := w.Bytes()
	kv.rf.Snapshot(index, snapshot)
	DPrintf("%d-%d after snapshot: %d", kv.gid, kv.me, kv.persister.RaftStateSize())
}

// be inited or get snapshot from leader
func (kv *ShardKV) readSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 { // bootstrap without any state?
		DPrintf("%d-%d read no data.", kv.gid, kv.me)
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var dbTmp map[string]string
	var dupTmp map[int64]int64
	var cfgTmp shardctrler.Config
	var cfgNumInLogTmp int
	var nextCfgTmp shardctrler.Config
	var want int
	var get int
	var lose map[int]map[int]map[string]string
	if d.Decode(&dbTmp) != nil ||
		d.Decode(&dupTmp) != nil ||
		d.Decode(&cfgTmp) != nil ||
		d.Decode(&cfgNumInLogTmp) != nil ||
		d.Decode(&nextCfgTmp) != nil ||
		d.Decode(&want) != nil ||
		d.Decode(&get) != nil ||
		d.Decode(&lose) != nil {
		DPrintf("decode snapshot fail.")
		return
	}
	kv.lock(kv.me, "readSnapshot")
	kv.db = dbTmp
	kv.dupDetect = dupTmp
	kv.cfg = cfgTmp
	kv.cfgNumInLog = cfgNumInLogTmp
	kv.nextCfg = nextCfgTmp
	kv.wantShardsNum = want
	kv.getShardsNum = get
	kv.loseData = lose
	// DPrintf("%d read snapshot\ndb: %v\ndup: %v", kv.me, kv.db, kv.dupDetect)
	kv.unlock(kv.me, "readSnapshot")
}

func (kv *ShardKV) fetchLatestCfg() {
	for {
		time.Sleep(fetchLatestCfgInterval)
		// check leader
		if _, isLeader := kv.rf.GetState(); !isLeader {
			time.Sleep(fetchLatestCfgInterval)
			continue
		}
		latestCfg := kv.mck.Query(kv.cfgNumInLog + 1)
		kv.lock(kv.me, "checkNewCfg")
		if latestCfg.Num == kv.cfgNumInLog {
			kv.unlock(kv.me, "checkNewCfg")
			continue
		}
		// new cfg, put into log
		kv.cfgNumInLog++
		kv.unlock(kv.me, "checkNewCfg")
		if _, _, isLeader := kv.rf.Start(latestCfg); !isLeader {
			time.Sleep(fetchLatestCfgInterval)
			continue
		}
		DPrintf("%d-%d put new cfg %d into log", kv.gid, kv.me, latestCfg.Num)
	}
}

// get a shard's data from another group, combine.
// locking
func (kv *ShardKV) combineShard(reply *TransDataReply) {
	// combine data
	for k, v := range reply.Database {
		kv.db[k] = v
	}
	for k, v := range reply.DupDetect {
		latest, ok := kv.dupDetect[k]
		if !ok || latest < v {
			kv.dupDetect[k] = v
		}
	}
	DPrintf("%d-%d db:\n%v", kv.gid, kv.me, kv.db)
	// inform delete
	go kv.informClear(reply.Shard, reply.CurCfgNum, kv.cfg.Groups[reply.Gid])
	kv.startAService(reply.Shard)
}

// start service for this shard, and check whether has started all service.
// locking
func (kv *ShardKV) startAService(shard int) {
	// start service
	kv.cfg.Shards[shard] = kv.gid
	kv.getShardsNum++
	DPrintf("%d-%d starts shard %d", kv.gid, kv.me, shard)
	// check cfg change
	if kv.getShardsNum == kv.wantShardsNum {
		kv.changeCfg(kv.nextCfg)
	}
}

func (kv *ShardKV) TransData(args *TransDataArgs, reply *TransDataReply) {
	kv.lock(kv.me, "TransData")
	defer kv.unlock(kv.me, "TransData")
	thisShardData, ok := kv.loseData[args.CurCfgNum][args.Shard]
	if !ok {
		// data hasn't been prepared
		reply.Prepared = false
		return
	}
	reply.Prepared = true
	toSendData := make(map[string]string)
	for k, v := range thisShardData {
		toSendData[k] = v
	}
	reply.Database = toSendData
	reply.DupDetect = make(map[int64]int64)
	for k, v := range kv.dupDetect {
		reply.DupDetect[k] = v
	}
	reply.Shard = args.Shard
	reply.CurCfgNum = args.CurCfgNum
	reply.Gid = kv.gid
}

func (kv *ShardKV) askForAShard(shard int, aimGid int) {
	kv.lock(kv.me, "askForAShard")
	aimServers := kv.cfg.Groups[aimGid]
	args := &TransDataArgs{
		Shard:     shard,
		CurCfgNum: kv.cfg.Num,
	}
	kv.unlock(kv.me, "askForAShard")
	for _, server := range aimServers {
		reply := &TransDataReply{}
		go kv.callTransData(server, args, reply)
	}
	DPrintf("ask for shard %d, %d-%d -> group %d", shard, kv.gid, kv.me, aimGid)
}

func (kv *ShardKV) callTransData(server string, args *TransDataArgs, reply *TransDataReply) {
	for {
		ok := kv.sendTransData(server, args, reply)
		if !ok || !reply.Prepared {
			// retry
			time.Sleep(retryInterval)
			kv.lock(kv.me, "callTransData retry")
			if kv.cfg.Num == args.CurCfgNum {
				kv.unlock(kv.me, "callTransData retry")
				continue
			} else {
				kv.unlock(kv.me, "callTransData retry")
				return
			}
		}
		kv.lock(kv.me, "callTransData")
		if kv.cfg.Num != args.CurCfgNum {
			// already change config
			kv.unlock(kv.me, "callTransData")
			return
		}
		kv.unlock(kv.me, "callTransData")
		kv.rf.Start(reply) // only leader put reply into raft log
		DPrintf("%d-%d put shard %d data into log", kv.gid, kv.me, args.Shard)
		return
	}
}

func (kv *ShardKV) sendTransData(server string, args *TransDataArgs, reply *TransDataReply) bool {
	ok := kv.make_end(server).Call("ShardKV.TransData", args, reply)
	return ok
}

func (kv *ShardKV) ClearLoseData(args *ClearLoseDataArgs, reply *ClearLoseDataReply) {
	kv.lock(kv.me, "ClearLoseData")
	defer kv.unlock(kv.me, "ClearLoseData")
	if _, ok := kv.loseData[args.CurCfgNum]; !ok {
		return
	}
	delete(kv.loseData[args.CurCfgNum], args.Shard)
	DPrintf("%d-%d clear loseData [%d-%d]", kv.gid, kv.me, args.CurCfgNum, args.Shard)
	if len(kv.loseData[args.CurCfgNum]) == 0 {
		delete(kv.loseData, args.CurCfgNum)
	}
}

func (kv *ShardKV) informClear(shard int, curCfgNum int, aimServers []string) {
	kv.lock(kv.me, "informClear")
	args := &ClearLoseDataArgs{
		Shard:     shard,
		CurCfgNum: curCfgNum,
	}
	kv.unlock(kv.me, "informClear")
	for _, server := range aimServers {
		reply := &ClearLoseDataReply{}
		go kv.callClearLoseData(server, args, reply)
	}
}

func (kv *ShardKV) callClearLoseData(server string, args *ClearLoseDataArgs, reply *ClearLoseDataReply) {
	for {
		ok := kv.sendClearLoseData(server, args, reply)
		if !ok {
			// retry
			time.Sleep(retryInterval)
			continue
		}
		return
	}
}

func (kv *ShardKV) sendClearLoseData(server string, args *ClearLoseDataArgs, reply *ClearLoseDataReply) bool {
	ok := kv.make_end(server).Call("ShardKV.ClearLoseData", args, reply)
	return ok
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(shardctrler.Config{})
	labgob.Register(&TransDataReply{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.cfg = kv.mck.Query(0)
	kv.wantShardsNum = 0
	kv.getShardsNum = 0
	kv.db = make(map[string]string)
	kv.resChan = make(map[int]chan res)
	kv.dupDetect = make(map[int64]int64)
	kv.persister = persister
	kv.cfgNumInLog = kv.cfg.Num
	kv.loseData = make(map[int]map[int]map[string]string)

	kv.readSnapshot(kv.persister.ReadSnapshot())
	go kv.applier()
	go kv.fetchLatestCfg()

	return kv
}

func (kv *ShardKV) lock(i int, msg string) {
	kv.mu.Lock()
	DPrintf("lock: %d-%d %v", kv.gid, i, msg)
}

func (kv *ShardKV) unlock(i int, msg string) {
	kv.mu.Unlock()
	DPrintf("unlock: %d-%d %v", kv.gid, i, msg)
}
