package shardmaster

import (
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"paxos"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type ShardMaster struct {
	mu               sync.Mutex
	l                net.Listener
	me               int
	dead             int32 // for testing
	unreliable       int32 // for testing
	px               *paxos.Paxos
	currentConfigNum int
	configs          []Config // indexed by config num
	curSequence      int
}

type Op struct {
	// Your data here.
	//desired config
	Config Config
	//either "query" or "config"
	Operation string
	//Unique ID per op for identification
	ID int64
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	// Your code here.
	joinGID, servers := args.GID, args.Servers

	sm.mu.Lock()
	defer sm.mu.Unlock()
	//fmt.Printf("join: GID %d\n", joinGID)
	for {
		//create new config with new GID, rebalance shards
		newGroups := make(map[int64][]string)
		for i, v := range sm.configs[sm.currentConfigNum].Groups {
			newGroups[i] = v
		}
		newGroups[joinGID] = servers
		newShards := sm.rebalanceShards(joinGID, -1, newGroups)

		newConfig := Config{Num: sm.currentConfigNum + 1, Shards: newShards, Groups: newGroups}
		operation := Op{Config: newConfig, Operation: "config", ID: sm.UniqueID()}
		//fmt.Printf("proposing operation: %v\n", operation)

		curSequence := sm.curSequence
		sm.curSequence++
		var opRes Op
		opStatus, opVal := sm.px.Status(curSequence)

		if opStatus == paxos.Decided {
			opRes = opVal.(Op)
		} else {
			//fmt.Printf("executing sequence: %d\n", curSequence)
			opRes = sm.ExecPax(curSequence, operation)
		}

		//fmt.Printf("executing operation: %v\n", opRes)
		sm.ExecOp(opRes)

		sm.px.Done(curSequence)

		if opRes.ID == operation.ID {
			break
		}
	}

	return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	// Your code here.
	leaveGID := args.GID

	sm.mu.Lock()
	defer sm.mu.Unlock()
	//fmt.Printf("leave: GID %d\n", leaveGID)
	for {
		//create new config with new GID, rebalance shards

		newGroups := make(map[int64][]string)
		for i, v := range sm.configs[sm.currentConfigNum].Groups {
			if i != leaveGID {
				newGroups[i] = v
			}
		}
		newShards := sm.rebalanceShards(-1, leaveGID, newGroups)

		newConfig := Config{Num: sm.currentConfigNum + 1, Shards: newShards, Groups: newGroups}
		operation := Op{Config: newConfig, Operation: "config", ID: sm.UniqueID()}

		curSequence := sm.curSequence
		sm.curSequence++

		var opRes Op
		opStatus, opVal := sm.px.Status(curSequence)

		if opStatus == paxos.Decided {
			opRes = opVal.(Op)
		} else {
			opRes = sm.ExecPax(curSequence, operation)
		}

		sm.ExecOp(opRes)

		sm.px.Done(curSequence)

		if opRes.ID == operation.ID {
			break
		}
	}

	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	// Your code here.
	// create new config with provided shard assigned to provided GID
	// loop until paxos decides on this config

	sm.mu.Lock()
	defer sm.mu.Unlock()
	//fmt.Printf("move: GID %d -> shard %d\n", args.GID, args.Shard)
	for {
		newShards := sm.configs[sm.currentConfigNum].Shards
		newShards[args.Shard] = args.GID

		newConfig := Config{Num: sm.currentConfigNum + 1, Shards: newShards, Groups: sm.configs[sm.currentConfigNum].Groups}
		operation := Op{Config: newConfig, Operation: "config", ID: sm.UniqueID()}

		curSequence := sm.curSequence
		sm.curSequence++

		var opRes Op
		opStatus, opVal := sm.px.Status(curSequence)

		if opStatus == paxos.Decided {
			opRes = opVal.(Op)
		} else {
			opRes = sm.ExecPax(curSequence, operation)
		}

		sm.ExecOp(opRes)

		sm.px.Done(curSequence)

		if opRes.ID == operation.ID {
			break
		}
	}
	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	// Your code here.
	queryNum := args.Num

	//fmt.Printf("query: %d\n", queryNum)
	sm.mu.Lock()
	defer sm.mu.Unlock()
	//easy if already decided, just return config num
	if queryNum <= sm.currentConfigNum && queryNum > -1 {
		reply.Config = sm.configs[queryNum]
		//fmt.Printf("replying to query without paxos: %v\n", reply.Config)
	} else {
		//if called with -1 or higher number, need to use paxos to decide when query is ready
		for {
			operation := Op{Operation: "query", ID: sm.UniqueID()}

			curSequence := sm.curSequence
			sm.curSequence++

			var opRes Op
			opStatus, opVal := sm.px.Status(curSequence)

			if opStatus == paxos.Decided {
				opRes = opVal.(Op)
			} else {
				opRes = sm.ExecPax(curSequence, operation)
			}

			sm.ExecOp(opRes)

			sm.px.Done(curSequence)

			if opRes.ID == operation.ID || opRes.Config.Num == queryNum {
				reply.Config = sm.configs[sm.currentConfigNum]
				break
			}
		}
		//fmt.Printf("replying to query after decision: %v\n", reply.Config)
	}

	return nil
}

func (sm *ShardMaster) ExecPax(seq int, op Op) Op {
	sm.px.Start(seq, op)

	timeCheck := 10 * time.Millisecond
	for {
		opStatus, opVal := sm.px.Status(seq)
		if opStatus == paxos.Decided {
			return opVal.(Op)
		}

		time.Sleep(timeCheck)
		if timeCheck < 10*time.Second {
			timeCheck *= 2
		}
	}
}

func (sm *ShardMaster) ExecOp(opReq Op) {
	config, op := opReq.Config, opReq.Operation

	//  Apply query
	if op == "query" {
		//  Apply config
	} else if op == "config" {
		sm.configs = append(sm.configs, config)
		//fmt.Printf("new config: %v\n", config)
		sm.currentConfigNum = config.Num
	}
}

func (sm *ShardMaster) UniqueID() int64 {
	return int64(sm.me)<<32 + int64(sm.curSequence)
}

func (sm *ShardMaster) rebalanceShards(joinGID int64, leaveGID int64, groups map[int64][]string) [NShards]int64 {
	//fmt.Printf("joinGID: %d, leaveGID: %d\n", joinGID, leaveGID)
	newShards := sm.configs[sm.currentConfigNum].Shards
	gidToShards := make(map[int64][]int)
	if joinGID != -1 {
		gidToShards[joinGID] = make([]int, 0)
	}

	//loop through shards to make map from gid to Shards
	for shard, shardGID := range newShards {
		if gidToShards[shardGID] == nil {
			gidToShards[shardGID] = make([]int, 0)
		}
		gidToShards[shardGID] = append(gidToShards[shardGID], shard)
	}

	//add groups that have no keys to rebalance
	for gid := range groups {
		if gidToShards[gid] == nil {
			gidToShards[gid] = make([]int, 0)
		}
	}

	//fmt.Printf("gidToShards: %v\n", gidToShards)

	//find number of gids/groups (not including 0, invalid gid)
	numGroups := len(groups)
	/* if leaveGID != -1 && gidToShards[leaveGID] != nil {
		numGroups--
	}
	if gidToShards[0] != nil {
		numGroups--
	} */

	//find necessary number of shards per group with remainder for balance
	shardsPerGroup := NShards / numGroups
	shardsRemainder := NShards % numGroups
	if shardsPerGroup < 1 {
		shardsPerGroup = 1
		shardsRemainder = 0
	}

	//slice for shards that are available to be reassigned
	availableShards := make([]int, 0)
	//map from gid to number of shards that the gid needs to reach balance
	gidToShardsNeeded := make(map[int64]int)

	//loop over all gids to find which ones need shards and which have too many
	for i, shards := range gidToShards {
		if i == 0 || groups[i] == nil {
			availableShards = append(availableShards, shards...)
		} else if shardsRemainder > 0 && len(shards) > shardsPerGroup+1 {
			shardsRemainder--
			numAvailable := len(shards) - shardsPerGroup + 1
			availableShards = append(availableShards, shards[:numAvailable]...)
			gidToShards[i] = shards[numAvailable:]
		} else if len(shards) > shardsPerGroup {
			numAvailable := len(shards) - shardsPerGroup
			availableShards = append(availableShards, shards[:numAvailable]...)
			gidToShards[i] = shards[numAvailable:]
		} else {
			numNeeded := shardsPerGroup - len(shards)
			gidToShardsNeeded[i] = numNeeded
		}
	}

	//loop over GIDs that need shards
	for gid, shardsNeeded := range gidToShardsNeeded {
		for i := 0; i < shardsNeeded; i++ {
			if len(availableShards) > 0 {
				newShards[availableShards[0]] = gid
				availableShards = availableShards[1:]
			}
		}
	}
	//fmt.Printf("rebalanced from: %v to -> %v\n", sm.configs[sm.currentConfigNum].Shards, newShards)
	return newShards
}

// please don't change these two functions.
func (sm *ShardMaster) Kill() {
	atomic.StoreInt32(&sm.dead, 1)
	sm.l.Close()
	sm.px.Kill()
}

// call this to find out if the server is dead.
func (sm *ShardMaster) isdead() bool {
	return atomic.LoadInt32(&sm.dead) != 0
}

// please do not change these two functions.
func (sm *ShardMaster) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&sm.unreliable, 1)
	} else {
		atomic.StoreInt32(&sm.unreliable, 0)
	}
}

func (sm *ShardMaster) isunreliable() bool {
	return atomic.LoadInt32(&sm.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int64][]string{}

	sm.curSequence = 0

	rpcs := rpc.NewServer()

	gob.Register(Op{})
	rpcs.Register(sm)
	sm.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	sm.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for sm.isdead() == false {
			conn, err := sm.l.Accept()
			if err == nil && sm.isdead() == false {
				if sm.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if sm.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && sm.isdead() == false {
				fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
				sm.Kill()
			}
		}
	}()

	return sm
}
