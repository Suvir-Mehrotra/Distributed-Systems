package shardkv

import (
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"paxos"
	"shardmaster"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
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
	Key    string
	Value  string
	OpType string
	CurID  int64
	PrevID int64
	ConNum int
}

type ShardKV struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	sm         *shardmaster.Clerk
	px         *paxos.Paxos

	gid int64 // my replica group ID

	// Your definitions here.

	con        shardmaster.Config          //  keep track of the current replica group/shard configuration
	curSeq     int                         //  keeps track of current sequence instance
	myData     map[string]string           //  keeps track of the current k/v database
	prevReqs   map[int64]string            //  keeps track of requests sent by the client
	dataInView map[int](map[string]string) //  keeps track of k/v database for a specified configuration
}

func (kv *ShardKV) RunPaxos(seq int, v Op) Op {
	kv.px.Start(seq, v)

	to := 10 * time.Millisecond
	for {
		operationStatus, operationValue := kv.px.Status(seq)
		if operationStatus == paxos.Decided {
			return operationValue.(Op)
		}

		time.Sleep(to)
		if to < 10*time.Second {
			to *= 2
		}
	}
}

func (kv *ShardKV) FreePrevReq(prevId int64) {
	if prevId != -1 {
		_, ok := kv.prevReqs[prevId]
		if ok {
			delete(kv.prevReqs, prevId)
		}
	}
}

func (kv *ShardKV) ExecGet(key string, value string, currID int64) {
	prev, ok := kv.myData[key]
	if ok {
		kv.prevReqs[currID] = prev
	} else {
		kv.prevReqs[currID] = ErrNoKey
	}
}

func (kv *ShardKV) ExecPut(key string, value string, currID int64) {
	kv.myData[key] = value
	kv.prevReqs[currID] = OK
}

func (kv *ShardKV) ExecAppend(key string, value string, currID int64) {
	prev, ok := kv.myData[key]
	if ok {
		kv.myData[key] = prev + value
	} else {
		kv.myData[key] = value
	}
	kv.prevReqs[currID] = OK
}

func (kv *ShardKV) ProcessData(args *ProcessDataArgs, reply *ProcessDataReply) error {
	data, ok := kv.dataInView[args.ConNum]
	if ok {
		reply.MyData = make(map[string]string)
		for k, v := range data {
			reply.MyData[k] = v
		}
		reply.Err = OK
	}

	return nil
}

func (kv *ShardKV) ExecReconfig(lastConNum int) {
	//  Update current server's DB until it reflects the latest configuration
	for kv.con.Num < lastConNum {
		if kv.con.Num == 0 {
			kv.con = kv.sm.Query(1)
			continue
		}

		//  Save what the current server's DB looked like for the current configuration
		currData := make(map[string]string)
		for k, v := range kv.myData {
			shard := key2shard(k)
			if kv.con.Shards[shard] == kv.gid {
				currData[k] = v
			}
		}
		kv.dataInView[kv.con.Num] = currData

		//  Update the current server's DB one view at a time
		currCon := kv.con
		nextCon := kv.sm.Query(kv.con.Num + 1)
		for shard, currGroup := range currCon.Shards {
			nextGroup := nextCon.Shards[shard]
			//  If a shard being served by some other replica group is now being served by me, retrieve the data
			if currGroup != nextGroup && nextGroup == kv.gid {
				done := false
				//  Update DB from one of the servers in the other replica group (retry in the face of network partitions)
				for !done {
					for _, server := range currCon.Groups[currGroup] {
						//  Prepare the arguments
						args := &ProcessDataArgs{currCon.Num}
						var reply ProcessDataReply

						//  Make RPC call to retrieve data from the other servers
						ok := call(server, "ShardKV.ProcessData", args, &reply)
						if ok && reply.Err == OK {
							//  Update database, done for next configuration
							for k, v := range reply.MyData {
								kv.myData[k] = v
							}

							done = true
							break
						}
					}
				}
			}
		}

		//  Move on to next view
		kv.con = nextCon
	}
}
func (kv *ShardKV) ExecOp(op Op) {
	key, value, currID, operation, conNum := op.Key, op.Value, op.CurID, op.OpType, op.ConNum
	switch operation {
	case "Get":
		kv.ExecGet(key, value, currID)
	case "Put":
		kv.ExecPut(key, value, currID)
	case "Append":
		kv.ExecAppend(key, value, currID)
	case "Reconfigure":
		kv.ExecReconfig(conNum)
	}
}

func (kv *ShardKV) Run(operation Op, getReply *GetReply, putAppendReply *PutAppendReply) {
	for {
		kv.curSeq++
		//  Gain consensus on this Paxos instance
		var operationResult Op
		operationStatus, operationValue := kv.px.Status(kv.curSeq)
		//  We are decided on this instance
		if operationStatus == paxos.Decided {
			operationResult = operationValue.(Op)
			//  We aren't decided on this instance, run Paxos until we are
		} else {
			operationResult = kv.RunPaxos(kv.curSeq, operation)
		}

		//  Clean up client requests that have previously been served
		kv.FreePrevReq(operation.PrevID)
		//  We have agreed on this instance, so apply the operation
		kv.ExecOp(operationResult)
		//  We are done processing this instance, and will no longer need it or any previous instance
		kv.px.Done(kv.curSeq)

		//  Paxos elected the current operation, done
		if operationResult.CurID == operation.CurID {
			val := kv.prevReqs[operation.CurID]
			if val == ErrNoKey {
				getReply.Value = ""
				getReply.Err = ErrNoKey
			} else {
				getReply.Value = kv.prevReqs[operation.CurID]
				getReply.Err = OK
			}

			putAppendReply.Err = OK

			break
		}
	}
}

func (kv *ShardKV) DuplicateGet(args *GetArgs) bool {
	key, curID := args.Key, args.CurID
	prev, ok := kv.prevReqs[curID]

	//  Duplicate RPC request
	if ok && prev == key {
		return true
	}

	return false
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	//  Check if this server is responsible for this key's shard
	shard := key2shard(args.Key)
	if kv.con.Shards[shard] != kv.gid {
		reply.Err = ErrWrongGroup
		return nil
	}

	//  Check if we have seen this request before, and if so, return stored value
	if kv.DuplicateGet(args) {
		reply.Value = kv.myData[args.Key]
		reply.Err = OK
		return nil
	}

	//  Prepare Paxos value
	currOp := Op{Key: args.Key, OpType: "Get", CurID: args.CurID, PrevID: args.PrevID}
	kv.Run(currOp, reply, &PutAppendReply{})

	return nil
}

func (kv *ShardKV) DuplicateAppend(args *PutAppendArgs) bool {
	_, ok := kv.prevReqs[args.CurID]

	//  Duplicate RPC request
	if ok {
		return true
	}

	return false
}

// RPC handler for client Put and Append requests
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	//  Check if this server is responsible for this key's shard
	shard := key2shard(args.Key)
	if kv.con.Shards[shard] != kv.gid {
		reply.Err = ErrWrongGroup
		return nil
	}

	//  Check if we have seen this request before
	if kv.DuplicateAppend(args) {
		reply.Err = OK
		return nil
	}

	//  Prepare Paxos value
	currOp := Op{Key: args.Key, Value: args.Value, OpType: args.Op, CurID: args.CurID, PrevID: args.PrevID}
	kv.Run(currOp, &GetReply{}, reply)

	return nil
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	//  Check if the configuration this server has is up-to-date
	lastConNum := (kv.sm.Query(-1)).Num
	if kv.con.Num != lastConNum {
		//  Prepare Paxos value
		currOp := Op{OpType: "Reconfigure", ConNum: lastConNum}
		kv.Run(currOp, &GetReply{}, &PutAppendReply{})
	}
}

// tell the server to shut itself down.
// please don't change these two functions.
func (kv *ShardKV) kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *ShardKV) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *ShardKV) Setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *ShardKV) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
//
func StartServer(gid int64, shardmasters []string,
	servers []string, me int) *ShardKV {
	gob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters)

	// Your initialization code here.
	// Don't call Join().

	kv.con = kv.sm.Query(-1)
	kv.curSeq = 0
	kv.myData = make(map[string]string)
	kv.prevReqs = make(map[int64]string)
	kv.dataInView = make(map[int](map[string]string))

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.isdead() == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.isdead() == false {
				if kv.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && kv.isdead() == false {
				fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	go func() {
		for kv.isdead() == false {
			kv.tick()
			time.Sleep(250 * time.Millisecond)
		}
	}()

	return kv
}
