package kvpaxos

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

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key       string
	Value     string
	Operation string
	CurrID    int64
	PrevID    int64
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	// Your definitions here.
	curSequence int
	myData      map[string]string
	preReqs     map[int64]string
}

func HasDupG(kv *KVPaxos, args *GetArgs) bool {
	key, currID := args.Key, args.CurrID
	pre, isOK := kv.preReqs[currID]

	//  Duplicate RPC request
	if isOK && pre == key {
		return true
	}

	return false
}

func ExecPax(kv *KVPaxos, seq int, v Op) Op {
	kv.px.Start(seq, v)

	timeCheck := 10 * time.Millisecond
	for {
		opStatus, opVal := kv.px.Status(seq)
		if opStatus == paxos.Decided {
			return opVal.(Op)
		}

		time.Sleep(timeCheck)
		if timeCheck < 10*time.Second {
			timeCheck *= 2
		}
	}
}

func ExecOp(kv *KVPaxos, opRes Op) {
	key, value, currID, op := opRes.Key, opRes.Value, opRes.CurrID, opRes.Operation
	prev, isOK := kv.myData[key]
	//  Apply GET
	if op == "Get" {
		if isOK {
			kv.preReqs[currID] = prev
		} else {
			kv.preReqs[currID] = ErrNoKey
		}
		//  Apply PUT
	} else if op == "Put" {
		kv.myData[key] = value
		kv.preReqs[currID] = OK
		//  Apply APPEND
	} else if op == "Append" {
		kv.myData[key] = prev + value
		kv.preReqs[currID] = OK
	}
}

func FinishPrev(kv *KVPaxos, prevID int64) {
	if prevID != -1 {
		_, isOK := kv.preReqs[prevID]
		if isOK {
			delete(kv.preReqs, prevID)
		}
	}
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if HasDupG(kv, args) {
		reply.Value = kv.myData[args.Key]
		reply.Err = OK
		return nil
	}
	for {

		operation := Op{Key: args.Key, Value: "", Operation: "Get", CurrID: args.CurrID, PrevID: args.PrevID}
		curSequence := kv.curSequence
		kv.curSequence++

		var opRes Op
		opStatus, opVal := kv.px.Status(curSequence)

		if opStatus == paxos.Decided {
			opRes = opVal.(Op)

		} else {
			opRes = ExecPax(kv, curSequence, operation)
		}

		FinishPrev(kv, args.PrevID)

		ExecOp(kv, opRes)

		kv.px.Done(curSequence)

		if opRes.CurrID == args.CurrID {
			val := kv.preReqs[args.CurrID]
			if val == ErrNoKey {
				reply.Value = ""
				reply.Err = ErrNoKey
			} else {
				reply.Value = kv.preReqs[args.CurrID]
				reply.Err = OK
			}

			break
		}
	}
	return nil
}

func HasDupA(kv *KVPaxos, args *PutAppendArgs) bool {
	_, isOK := kv.preReqs[args.CurrID]

	//  Duplicate RPC request
	if isOK {
		return true
	}

	return false
}

func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if HasDupA(kv, args) {
		reply.Err = OK
		return nil
	}
	for {

		operation := Op{Key: args.Key, Value: args.Value, Operation: args.Op, CurrID: args.CurrID, PrevID: args.PrevID}
		curSequence := kv.curSequence
		kv.curSequence++

		var opRes Op
		opStatus, opVal := kv.px.Status(curSequence)

		if opStatus == paxos.Decided {
			opRes = opVal.(Op)

		} else {
			opRes = ExecPax(kv, curSequence, operation)
		}
		FinishPrev(kv, args.PrevID)

		ExecOp(kv, opRes)

		kv.px.Done(curSequence)

		if opRes.CurrID == args.CurrID {
			break
		}
	}
	reply.Err = OK
	return nil
}

// tell the server to shut itself down.
// please do not change these two functions.
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *KVPaxos) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *KVPaxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *KVPaxos) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me

	// Your initialization code here.

	kv.curSequence = 0
	kv.myData = make(map[string]string)
	kv.preReqs = make(map[int64]string)

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
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}
