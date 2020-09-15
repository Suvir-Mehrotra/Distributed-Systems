package pbservice

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"viewservice"
)

type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	me         string
	vs         *viewservice.Clerk
	// Your declarations here.
	currView   viewservice.View
	pastReqs   map[int64]KeyVal
	myData     map[string]string
	dataSynced bool
}
type KeyVal struct {
	Key   string
	Value string
	Op    string
}

func IsDuplicateReq(pb *PBServer, args *GetArgs, reply *GetReply) bool {
	currKey, currID := args.Key, args.ID
	prevReq, isOK := pb.pastReqs[currID]
	if isOK && prevReq.Key == currKey {
		return true
	}
	return false
}

func (pb *PBServer) ProcessReq(args *GetArgs, reply *GetReply) error {
	currVal, isOK := pb.myData[args.Key]
	if isOK {
		reply.Value = currVal
		reply.Err = OK
	} else {
		reply.Err = ErrNoKey
	}

	pb.pastReqs[args.ID] = KeyVal{Key: args.Key, Value: "", Op: "Get"}

	return nil
}

func (pb *PBServer) SendReqToBackup(args *GetArgs, reply *GetReply) error {

	if pb.currView.Backup != pb.me {
		reply.Err = ErrWrongServer
		return nil
	}

	if IsDuplicateReq(pb, args, reply) {
		reply.Value = pb.myData[args.Key]
		reply.Err = OK
		return nil
	}

	//  Update backup's database with Get operation
	pb.ProcessReq(args, reply)

	return nil

}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()

	if pb.currView.Primary != pb.me {
		reply.Err = ErrWrongServer
		return nil
	}

	if IsDuplicateReq(pb, args, reply) {
		reply.Value = pb.myData[args.Key]
		reply.Err = OK
		return nil
	}

	pb.ProcessReq(args, reply)

	if pb.currView.Backup != "" {

		isOK := call(pb.currView.Backup, "PBServer.SendReqToBackup", args, &reply)

		if !isOK || reply.Value != pb.myData[args.Key] || reply.Err == ErrWrongServer {
			pb.dataSynced = true
		}
	}

	return nil
}

func (pb *PBServer) ProcessReqApp(args *PutAppendArgs, reply *PutAppendReply) error {
	currKey, currValue := args.Key, args.Value
	if args.Op == "Put" {
		pb.myData[currKey] = currValue
	} else if args.Op == "Append" {
		prevReq := pb.myData[currKey]
		pb.myData[currKey] = prevReq + currValue
	}

	pb.pastReqs[args.ID] = KeyVal{Key: args.Key, Value: args.Value, Op: args.Op}

	reply.Err = OK
	return nil
}

func IsDupReqApp(pb *PBServer, args *PutAppendArgs, reply *PutAppendReply) bool {
	currKey, currVal, currOp, currID := args.Key, args.Value, args.Op, args.ID
	prevReq, isOK := pb.pastReqs[currID]

	if isOK && prevReq.Key == currKey && prevReq.Value == currVal && prevReq.Op == currOp {
		return true
	}

	return false
}

func (pb *PBServer) SendAppendToBackup(args *PutAppendArgs, reply *PutAppendReply) error {

	if pb.currView.Backup != pb.me {
		reply.Err = ErrWrongServer
		return nil
	}

	if IsDupReqApp(pb, args, reply) {
		reply.Err = OK
		return nil
	}

	pb.ProcessReqApp(args, reply)

	return nil
}

func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {

	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()
	if pb.currView.Primary != pb.me {
		reply.Err = ErrWrongServer
		return nil
	}

	if IsDupReqApp(pb, args, reply) {
		reply.Err = OK
		return nil
	}

	pb.ProcessReqApp(args, reply)

	if pb.currView.Backup != "" {
		//  Send an RPC request, wait for the reply
		isOK := call(pb.currView.Backup, "PBServer.SendAppendToBackup", args, &reply)

		if !isOK || pb.myData[args.Key] != args.Value || reply.Err != OK {
			pb.dataSynced = true
		}
	}

	return nil
}

//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//

func (pb *PBServer) DataToBackup(args *DataToBackupArgs,
	reply *DataToBackupReply) error {

	newView, err := pb.vs.Ping(pb.currView.Viewnum)
	if err != nil {
		fmt.Errorf("Ping(%v) failed", pb.currView.Viewnum)
	}

	if newView.Backup != pb.me {
		reply.Err = ErrWrongServer
		return nil
	}

	pb.myData = args.MyData
	pb.pastReqs = args.PastReqs

	reply.Err = OK
	return nil
}

func (pb *PBServer) tick() {

	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()
	newView, err := pb.vs.Ping(pb.currView.Viewnum)
	if err != nil {
		fmt.Errorf("Ping(%v) failed", pb.currView.Viewnum)
	}
	if newView.Primary == pb.me && pb.currView.Backup != newView.Backup && newView.Backup != "" {
		pb.dataSynced = true
	}
	if pb.dataSynced == true {
		pb.dataSynced = false
		args := &DataToBackupArgs{MyData: pb.myData, PastReqs: pb.pastReqs}
		var reply DataToBackupReply

		isOK := call(newView.Backup, "PBServer.DataToBackup", args, &reply)
		//  If something went wrong, then sync up on next tick
		if !isOK || reply.Err != OK {
			pb.dataSynced = true
		}
	}
	pb.currView = newView

}

// tell the server to shut itself down.
// please do not change these two functions.
func (pb *PBServer) kill() {
	atomic.StoreInt32(&pb.dead, 1)
	pb.l.Close()
}

// call this to find out if the server is dead.
func (pb *PBServer) isdead() bool {
	return atomic.LoadInt32(&pb.dead) != 0
}

// please do not change these two functions.
func (pb *PBServer) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&pb.unreliable, 1)
	} else {
		atomic.StoreInt32(&pb.unreliable, 0)
	}
}

func (pb *PBServer) isunreliable() bool {
	return atomic.LoadInt32(&pb.unreliable) != 0
}

func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	// Your pb.* initializations here.
	pb.currView = viewservice.View{Viewnum: 0, Primary: "", Backup: ""}
	pb.myData = make(map[string]string)
	pb.pastReqs = make(map[int64]KeyVal)
	pb.dataSynced = false

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.isdead() == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.isdead() == false {
				if pb.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && pb.isdead() == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
	}()

	go func() {
		for pb.isdead() == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
	}()

	return pb
}
