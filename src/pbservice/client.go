package pbservice

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"net/rpc"
	"time"
	"viewservice"
)

type Clerk struct {
	vs *viewservice.Clerk
	// Your declarations here
	// UUID per client if using UUID per client instead of per transaction
	currView viewservice.View
}

// this may come in handy.
func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(vshost string, me string) *Clerk {
	ck := new(Clerk)
	ck.vs = viewservice.MakeClerk(me, vshost)
	// Your ck.* initializations here
	//either set UUID per client here if we can assume that only one transaction per client at a time
	//or send a new UUID per transaction in the RPC calls
	//i think we can assume only one transaction at a time so probably UUID per client set here
	ck.currView, _ = ck.vs.Get()
	return ck
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will return an
// error after a while if the server is dead.
// don't provide your own time-out mechanism.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

//
// fetch a key's value from the current primary;
// if they key has never been set, return "".
// Get() must keep trying until it either the
// primary replies with the value or the primary
// says the key doesn't exist (has never been Put().
//
func (ck *Clerk) Get(key string) string {
	// Your code here.

	//  Prepare the arguments
	args := &GetArgs{Key: key, ID: nrand()} //  generate unique request ID to handle dup requests
	var reply GetReply

	//  Send an RPC request, wait for the reply
	ok := call(ck.currView.Primary, "PBServer.Get", args, &reply)

	//  Keep retrying until we get an answer
	for ok == false || reply.Err == ErrWrongServer {
		time.Sleep(viewservice.PingInterval)
		ck.currView, _ = ck.vs.Get()
		ok = call(ck.currView.Primary, "PBServer.Get", args, &reply)
	}

	if reply.Err == ErrNoKey {
		return ""
	}

	return reply.Value
}

//
// send a Put or Append RPC
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// similar to get in structure
	args := &PutAppendArgs{Key: key, Value: value, Op: op, ID: nrand()} //  generate unique request ID to handle dup requests
	var reply PutAppendReply

	//  Send an RPC request, wait for the reply
	ok := call(ck.currView.Primary, "PBServer.PutAppend", args, &reply)

	//  Keep retrying until we get an answer
	for ok == false || reply.Err != OK {
		time.Sleep(viewservice.PingInterval)
		ck.currView, _ = ck.vs.Get()
		ok = call(ck.currView.Primary, "PBServer.PutAppend", args, &reply)
	}
}

//
// tell the primary to update key's value.
// must keep trying until it succeeds.
//
func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

//
// tell the primary to append to key's value.
// must keep trying until it succeeds.
//
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
