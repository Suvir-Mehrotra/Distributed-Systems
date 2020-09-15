package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (Fate, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

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
)

// px.Status() return values, indicating
// whether an agreement has been decided,
// or Paxos has not yet reached agreement,
// or it was agreed but forgotten (i.e. < Min()).
type Fate int
type Err string

const (
	Decided            Fate = iota + 1
	Pending                 // not yet decided.
	Forgotten               // decided but forgotten.
	ErrPrepareRejected = "prepare-reject"
	ErrAcceptRejected  = "accept-reject"
	ErrDecided         = "decided"
	OK                 = "ok"
)

type PrepareArgs struct {
	Seq      int
	N        int
	Done     int
	FromPeer int
}

type PrepareReply struct {
	Err  Err
	N    int
	Na   int
	Np   int
	Va   interface{}
	Done int
}

type AcceptArgs struct {
	Seq      int
	N        int
	Vprime   interface{}
	Done     int
	FromPeer int
}

type AcceptReply struct {
	Err  Err
	N    int
	Done int
}

type LearnArgs struct {
	Seq      int
	Na       int
	Va       interface{}
	Done     int
	FromPeer int
}

type LearnReply struct {
	Err  Err
	Done int
}

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	rpcCount   int32 // for testing
	peers      []string
	me         int // index into peers[]

	// Your data here.
	numPeers      int
	majorityCount int
	//define a map from seq to a struct containing:
	//highest prepare seen, highest accept seen, value accepted (by this peer), and decided state
	instances map[int]*Instance

	//done seq number for all peers
	peersDone []int

	//int for max seq number in use
	maxSeq int
}

// Data needed for each instance of Paxos
// Fine grained locking so multiple instances can run concurrently more easily (may cause problems with delete, look later)
type Instance struct {
	//mu      sync.Mutex
	np      int
	na      int
	va      interface{}
	decided bool
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			//fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	// Your code here.

	min := px.Min()
	px.mu.Lock()
	if px.instances[seq] == nil && seq >= min {
		px.CreateInstance(seq)
	}
	px.mu.Unlock()

	status, _ := px.Status(seq)
	if status == Decided || status == Forgotten {
		return
	}
	go px.Proposer(seq, v)
	//execute goroutine to start agreement process so this can return immediately
	//goroutine needs to propose value from this peer, also run acceptor and learner?
	//actual paxos agreement for single instance should be run by this
	//goroutine also needs to transmit done value for each peer in the messages and update min and delete from map
	//set na to 0
}

func (px *Paxos) Proposer(seq int, v interface{}) {
	largestNp := 0

	px.mu.Lock()
	valid := px.instances[seq] != nil && px.instances[seq].decided == false && !px.isdead()
	px.mu.Unlock()

	for valid {
		largestNp++

		n := (largestNp)*px.numPeers + px.me

		Vprime := v
		largestNa := -1
		prepareCount := 0
		px.mu.Lock()
		prepareArgs := PrepareArgs{seq, n, px.peersDone[px.me], px.me}
		px.mu.Unlock()
		prepareReply := PrepareReply{}
		for i, p := range px.peers {
			if i == px.me {
				px.Prepare(&prepareArgs, &prepareReply)
			} else {
				isOK := call(p, "Paxos.Prepare", &prepareArgs, &prepareReply)
				if !isOK {
					//rpc call error (network)
					continue
				}

				px.mu.Lock()
				px.UpdatePeersDone(i, prepareReply.Done)
				px.mu.Unlock()
			}

			if prepareReply.Err == ErrDecided {
				px.Decide(seq, prepareReply.Na, prepareReply.Va)

				return
			} else if prepareReply.Err == OK {
				prepareCount++
				if prepareReply.Na != -1 && prepareReply.Na > largestNa {
					Vprime = prepareReply.Va
					largestNa = prepareReply.Na
				}
			} else {
				//increase max prepare seen
				if prepareReply.Np > n {
					largestNp = prepareReply.Np / px.numPeers
				}
			}
		}

		if prepareCount < px.majorityCount {
			px.mu.Lock()
			valid = px.instances[seq] != nil && px.instances[seq].decided == false && !px.isdead()
			px.mu.Unlock()
			continue
		}

		acceptCount := 0
		px.mu.Lock()
		acceptArgs := AcceptArgs{seq, n, Vprime, px.peersDone[px.me], px.me}
		px.mu.Unlock()
		acceptReply := AcceptReply{}
		for i, p := range px.peers {
			if i == px.me {
				px.Accept(&acceptArgs, &acceptReply)
			} else {
				isOK := call(p, "Paxos.Accept", &acceptArgs, &acceptReply)
				if !isOK {
					//rpc call error (network)
					continue
				}
				px.mu.Lock()
				px.UpdatePeersDone(i, acceptReply.Done)
				px.mu.Unlock()
			}

			if acceptReply.Err == OK && acceptReply.N == n {
				acceptCount++
			}

		}

		if acceptCount < px.majorityCount {
			px.mu.Lock()
			valid = px.instances[seq] != nil && px.instances[seq].decided == false && !px.isdead()
			px.mu.Unlock()
			continue
		}

		//learnCount := 0
		px.Decide(seq, n, Vprime)

		px.mu.Lock()
		valid = px.instances[seq] != nil && px.instances[seq].decided == false && !px.isdead()
		px.mu.Unlock()
	}

	px.mu.Lock()
	px.ForgetDone()
	px.mu.Unlock()
}

//need to decide how to send done for each peer through these rpcs, or just send min

//define rpc for acceptor prepare (takes instance and n, replies with ok, n, na, va or reject)
func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
	seq, n, from, done := args.Seq, args.N, args.FromPeer, args.Done

	px.mu.Lock()
	defer px.mu.Unlock()

	if px.instances[seq] == nil {
		px.CreateInstance(seq)
	}
	if px.instances[seq].decided {
		reply.Err = ErrDecided
		reply.Na = px.instances[seq].na
		reply.Va = px.instances[seq].va
	} else if n > px.instances[seq].np {
		px.instances[seq].np = n
		reply.Err = OK
		reply.N = n
		reply.Na = px.instances[seq].na
		reply.Va = px.instances[seq].va
	} else {
		reply.Err = ErrPrepareRejected
		//used to help proposer find new highest np
		reply.Np = px.instances[seq].np
	}

	px.UpdatePeersDone(from, done)
	reply.Done = px.peersDone[px.me]
	px.ForgetDone()
	return nil
}

func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {

	seq, n, v, from, done := args.Seq, args.N, args.Vprime, args.FromPeer, args.Done
	px.mu.Lock()
	defer px.mu.Unlock()

	if px.instances[seq] == nil {
		px.CreateInstance(seq)
	}

	if px.instances[seq].decided {
		reply.Err = ErrDecided
	} else if n >= px.instances[seq].np {
		px.instances[seq].np = n
		px.instances[seq].na = n
		px.instances[seq].va = v

		reply.Err = OK
		reply.N = n
	} else {
		reply.Err = ErrAcceptRejected
	}

	px.UpdatePeersDone(from, done)
	reply.Done = px.peersDone[px.me]
	px.ForgetDone()
	return nil
}

func (px *Paxos) Learn(args *LearnArgs, reply *LearnReply) error {

	seq, n, v, from, done := args.Seq, args.Na, args.Va, args.FromPeer, args.Done
	px.mu.Lock()
	defer px.mu.Unlock()
	if px.instances[seq] == nil {
		px.CreateInstance(seq)
	}
	if !px.instances[seq].decided {
		px.instances[seq].na = n
		px.instances[seq].va = v
		px.instances[seq].decided = true
	}
	px.UpdatePeersDone(from, done)
	reply.Done = px.peersDone[px.me]
	px.ForgetDone()
	return nil
}

//assume caller has lock
func (px *Paxos) CreateInstance(seq int) {

	//create instance and update max if necessary

	px.instances[seq] = &Instance{-1, -1, nil, false}
	if seq > px.maxSeq {
		px.maxSeq = seq
	}
}

//assume caller has lock
func (px *Paxos) UpdatePeersDone(peer int, doneSeq int) {
	if doneSeq > px.peersDone[peer] {
		px.peersDone[peer] = doneSeq
	}
}

//assume caller has lock
func (px *Paxos) ForgetDone() {
	minDone := px.peersDone[0]
	for _, doneSeq := range px.peersDone {
		if doneSeq < minDone {
			minDone = doneSeq
		}
	}

	if minDone > -1 {
		for i := 0; i <= minDone; i++ {
			delete(px.instances, i)
		}
	}
}

func (px *Paxos) Decide(seq int, n int, v interface{}) {
	px.mu.Lock()
	learnArgs := LearnArgs{seq, n, v, px.peersDone[px.me], px.me}
	px.mu.Unlock()

	learnReply := LearnReply{}
	for i, p := range px.peers {

		if i == px.me {
			px.Learn(&learnArgs, &learnReply)
		} else {
			isOK := call(p, "Paxos.Learn", &learnArgs, &learnReply)
			if !isOK {
				continue
			}

			px.mu.Lock()
			px.UpdatePeersDone(i, learnReply.Done)
			px.mu.Unlock()
		}
	}
}

// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	// Your code here.
	//broadcast highest done seq to other peers
	//find minimum sequence number still in use, discard all lower instances
	//(maybe just update local state here and broadcast somewhere else?)

	px.mu.Lock()
	defer px.mu.Unlock()
	px.peersDone[px.me] = seq
	px.ForgetDone()
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	// Your code here.
	// probably just return the max seq number from local state?

	px.mu.Lock()
	defer px.mu.Unlock()
	return px.maxSeq
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
	// You code here.
	// return min seq number from local state, it will be updated by the single instance paxos goroutine run by start()

	px.mu.Lock()
	defer px.mu.Unlock()
	min := px.peersDone[0]
	for _, doneSeq := range px.peersDone {
		if doneSeq < min {
			min = doneSeq
		}
	}
	return min + 1
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	// Your code here.
	// check if seq number is less than min, return forgotten if it is
	// check map for struct for given seq, if agreed value is nil, return pending with agreed value
	// otherwise return decided and agreed value for seq

	min := px.Min()
	px.mu.Lock()
	defer px.mu.Unlock()
	if seq < min {
		return Forgotten, nil
	} else if px.instances[seq] != nil {
		if px.instances[seq].decided {
			return Decided, px.instances[seq].va
		}
	}
	return Pending, nil
}

//
// tell the peer to shut itself down.
// for testing.
// please do not change these two functions.
//
func (px *Paxos) Kill() {
	atomic.StoreInt32(&px.dead, 1)
	if px.l != nil {
		px.l.Close()
	}
}

//
// has this peer been asked to shut down?
//
func (px *Paxos) isdead() bool {
	return atomic.LoadInt32(&px.dead) != 0
}

// please do not change these two functions.
func (px *Paxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&px.unreliable, 1)
	} else {
		atomic.StoreInt32(&px.unreliable, 0)
	}
}

func (px *Paxos) isunreliable() bool {
	return atomic.LoadInt32(&px.unreliable) != 0
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me

	// Your initialization code here.
	px.numPeers = len(px.peers)
	px.majorityCount = (px.numPeers)/2 + 1
	px.maxSeq = -1
	px.instances = make(map[int]*Instance)
	px.peersDone = make([]int, px.numPeers)
	for i := range px.peersDone {
		px.peersDone[i] = -1
	}

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.isdead() == false {
				conn, err := px.l.Accept()
				if err == nil && px.isdead() == false {
					if px.isunreliable() && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.isunreliable() && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					} else {
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.isdead() == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	return px
}
