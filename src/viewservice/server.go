package viewservice

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     int32 // for testing
	rpccount int32 // for testing
	me       string

	// Your declarations here.
	currentView  View
	lastPings    map[string]time.Time //changed to last pings
	idleServ     string
	primaryAcked bool
}

/*
//handles response when primary fails
//assuming that lock is acquired in caller
func handlePrimaryDead(vs *ViewServer) {
	log.Printf("Detected dead primary: %s", vs.currentView.Primary)
	vs.nextView.Primary = vs.currentView.Backup
	//viewnum of nextview set in handlebackup
	log.Printf("Replacing dead primary with: %s", vs.nextView.Primary)
	handleBackupDead(vs)
}

//handles response where backup fails
//assumes caller has lock
func handleBackupDead(vs *ViewServer) {
	log.Printf("Detected dead backup: %s", vs.currentView.Backup)
	if len(vs.idleServ) > 0 {
		for k := range vs.idleServ {
			vs.nextView.Backup = k
			delete(vs.idleServ, k)
			break
		}
	} else {
		vs.nextView.Backup = ""
	}
	log.Printf("Replacing dead backup with: %s", vs.nextView.Backup)
	//IMPORTANT: this may not work as a signal to update currentview to nextview
	//special cases where multiple failures occur may not be ahndled properly, result in skipping viewnum
	vs.nextView.Viewnum = vs.currentView.Viewnum + 1
}
*/
//handles checking viewnum and acked num and whether or not to update currentview to next
//assumes caller has lock
func updateView(vs *ViewServer, primary string, backup string) {

	vs.currentView.Viewnum = 1 + vs.currentView.Viewnum
	//may need to set some nextview vars in here first, probably not
	vs.currentView.Primary = primary
	vs.currentView.Backup = backup
	vs.primaryAcked = false
	vs.idleServ = ""
	log.Printf("Updated view: current viewnum: %d\n", vs.currentView.Viewnum)

	log.Printf("Updated view: current primary is: %s\n\t\t\t\t  current backup is %s\n \n", vs.currentView.Primary, vs.currentView.Backup)

}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	vs.mu.Lock()
	serv, currViewNum := args.Me, args.Viewnum
	vs.lastPings[serv] = time.Now()

	if serv == vs.currentView.Primary {
		if currViewNum == 0 && vs.primaryAcked == true { //  Primary crashed
			//log.Printf("Calling updateview from ping: primary restarted")
			updateView(vs, vs.currentView.Backup, vs.idleServ)
		} else if currViewNum == vs.currentView.Viewnum { //  Primary ACKs current view
			vs.primaryAcked = true
		}
	} else if serv == vs.currentView.Backup { //  Pings from backup
		if currViewNum == 0 && vs.primaryAcked == true { // Backup crashed
			//log.Printf("Calling updateview from ping: backup crashed")
			updateView(vs, vs.currentView.Primary, vs.idleServ)
		}
	} else { //  Pings from other
		if vs.currentView.Viewnum == 0 { //  First time, so make primary
			//log.Printf("Calling updateview from ping: setting intial primary")
			updateView(vs, serv, "")
		} else { //  Make the new idle server
			vs.idleServ = serv
		}
	}

	reply.View = vs.currentView
	vs.mu.Unlock()
	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {
	//looks good-Suvir
	vs.mu.Lock()
	log.Printf("Get: current primary is: %s\n\t\t\t current backup is %s", vs.currentView.Primary, vs.currentView.Backup)
	reply.View = vs.currentView
	vs.mu.Unlock()

	return nil
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {
	vs.mu.Lock()

	//update view: if time.now - lastping > deadpings * pinginterval, dead
	currTime := time.Now()
	primTime := vs.lastPings[vs.currentView.Primary]
	backTime := vs.lastPings[vs.currentView.Backup]
	idleTime := vs.lastPings[vs.idleServ]

	//check all idle servers for timeout
	if currTime.Sub(idleTime) >= PingInterval*DeadPings {
		vs.idleServ = ""
	} else if vs.primaryAcked == true && vs.idleServ != "" && vs.currentView.Backup == "" {
		//log.Printf("Calling updateview from tick, setting backup to idle")
		updateView(vs, vs.currentView.Primary, vs.idleServ)

	}

	if currTime.Sub(primTime) >= PingInterval*DeadPings {
		if vs.primaryAcked == true {
			//log.Printf("Calling updateview from tick, primary timed out")
			updateView(vs, vs.currentView.Backup, vs.idleServ)
		}
	}

	if currTime.Sub(backTime) >= PingInterval*DeadPings {
		if vs.primaryAcked == true && vs.idleServ != "" {
			//log.Printf("Calling updateview from tick, backup timed out")
			updateView(vs, vs.currentView.Primary, vs.idleServ)
		}
	}

	vs.mu.Unlock()
}

//
// tell the server to shut itself down.
// for testing.
// please don't change these two functions.
//
func (vs *ViewServer) Kill() {
	atomic.StoreInt32(&vs.dead, 1)
	vs.l.Close()
}

//
// has this server been asked to shut down?
//
func (vs *ViewServer) isdead() bool {
	return atomic.LoadInt32(&vs.dead) != 0
}

// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.

	vs.currentView = View{0, "", ""}
	vs.idleServ = ""
	vs.lastPings = make(map[string]time.Time)
	vs.primaryAcked = false

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.isdead() == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.isdead() == false {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.isdead() == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.isdead() == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
