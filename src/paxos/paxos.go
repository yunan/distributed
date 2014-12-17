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
// px.Status(seq int) (decided bool, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import (
	"bytes"
	"encoding/gob"
	"net"
	"strconv"
	"time"

	"github.com/jmhodges/levigo"
)
import "net/rpc"
import "log"
import "os"
import "syscall"
import "sync"
import "fmt"
import "math/rand"

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       bool
	unreliable bool
	rpcCount   int
	peers      []string
	me         int // index into peers[]

	// Your data here.
	ins  map[int]*instances
	done []int

	db             *levigo.DB
	dbLock         sync.Mutex
	dbReadOptions  *levigo.ReadOptions
	dbWriteOptions *levigo.WriteOptions
}

type instances struct {
	Decide bool
	N_p    int64       //(highest prepare seen)
	N_a    int64       // (highest accept seen)
	V_a    interface{} //(highest accept seen)
	Prop   Proposal
}

type Proposal struct {
	Num int64
	Val interface{}
}

type PrepareArg struct {
	Num int64
	Seq int
}

type AcceptArg struct {
	Num int64
	Seq int
	Val interface{}
}

type DecisionArg struct {
	Seq  int
	Num  int64
	Val  interface{}
	Done int
	Me   int
}

const (
	OK      = "OK"
	REJECT  = "REJECT"
	persist = true
	NetWork = false
)

type PaxosReply struct {
	Result string
	N_a    int64
	V_a    interface{}
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
	var c *rpc.Client
	var err error
	if NetWork {
		c, err = rpc.Dial("tcp", srv)
	} else {
		c, err = rpc.Dial("unix", srv)
	}
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			///			fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	//	log.Printf("call %s %s\n", srv, name)
	err = c.Call(name, args, reply)
	//fmt.Printf("call ok\n")
	if err == nil {
		return true
	}

	//	fmt.Println(err)
	return false
}

/*

pseudo-code (for a single instance) from the lecture:


pseudo-code (for a single instance) from the lecture:


proposer(v):
  while not decided:
    choose n, unique and higher than any n seen so far
    send prepare(n) to all servers including self
    if prepare_ok(N_a, V_a) from majority:
      v' = V_a with highest N_a; choose own v otherwise
      send accept(n, v') to all
      if accept_ok(n) from majority:
        send decided(v') to all

acceptor's state:
  N_p (highest prepare seen)
  N_a, V_a (highest accept seen)

acceptor's prepare(n) handler:
  if n > N_p
    N_p = n
    reply prepare_ok(N_a, V_a)
  else
    reply prepare_reject

acceptor's accept(n, v) handler:
  if n >= N_p
    N_p = n
    N_a = n
    V_a = v
    reply accept_ok(n)
  else
    reply accept_reject


*/

func (px *Paxos) makeins(seq int) {
	px.ins[seq] = &instances{Decide: false, N_p: -1, N_a: -1, V_a: -1, Prop: Proposal{0, nil}}
}

func (px *Paxos) getins(seq int) {
	if persist {
		//fmt.Println("getins")
		px.dbLock.Lock()
		defer px.dbLock.Unlock()
		if px.dead {
			return
		}

		key := "ins_" + strconv.Itoa(seq)
		//fmt.Println("getins", key, px.db)
		keyBytes, err := px.db.Get(px.dbReadOptions, []byte(key))
		if err == nil && len(keyBytes) > 0 {
			buffer := *bytes.NewBuffer(keyBytes)
			decoder := gob.NewDecoder(&buffer)
			var val instances
			err = decoder.Decode(&val)
			if err != nil {
				log.Println("decode err", err)
			} else {
				px.ins[seq] = &val
				//	fmt.Printf("%#v", &val)
			}
		} else {
			if err != nil {
				log.Println("get ins err ", err)
			}
		}
	}
	if _, exist := px.ins[seq]; !exist {
		px.makeins(seq)
	}
}

func (px *Paxos) writeins(seq int) {
	if !persist {
		return
	}
	//fmt.Println("writeins")
	px.dbLock.Lock()
	defer px.dbLock.Unlock()
	if px.dead {
		return
	}

	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	err := enc.Encode(px.ins[seq])
	if err != nil {
		log.Println(err)
	} else {
		key := "ins_" + strconv.Itoa(seq)
		err := px.db.Put(px.dbWriteOptions, []byte(key), buffer.Bytes())
		if err != nil {
			log.Println("\terror writing to database", err)
		}
	}
}

func (px *Paxos) writedone() {
	if !persist {
		return
	}
	px.dbLock.Lock()
	defer px.dbLock.Unlock()
	if px.dead {
		return
	}
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	err := enc.Encode(px.done)
	if err != nil {
		log.Println(err)
	} else {
		key := "done"
		err := px.db.Put(px.dbWriteOptions, []byte(key), buffer.Bytes())
		if err != nil {
			log.Println("\terror writing to database", err)
		}
	}
}

func (px *Paxos) getdone() {
	if !persist {
		return
	}
	px.dbLock.Lock()
	defer px.dbLock.Unlock()

	if px.dead {
		return
	}

	doneBytes, err := px.db.Get(px.dbReadOptions, []byte("done"))
	if err == nil && len(doneBytes) > 0 {
		buffer := *bytes.NewBuffer(doneBytes)
		decoder := gob.NewDecoder(&buffer)
		var doneDecoded []int
		err = decoder.Decode(&doneDecoded)
		if err != nil {
			log.Println("decode err", err)
		} else {
			for peer, doneVal := range doneDecoded {
				px.done[peer] = doneVal
			}
		}
	} else {
		if err != nil {
			log.Println("get done err ", err)
		}
	}
}

func (px *Paxos) delete(seq int) {
	delete(px.ins, seq)
	if !persist {
		return
	}
	px.dbLock.Lock()
	defer px.dbLock.Unlock()
	if px.dead {
		return
	}

	key := "ins_" + strconv.Itoa(seq)
	err := px.db.Delete(px.dbWriteOptions, []byte(key))
	if err != nil {
		log.Println()
	}
}

func (px *Paxos) dbinit() {
	if !persist {
		return
	}
	px.dbLock.Lock()
	defer px.dbLock.Unlock()
	px.mu.Lock()
	defer px.mu.Unlock()
	if px.dead {
		return
	}
	gob.Register(instances{})

	px.dbReadOptions = levigo.NewReadOptions()
	px.dbWriteOptions = levigo.NewWriteOptions()
	options := levigo.NewOptions()
	options.SetCache(levigo.NewLRUCache(3 << 30))
	options.SetCreateIfMissing(true)
	dbname := "./paxosdb" + strconv.Itoa(rand.Int())
	os.RemoveAll(dbname) //for test
	var err error
	px.db, err = levigo.Open(dbname, options)
	if err != nil {
		log.Println("open db", err)
	}
}

func (px *Paxos) SendPrepare(args *PrepareArg, reply *PaxosReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	//fmt.Printf("sendPrepare %d\n", px.me)

	px.getins(args.Seq)
	if px.dead {
		reply.Result = REJECT
		return nil
	}

	if args.Num > px.ins[args.Seq].N_p {
		px.ins[args.Seq].N_p = args.Num
		px.writeins(args.Seq)
		reply.Result = OK
		reply.N_a = px.ins[args.Seq].N_a
		reply.V_a = px.ins[args.Seq].V_a
	} else {
		reply.Result = REJECT
	}
	//fmt.Printf("%d sendprepare ok\n", px.me)
	return nil
}

func (px *Paxos) SendAccept(args *AcceptArg, reply *PaxosReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	px.getins(args.Seq)
	if px.dead {
		reply.Result = REJECT
		return nil
	}

	if args.Num >= px.ins[args.Seq].N_p {
		px.ins[args.Seq].N_p = args.Num
		px.ins[args.Seq].N_a = args.Num
		px.ins[args.Seq].V_a = args.Val
		px.writeins(args.Seq)
		reply.Result = OK
	} else {
		reply.Result = REJECT
	}
	return nil
}

func (px *Paxos) MakeDecision(args *DecisionArg, reply *PaxosReply) error {
	//fmt.Printf("px.me %d decide seq %d val %v\n", px.me, args.Seq, args.Val)
	px.mu.Lock()
	defer px.mu.Unlock()

	px.getins(args.Seq)
	if px.dead {
		return nil
	}

	px.ins[args.Seq].Decide = true
	px.ins[args.Seq].Prop = Proposal{Num: args.Num, Val: args.Val}
	px.done[args.Me] = args.Done
	px.writeins(args.Seq)
	px.writedone()
	return nil
}

func (px *Paxos) prepare(seq int, n int64, v interface{}) (bool, interface{}) {
	num := 0
	prop := Proposal{-1, v}
	//fmt.Printf("prepare\n")
	for i, peer := range px.peers {
		//		fmt.Printf("%d %s\n", i, peer)
		args := PrepareArg{Num: n, Seq: seq}
		var reply PaxosReply
		if i == px.me {
			px.SendPrepare(&args, &reply)
		} else {
			call(peer, "Paxos.SendPrepare", &args, &reply)
		}
		if reply.Result == OK {
			if reply.N_a > prop.Num {
				prop.Num = reply.N_a
				prop.Val = reply.V_a
			}
			num++
		}
		//		fmt.Printf("aaa %d\n", num)
	}
	//	fmt.Printf("prepare result %d %d\n", num, len(px.peers)/2)
	return num*2 >= len(px.peers), prop.Val
}

func (px *Paxos) accept(seq int, n int64, v interface{}) bool {
	num := 0
	for i, peer := range px.peers {
		args := AcceptArg{Seq: seq, Num: n, Val: v}
		var reply PaxosReply
		if i == px.me {
			px.SendAccept(&args, &reply)
		} else {
			call(peer, "Paxos.SendAccept", &args, &reply)
		}
		if reply.Result == OK {
			num++
		}
	}
	return num*2 >= len(px.peers)

}

func (px *Paxos) decision(seq int, n int64, val interface{}) {
	args := DecisionArg{Seq: seq, Num: n, Val: val, Done: px.done[px.me], Me: px.me}
	var reply PaxosReply
	for i, peer := range px.peers {
		if i == px.me {
			px.MakeDecision(&args, &reply)
		} else {
			call(peer, "Paxos.MakeDecision", &args, &reply)
		}
	}
}

func (px *Paxos) proposer(seq int, v interface{}) {
	//fmt.Printf("proposer %d %d %v\n", px.me, seq, v)
	for {
		decide, _ := px.Status(seq)
		if decide {
			return
		}
		n := time.Now().Unix()
		ok, V_a := px.prepare(seq, n, v)
		//		fmt.Printf("%d %d\n", ok, V_a)
		if ok {
			ok = px.accept(seq, n, V_a)
		}
		if ok {
			px.decision(seq, n, V_a)
		}
	}
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
	go func() {
		if seq < px.Min() {
			return
		}
		decide, _ := px.Status(seq)
		if decide {
			return
		}
		px.proposer(seq, v)
	}()
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	// Your code here.
	px.getdone()
	if px.done[px.me] < seq {
		px.done[px.me] = seq
		px.writedone()
	}
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	// Your code here.
	max := 0
	for k, _ := range px.ins {
		if k > max {
			max = k
		}
	}
	return max
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
	px.mu.Lock()
	defer px.mu.Unlock()
	px.getdone()
	min := px.done[px.me]
	for k := range px.done {
		if min > px.done[k] {
			min = px.done[k]
		}
	}
	for k, _ := range px.ins {
		if k <= min && px.ins[k].Decide {
			px.delete(k)
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
func (px *Paxos) Status(seq int) (bool, interface{}) {
	// Your code here.
	px.mu.Lock()
	defer px.mu.Unlock()
	px.getins(seq)
	if _, exist := px.ins[seq]; !exist {
		return false, nil
	}
	return px.ins[seq].Decide, px.ins[seq].Prop.Val
}

//
// tell the peer to shut itself down.
// for testing.
// please do not change this function.
//
func (px *Paxos) Kill() {
	if px.dead {
		return
	}
	px.dead = true
	if px.l != nil {
		px.l.Close()
	}
	if persist {
		px.dbLock.Lock()
		px.db.Close()
		px.dbReadOptions.Close()
		px.dbWriteOptions.Close()
		px.dbLock.Unlock()
	}
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
	px.ins = map[int]*instances{}
	px.done = make([]int, len(peers))
	for k, _ := range px.done {
		px.done[k] = -1
	}
	px.dbinit()
	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		var l net.Listener
		var e error
		if NetWork {
			l, e = net.Listen("tcp", peers[me])
			if e != nil {
				log.Fatal("listen error: ", e)
			}
		} else {
			l, e = net.Listen("unix", peers[me])
			if e != nil {
				log.Fatal("listen error: ", e)
			}
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.dead == false {
				conn, err := px.l.Accept()
				if err == nil && px.dead == false {
					if px.unreliable && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.unreliable && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						px.rpcCount++
						go rpcs.ServeConn(conn)
					} else {
						px.rpcCount++
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.dead == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	return px
}
