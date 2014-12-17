package shardmaster

import (
	"bytes"
	"net"
	"time"

	"github.com/jmhodges/levigo"
)
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import (
	"strconv"
	"sync"
)
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"

const (
	SMLeave = "Leave"
	SMJoin  = "Join"
	SMMove  = "Move"
	SMQuery = "Query"
	persist = true
	Network = false
)

type ShardMaster struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       bool // for testing
	unreliable bool // for testing
	px         *paxos.Paxos

	configs   []Config // indexed by config num
	process   int
	confignum int

	db             *levigo.DB
	dbLock         sync.Mutex
	dbReadOptions  *levigo.ReadOptions
	dbWriteOptions *levigo.WriteOptions
}

type Op struct {
	// Your data here.
	Type    string
	Uid     string
	GID     int64
	Shard   int
	Servers []string
	Num     int
}

/*
func (sm *ShardMaster) reblance() {
	k := 0
	for k < NShards {
		for i, _ := range sm.configs[sm.confignum].Groups {
			sm.configs[sm.confignum].Shards[k] = i
			k++
			if k >= NShards {
				break
			}
		}
	}
}
*/

func (sm *ShardMaster) WriteConf(confignum int) {
	if !persist {
		return
	}
	sm.dbLock.Lock()
	defer sm.dbLock.Unlock()
	var buffer bytes.Buffer
	if sm.dead {
		return
	}
	enc := gob.NewEncoder(&buffer)
	err := enc.Encode(sm.configs[confignum])
	//fmt.Println("write_conf_", confignum, sm.configs[confignum])
	if err != nil {
		log.Println(err)
	} else {
		key := "config_" + strconv.Itoa(confignum)
		err := sm.db.Put(sm.dbWriteOptions, []byte(key), buffer.Bytes())
		if err != nil {
			log.Println("\terror writing to database", err)
		}
	}
}

func (sm *ShardMaster) GetConf(confignum int) {
	if persist {
		//fmt.Println("get_config_", confignum)
		sm.dbLock.Lock()
		defer sm.dbLock.Unlock()
		if sm.dead {
			return
		}
		key := "config_" + strconv.Itoa(confignum)
		keyBytes, err := sm.db.Get(sm.dbReadOptions, []byte(key))
		if err == nil && len(keyBytes) > 0 {
			buffer := *bytes.NewBuffer(keyBytes)
			decoder := gob.NewDecoder(&buffer)
			var val Config
			err = decoder.Decode(&val)
			if err != nil {
				log.Println("decode err", err)
			} else {
				sm.configs[confignum] = val
				//fmt.Println(val)
			}
		} else {
			log.Println("get config err ", err)
		}
	}
}

func (sm *ShardMaster) WriteString(key string, val int) {
	if !persist {
		return
	}
	sm.dbLock.Lock()
	defer sm.dbLock.Unlock()
	if sm.dead {
		return
	}
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	err := enc.Encode(val)
	if err != nil {
		log.Println(err)
	} else {
		err := sm.db.Put(sm.dbWriteOptions, []byte(key), buffer.Bytes())
		if err != nil {
			log.Println("\terror writing to database", err)
		}
	}
}

func (sm *ShardMaster) GetString(key string) int {
	if !persist {
		return 0
	}
	sm.dbLock.Lock()
	defer sm.dbLock.Unlock()
	if sm.dead {
		return 0
	}
	keyBytes, err := sm.db.Get(sm.dbReadOptions, []byte(key))
	if err == nil && len(keyBytes) > 0 {
		buffer := *bytes.NewBuffer(keyBytes)
		decoder := gob.NewDecoder(&buffer)
		var val int
		err = decoder.Decode(&val)
		if err != nil {
			log.Println("decode err", err)
		} else {
			return val
		}
	} else {
		log.Println("get done err ", err)
	}
	return 0
}

func (sm *ShardMaster) dbinit() {
	if !persist {
		return
	}
	sm.dbLock.Lock()
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if sm.dead {
		return
	}
	gob.Register(Config{})

	sm.dbReadOptions = levigo.NewReadOptions()
	sm.dbWriteOptions = levigo.NewWriteOptions()
	options := levigo.NewOptions()
	options.SetCache(levigo.NewLRUCache(3 << 30))
	options.SetCreateIfMissing(true)
	dbname := "./shardmasterdb" + strconv.Itoa(sm.me)
	os.RemoveAll(dbname) // for test,
	var err error
	sm.db, err = levigo.Open(dbname, options)
	if err != nil {
		log.Println("open db", err)
	}
	sm.dbLock.Unlock()
	sm.confignum = sm.GetString("confignum")
	sm.process = sm.GetString("process")
	sm.WriteConf(0)
	//log.Println("db init success")
}

func (sm *ShardMaster) newconf() {
	//	fmt.Println(sm.confignum)
	sm.GetConf(sm.confignum)
	old := sm.configs[sm.confignum]
	var newconfig Config
	sm.confignum++
	sm.WriteString("confignum", sm.confignum)
	newconfig.Num = sm.confignum
	newconfig.Groups = map[int64][]string{}
	for i, v := range old.Shards {
		newconfig.Shards[i] = v
	}
	for i, v := range old.Groups {
		newconfig.Groups[i] = v
	}
	sm.configs = append(sm.configs, newconfig)
	sm.WriteConf(sm.confignum)
}

func (sm *ShardMaster) find_m() (int, int64, int, int64) {
	counts := map[int64]int{}
	//sm.GetConf(sm.confignum)
	c := sm.configs[sm.confignum]
	for _, g := range c.Shards {
		counts[g] += 1
	}
	min := 257
	max := 0
	var minx, maxx int64
	for g, _ := range c.Groups {
		if counts[g] > max {
			max = counts[g]
			maxx = g
		}
		if counts[g] < min {
			min = counts[g]
			minx = g
		}
	}
	return min, minx, max, maxx
}

func (sm *ShardMaster) ApplyJoin(op Op) Config {
	//fmt.Println("join")
	sm.newconf()
	if _, exist := sm.configs[sm.confignum].Groups[op.GID]; exist {
		return sm.configs[sm.confignum]
	}
	///fmt.Println(len(sm.configs[sm.confignum].Groups))
	//for i := 0; i < NShards; i++ {
	//		fmt.Printf("shard %v\n", sm.configs[sm.confignum].Shards[i])
	//	}
	for i := 0; ; i++ {
		min, _, _, maxx := sm.find_m()
		//fmt.Println(i, min, maxx)
		if i >= min {
			break
		}
		for i, v := range sm.configs[sm.confignum].Shards {
			if v == maxx {
				sm.configs[sm.confignum].Shards[i] = op.GID
				break
			}
		}
	}
	sm.configs[sm.confignum].Groups[op.GID] = op.Servers
	sm.WriteConf(sm.confignum)
	//fmt.Println("join done")
	//sm.reblance()

	return sm.configs[sm.confignum]
}

func (sm *ShardMaster) ApplyLeave(op Op) Config {
	sm.newconf()
	if _, exist := sm.configs[sm.confignum].Groups[op.GID]; !exist {
		return sm.configs[sm.confignum]
	}

	delete(sm.configs[sm.confignum].Groups, op.GID)
	for i := 0; ; i++ {
		_, minx, _, _ := sm.find_m()
		replace := false
		for i, v := range sm.configs[sm.confignum].Shards {
			if v == op.GID {
				sm.configs[sm.confignum].Shards[i] = minx
				replace = true
				break
			}
		}
		if !replace {
			break
		}
	}

	//sm.reblance()
	sm.WriteConf(sm.confignum)
	return sm.configs[sm.confignum]
}

func (sm *ShardMaster) ApplyMove(op Op) Config {
	sm.GetConf(sm.confignum)
	newconfig := sm.configs[sm.confignum]
	sm.confignum++
	sm.WriteString("confignum", sm.confignum)
	newconfig.Num = sm.confignum
	newconfig.Shards[op.Shard] = op.GID
	sm.configs = append(sm.configs, newconfig)
	sm.WriteConf(sm.confignum)
	return sm.configs[sm.confignum]
}

func (sm *ShardMaster) ApplyQuery(op Op) Config {
	num := op.Num
	if num == -1 || num > sm.confignum {
		sm.GetConf(sm.confignum)
		return sm.configs[sm.confignum]
	} else {
		sm.GetConf(num)
		return sm.configs[num]
	}
}

func (sm *ShardMaster) Apply(op Op) Config {
	var res Config
	switch op.Type {
	case SMJoin:
		res = sm.ApplyJoin(op)
	case SMLeave:
		res = sm.ApplyLeave(op)
	case SMMove:
		res = sm.ApplyMove(op)
	case SMQuery:
		res = sm.ApplyQuery(op)
	}
	sm.process = sm.process + 1
	sm.WriteString("process", sm.process)
	sm.px.Done(sm.process)
	return res
}

func (sm *ShardMaster) Wait(seq int) Op {
	for {
		decide, val := sm.px.Status(seq)
		if decide {
			return val.(Op)
		}
		time.Sleep(time.Millisecond * 10)
	}
}

func (sm *ShardMaster) AddOp(op Op) Config {
	op.Uid = strconv.FormatInt(rand.Int63(), 10)
	for {
		seq := sm.process + 1
		sm.WriteString("process", sm.process)
		decide, t := sm.px.Status(seq)
		var res Op
		if decide {
			res = t.(Op)
		} else {
			sm.px.Start(seq, op)
			res = sm.Wait(seq)
		}
		result := sm.Apply(res)
		if res.Uid == op.Uid {
			return result
		}
	}
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.AddOp(Op{Type: SMJoin, GID: args.GID, Servers: args.Servers})
	//fmt.Printf("Join %v\n", args.GID)
	//fmt.Printf("cfgnum %v tot %d shards\n", sm.confignum, len(sm.configs[sm.confignum].Groups))
	return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.AddOp(Op{Type: SMLeave, GID: args.GID})

	//fmt.Printf("leave %v\n", args.GID)
	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.AddOp(Op{Type: SMMove, Shard: args.Shard, GID: args.GID})
	//fmt.Printf("move %v to %v\n", args.Shard, args.GID)
	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()
	reply.Config = sm.AddOp(Op{Type: SMQuery, Num: args.Num})
	//fmt.Printf("query %d len %d\n", args.Num, len(reply.Config.Groups))
	return nil
}

// please don't change this function.
func (sm *ShardMaster) Kill() {
	sm.dead = true
	sm.l.Close()
	sm.px.Kill()
	if persist {
		sm.dbLock.Lock()
		sm.db.Close()
		sm.dbReadOptions.Close()
		sm.dbWriteOptions.Close()
		sm.dbLock.Unlock()
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *ShardMaster {
	gob.Register(Op{})

	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int64][]string{}

	sm.confignum = 0
	sm.process = 0

	rpcs := rpc.NewServer()
	rpcs.Register(sm)

	sm.px = paxos.Make(servers, me, rpcs)
	sm.dbinit()

	var l net.Listener
	var e error
	if Network {
		l, e = net.Listen("tcp", servers[me])
	} else {
		os.Remove(servers[me])
		l, e = net.Listen("unix", servers[me])
	}
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	sm.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for sm.dead == false {
			conn, err := sm.l.Accept()
			if err == nil && sm.dead == false {
				if sm.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if sm.unreliable && (rand.Int63()%1000) < 200 {
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
			if err != nil && sm.dead == false {
				fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
				sm.Kill()
			}
		}
	}()

	return sm
}
