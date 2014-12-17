package shardkv

import (
	"bytes"
	"github.com/jmhodges/levigo"
	"strings"

	"net"
)
import "fmt"
import "net/rpc"
import "log"
import "time"
import "paxos"
import (
	"strconv"
	"sync"
)
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "shardmaster"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		fmt.Printf(format, a...)
	}
	return
}

const (
	OpPut      = "Put"
	OpGet      = "Get"
	OpReconfig = "reconfig"
	OpGetshard = "getshard"
	persist    = true
	NetWork    = false
)

const (
	ErrNotReady = "notready"
	CHECKOK     = "checkok"
	FAIL        = "fail"
)

type Op struct {
	// Your definitions here.
	Type     string
	Key      string
	Me       string
	Value    string
	Dohash   bool
	Uid      string
	Config   shardmaster.Config
	Reconfig GetShardReply
	UUid     string
}

type ShardKV struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       bool // for testing
	unreliable bool // for testing
	sm         *shardmaster.Clerk
	px         *paxos.Paxos

	gid int64 // my replica group ID

	// Your definitions here.
	kv      map[string]string
	seen    map[string]int
	reply   map[string]string
	process int
	config  shardmaster.Config

	db             *levigo.DB
	dbLock         sync.Mutex
	dbReadOptions  *levigo.ReadOptions
	dbWriteOptions *levigo.WriteOptions
}

func (kv *ShardKV) WriteConfig() {
	if !persist {
		return
	}
	kv.dbLock.Lock()
	defer kv.dbLock.Unlock()
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	err := enc.Encode(kv.config)
	//fmt.Println("write_conf_", confignum, sm.configs[confignum])
	if err != nil {
		log.Println(err)
	} else {
		key := "config"
		err := kv.db.Put(kv.dbWriteOptions, []byte(key), buffer.Bytes())
		if err != nil {
			log.Println("\terror writing to database", err)
		}
	}
}

func (kv *ShardKV) GetConfig() {
	if persist {
		//fmt.Println("get_config_", confignum)
		kv.dbLock.Lock()
		defer kv.dbLock.Unlock()

		key := "config"
		keyBytes, err := kv.db.Get(kv.dbReadOptions, []byte(key))
		if err == nil && len(keyBytes) > 0 {
			buffer := *bytes.NewBuffer(keyBytes)
			decoder := gob.NewDecoder(&buffer)
			var val shardmaster.Config
			err = decoder.Decode(&val)
			if err != nil {
				log.Println("decode err", err)
			} else {
				kv.config = val
			}
		} else {
			log.Println("get config err ", err)
		}
	}
	return
}

func (kv *ShardKV) WriteKV(key string, val string) {
	if !persist {
		return
	}
	kv.dbLock.Lock()
	defer kv.dbLock.Unlock()

	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	err := enc.Encode(val)

	if err != nil {
		log.Println(err)
	} else {
		err := kv.db.Put(kv.dbWriteOptions, []byte(key), buffer.Bytes())
		if err != nil {
			log.Println("\terror writing to database", err)
		}
	}
}

func (kv *ShardKV) GetKV(key string) (string, Err) {
	if !persist {
		return "", FAIL
	}
	kv.dbLock.Lock()
	defer kv.dbLock.Unlock()

	//fmt.Println("getkey ", key)

	keyBytes, err := kv.db.Get(kv.dbReadOptions, []byte(key))
	if err == nil && len(keyBytes) > 0 {
		buffer := *bytes.NewBuffer(keyBytes)
		decoder := gob.NewDecoder(&buffer)
		var val string
		err = decoder.Decode(&val)
		if err != nil {
			log.Println("decode err", err)
		} else {
			return val, OK
		}
	} else {
		log.Println("get key err ", err)
	}
	return "", FAIL
}

func (kv *ShardKV) GetKey(key string) (string, bool) {
	if persist {
		val, err := kv.GetKV("key_" + key)
		if err == OK {
			kv.kv[key] = val
		}
	}
	val, exist := kv.kv[key]
	return val, exist
}

func (kv *ShardKV) GetSeen(key string) {
	if persist {
		val, err := kv.GetKV("seen_" + key)
		if err == OK {
			kv.seen[key], _ = strconv.Atoi(val)
			kv.reply[key], _ = kv.GetKV("reply_" + key)
		}
	}
}

func (kv *ShardKV) WriteSeen(client string) {
	kv.WriteKV("seen_"+client, strconv.Itoa(kv.seen[client]))
	kv.WriteKV("reply_"+client, kv.reply[client])
}

func (kv *ShardKV) IterKey(shard int) {
	if !persist {
		return
	}
	iterator := kv.db.NewIterator(kv.dbReadOptions)
	iterator.Seek([]byte("key_"))
	for iterator.Valid() {
		keyBytes := iterator.Key()
		key := string(keyBytes)
		if strings.Index(key, "key_") < 0 {
			iterator.Next()
			continue
		}
		key = key[len("key_"):]
		if key2shard(key) != shard {
			iterator.Next()
			continue
		}

		valueBytes := iterator.Value()
		bufferVal := *bytes.NewBuffer(valueBytes)
		decoderVal := gob.NewDecoder(&bufferVal)
		var val string
		err := decoderVal.Decode(&val)
		if err != nil {
			iterator.Next()
			continue
		}
		kv.kv[key] = val
		iterator.Next()
	}
	iterator.Close()
}

func (kv *ShardKV) IterSeen() {
	if !persist {
		return
	}
	iterator := kv.db.NewIterator(kv.dbReadOptions)
	iterator.Seek([]byte("seen_"))
	for iterator.Valid() {
		keyBytes := iterator.Key()
		key := string(keyBytes)
		if strings.Index(key, "seen_") < 0 {
			iterator.Next()
			continue
		}
		key = key[len("seen_"):]

		valueBytes := iterator.Value()
		bufferVal := *bytes.NewBuffer(valueBytes)
		decoderVal := gob.NewDecoder(&bufferVal)
		var val string
		err := decoderVal.Decode(&val)
		if err != nil {
			iterator.Next()
			continue
		}
		kv.seen[key], _ = strconv.Atoi(val)
		kv.reply[key], _ = kv.GetKV("reply_" + key)
		iterator.Next()
	}
	iterator.Close()
}

func (kv *ShardKV) dbinit() {
	if !persist {
		return
	}
	kv.dbLock.Lock()
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.dead {
		return
	}
	gob.Register(shardmaster.Config{})

	kv.dbReadOptions = levigo.NewReadOptions()
	kv.dbWriteOptions = levigo.NewWriteOptions()
	options := levigo.NewOptions()
	options.SetCache(levigo.NewLRUCache(3 << 30))
	options.SetCreateIfMissing(true)
	//dbname := "./shardkvdb" + strconv.Itoa(kv.me)
	dbname := "./shardkvdb" + strconv.Itoa(rand.Int())
	os.RemoveAll(dbname) // for test,
	var err error
	kv.db, err = levigo.Open(dbname, options)
	if err != nil {
		log.Println("open db", err)
	}
	kv.dbLock.Unlock()
	val, er := kv.GetKV("process")
	if er == OK {
		kv.process, _ = strconv.Atoi(val)
	}
	kv.GetConfig()
	//log.Println("db init success")
}

func (kv *ShardKV) GetShard(args *GetShardArgs, reply *GetShardReply) error {
	if kv.config.Num < args.Config.Num {
		reply.Err = ErrNotReady
		return nil
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	shard := args.Shard
	kv.AddOp(Op{Type: OpGetshard})
	//	DPrintf("getshard")

	reply.Kv = map[string]string{}
	reply.Seen = map[string]int{}
	reply.Reply = map[string]string{}

	kv.IterKey(shard)
	for key := range kv.kv {
		if key2shard(key) == shard {
			reply.Kv[key] = kv.kv[key]
		}
	}

	kv.IterSeen()
	for client := range kv.seen {
		reply.Seen[client] = kv.seen[client]
		reply.Reply[client] = kv.reply[client]
	}
	reply.Err = OK
	return nil
}

func (reply *GetShardReply) Merge(other GetShardReply) {
	for key := range other.Kv {
		reply.Kv[key] = other.Kv[key]
	}
	for client := range other.Seen {
		seq, exist := reply.Seen[client]
		if !exist || other.Seen[client] > seq {
			reply.Seen[client] = other.Seen[client]
			reply.Reply[client] = other.Reply[client]
		}
	}
}

func (kv *ShardKV) Reconfigure(newcfg shardmaster.Config) bool {
	//	DPrintf("reconfig\n", newcfg.Num)
	reconfig := GetShardReply{OK, map[string]string{}, map[string]int{}, map[string]string{}}

	oldcfg := &kv.config
	for i := 0; i < shardmaster.NShards; i++ {
		gid := oldcfg.Shards[i]
		if newcfg.Shards[i] == kv.gid && gid != kv.gid {
			args := &GetShardArgs{i, *oldcfg}
			var reply GetShardReply
			for {
				flag := true
				for _, srv := range oldcfg.Groups[gid] {
					flag = false
					ok := call(srv, "ShardKV.GetShard", args, &reply)
					if ok && reply.Err == OK {
						flag = true
						break
					}
					if ok && reply.Err == ErrNotReady {
						return false
					}
				}
				if flag {
					break
				}
			}
			reconfig.Merge(reply)
		}
	}
	op := Op{Type: OpReconfig, Config: newcfg, Reconfig: reconfig}
	kv.AddOp(op)
	//	DPrintf("new config %v\n", kv.config.Num)
	return true
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	newcfg := kv.sm.Query(-1)
	for i := kv.config.Num + 1; i <= newcfg.Num; i++ {
		cfg := kv.sm.Query(i)
		if !kv.Reconfigure(cfg) {
			return
		}
	}
}

// tell the server to shut itself down.
func (kv *ShardKV) kill() {
	if kv.dead {
		return
	}
	kv.dead = true
	kv.l.Close()
	kv.px.Kill()
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
	kv.kv = map[string]string{}
	kv.seen = map[string]int{}
	kv.reply = map[string]string{}
	kv.config = shardmaster.Config{Num: -1}
	kv.process = 0

	kv.dbinit()

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)
	var l net.Listener
	var e error
	if NetWork {
		l, e = net.Listen("tcp", servers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
	} else {
		l, e = net.Listen("unix", servers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
	}
	kv.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.dead == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.dead == false {
				if kv.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.unreliable && (rand.Int63()%1000) < 200 {
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
			if err != nil && kv.dead == false {
				fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	go func() {
		for kv.dead == false {
			kv.tick()
			time.Sleep(250 * time.Millisecond)
		}
	}()

	return kv
}

func (kv *ShardKV) ApplyPutOrGet(op Op) (result string, err Err) {
	//	DPrintf("putorget %v\n", op.Key)
	err = OK
	result, ok := kv.GetKey(op.Key)
	if !ok {
		result = ""
		err = OK
	}
	//fmt.Printf("op.me %v res %v type %v key %v val %v uid %v\n", op.Me, result, op.Type, op.Key, op.Value, op.Uid)
	kv.seen[op.Me], _ = strconv.Atoi(op.Uid)
	kv.reply[op.Me] = result
	kv.WriteSeen(op.Me)
	if op.Type == OpPut {
		if op.Dohash {
			op.Value = strconv.Itoa(int(hash(result + op.Value)))
		}
		kv.kv[op.Key] = op.Value
		kv.WriteKV("key_"+op.Key, op.Value)
	}
	return result, err
}

func (kv *ShardKV) ApplyReconfig(op Op) (result string, err Err) {
	info := &op.Reconfig
	for key := range info.Kv {
		kv.kv[key] = info.Kv[key]
	}
	for client := range info.Seen {
		seq, exist := kv.seen[client]
		if !exist || info.Seen[client] > seq {
			kv.seen[client] = info.Seen[client]
			kv.reply[client] = info.Reply[client]
			kv.WriteSeen(client)
		}
	}
	kv.config = op.Config
	kv.WriteConfig()
	return "", OK
}

func (kv *ShardKV) Apply(op Op) (result string, err Err) {
	err = OK
	switch op.Type {
	case OpPut, OpGet:
		result, err = kv.ApplyPutOrGet(op)
	case OpReconfig:
		result, err = kv.ApplyReconfig(op)
	}
	kv.process++
	kv.WriteKV("process", strconv.Itoa(kv.process))
	kv.px.Done(kv.process)
	return result, err
}

func (kv *ShardKV) WaitDecide(seq int) Op {
	for {
		decide, val := kv.px.Status(seq)
		if decide {
			return val.(Op)
		}
		time.Sleep(time.Millisecond * 10)
	}
}

func (kv *ShardKV) Check(op Op) (result string, err Err) {
	switch op.Type {
	case OpPut, OpGet:
		shard := key2shard(op.Key)
		if kv.gid != kv.config.Shards[shard] {
			return "", ErrWrongGroup
		}
		kv.GetSeen(op.Me)
		seq, exist := kv.seen[op.Me]
		opseq, _ := strconv.Atoi(op.Uid)
		//fmt.Printf("check op.me %v res %v type %v key %v val %v uid %v\n", op.Me, result, op.Type, op.Key, op.Value, op.Uid)
		//fmt.Printf("op %v check seq %v opseq %v\n", op.Type, seq, opseq)
		if exist && seq >= opseq {
			return kv.reply[op.Me], OK
		}
	case OpReconfig:
		if op.Config.Num <= kv.config.Num {
			return "", CHECKOK
		}
	}
	return "", CHECKOK
}

func (kv *ShardKV) AddOp(op Op) (result string, err Err) {
	ok := false
	op.UUid = strconv.FormatInt(rand.Int63(), 10)
	for !ok {
		result, err = kv.Check(op)
		if err == ErrWrongGroup {
			return "", err
		}
		if err != CHECKOK {
			return result, err
		}
		seq := kv.process + 1
		decide, t := kv.px.Status(seq)
		var res Op
		if decide {
			res = t.(Op)
		} else {
			kv.px.Start(seq, op)
			res = kv.WaitDecide(seq)
		}
		ok = res.UUid == op.UUid
		result, err = kv.Apply(res)
	}
	return result, err
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	//  log.Printf("get %s\n", args.Key)
	reply.Value, reply.Err = kv.AddOp(Op{Type: OpGet, Key: args.Key, Uid: args.Uid, Me: args.Me})
	//fmt.Printf("get %v seq %v\n", args.Key, args.Uid)
	//fmt.Printf("get reply %d %d %v\n", args.Key, args.Uid, reply.Value)
	return nil
}

func (kv *ShardKV) Put(args *PutArgs, reply *PutReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	//fmt.Printf("put %s %s %v seq %v\n", args.Key, args.Value, args.DoHash, args.Uid)
	reply.PreviousValue, reply.Err = kv.AddOp(Op{Type: OpPut, Key: args.Key, Value: args.Value, Dohash: args.DoHash, Uid: args.Uid, Me: args.Me})
	//fmt.Printf("put key %s value %s dohash %v uid %v\n", args.Key, args.Value, args.DoHash, args.Uid)
	//fmt.Printf("putResult %s %s\n", reply.PreviousValue, reply.Err)
	return nil
}
