package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

// maintain a session for each connected client
type Session struct {
	seq int
	// contain the value before append, in case concurrent appending to same key
	previous string
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	storage map[string]string
	session map[int64]Session
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Value = kv.storage[args.Key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	temp := kv.session[args.Id]

	//only reply if sequence is correct
	if args.Seq == temp.seq {
		//server sequence increment first
		temp.seq = args.Seq + 1
		delete(kv.storage, args.Key)

		kv.storage[args.Key] = args.Value
		kv.session[args.Id] = temp
	}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	temp := kv.session[args.Id]

	//only append if sequence is correct
	if args.Seq == temp.seq {
		temp.seq = args.Seq + 1
		temp.previous = kv.storage[args.Key]

		kv.storage[args.Key] = kv.storage[args.Key] + args.Value
		kv.session[args.Id] = temp
	} else {

	}
	reply.Value = temp.previous
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.

	kv.storage = make(map[string]string)
	kv.session = make(map[int64]Session)
	kv.mu = sync.Mutex{}
	return kv
}
