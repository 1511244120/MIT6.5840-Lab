package kvsrv

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	server *labrpc.ClientEnd
	// You will have to modify this struct.
	// client id
	id int64
	// client sending sequence
	seq int
	// wait for lost message
	sendTimeout time.Duration
	// time gap between sending consecutive requests
	nextWait time.Duration
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server
	// You'll have to add code here.
	// initialization
	ck.id = nrand()
	ck.seq = 0
	ck.nextWait = 50 * time.Millisecond
	ck.sendTimeout = 500 * time.Millisecond
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	args := GetArgs{
		Key: key,
	}

	reply := GetReply{}

	for {
		select {
		case <-time.After(ck.sendTimeout):
			{
				// lost
				return ""
			}
		default:
			ok := ck.server.Call("KVServer.Get", &args, &reply)
			if ok {
				return reply.Value
			}
			time.Sleep(ck.nextWait)
		}
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) string {
	// You will have to modify this function.
	args := PutAppendArgs{
		Key:   key,
		Value: value,
		Seq:   ck.seq,
		Id:    ck.id,
	}

	reply := PutAppendReply{}

	for {
		select {
		case <-time.After(ck.sendTimeout):
			{
				return ""
			}
		default:
			ok := ck.server.Call("KVServer."+op, &args, &reply)
			if ok {
				// increase sequence if succussfully gets reply
				ck.seq++
				return reply.Value
			}
			time.Sleep(ck.nextWait)
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}
