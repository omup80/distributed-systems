package pbservice

import (
	"net"
)
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "math/rand"

type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	me         string
	vs         *viewservice.Clerk

	// Your declarations here.
	data           map[string]string
	view           viewservice.View
	requestServed  map[int64]bool
	requestResponse  map[int64]string
	isDataBackedUp bool
	rwmu           sync.RWMutex
}

func (pb *PBServer) Get(args GetArgs, reply *GetReply) error {

	// Your code here.

	//handled := pb.HandleSequence(args.ClientID, args.SeqNum)
	pb.rwmu.Lock()
	defer pb.rwmu.Unlock()
	if pb.me != pb.view.Primary{
		reply.Err = ErrWrongServer
		return nil
	}

	if pb.me != pb.view.Primary{
		reply.Err = ErrWrongServer
		return nil
	}
	if pb.view.Backup != "" {
		done := call(pb.view.Backup, "PBServer.BackupGet", args, reply)
		if done && reply.Err == OK {
			value := pb.data[args.Key]

			if reply.Value != value {
				pb.backUpData()

			}
			pb.requestResponse[args.RequestId] = value

			reply.Value = value
			reply.Err = OK

		}else if done && reply.Err==ErrWrongServer{
			reply.Err = ErrWrongServer
			if pb.vs.Primary() != pb.me {
				pb.isDataBackedUp = false
			}
			return nil
		//done and ok case
		}else{
			//if pb.vs.Primary() != pb.me{
				reply.Err = ErrWrongServer

				//pb.isDataBackedUp = false
			//}

			return nil
		}

	} else {
		value := pb.data[args.Key]
		reply.Value = value
		reply.Err = OK

	}
	//time.Sleep(10 * time.Millisecond)
	return nil
}

func (pb *PBServer) BackupGet(args GetArgs, reply *GetReply) error {
	//remove
	//pb.mu.Lock()
	//defer pb.mu.Unlock()

	if pb.me == pb.view.Primary {
		reply.Err = ErrWrongServer
	} else {
		reply.Err = OK
		reply.Value = pb.data[args.Key]

	}
	return nil
}
func (pb *PBServer) backUpData() {
	argsT := TransferDataArgs{}
	replyT := &TransferDataReply{}
	done := call(pb.view.Primary, "PBServer.TransferData", argsT, replyT)

	if done {
		pb.data = replyT.Data
		pb.isDataBackedUp = replyT.BackUpDone


	}
}
func (pb *PBServer) BackupPutAppend(args PutAppendArgs, reply *PutAppendReply) error {
	//pb.mu.Lock()
	//defer pb.mu.Unlock()

	if pb.requestServed[args.RequestId] == true {
		reply.Err = OK
		return nil
	}

	if pb.me == pb.view.Primary {
		reply.Err = ErrWrongServer
		pb.isDataBackedUp = false

		return nil
	}

	if !pb.isDataBackedUp {

	//	pb.backUpData()
		reply.Err = ErrWrongServer
		pb.isDataBackedUp = false

		return nil
	}



	pb.updateData(args.Op, args.Key, args.Value)
	reply.Err = OK
	pb.requestServed[args.RequestId] = true

	//fmt.Println("backup",pb.data)


	return nil
}

func (pb *PBServer) PutAppend(args PutAppendArgs, reply *PutAppendReply) error {

	// Your code here.



	pb.rwmu.Lock()
	defer pb.rwmu.Unlock()
	if pb.me != pb.view.Primary{
		reply.Err = ErrWrongServer
		return nil
	}
	backUpServer := pb.view.Backup

	if pb.requestServed[args.RequestId] == true {
		reply.Err = OK
		return nil
	}
	if pb.view.Backup != "" {
		ok := call(backUpServer, "PBServer.BackupPutAppend", args, reply)
		if ok && reply.Err == ErrWrongServer {
			reply.Err =  ErrWrongServer
			if pb.vs.Primary() != pb.me{
				pb.isDataBackedUp = false
			}

			return nil
		} else if !ok{
			reply.Err = ErrWrongServer
			//if pb.vs.Primary() != pb.me{
				//pb.isDataBackedUp = false
			//}
			return nil
		//ok and ok
		}else{

			pb.updateData(args.Op, args.Key, args.Value)
			pb.requestServed[args.RequestId] = true
			reply.Err = OK
		}
	} else {

		pb.updateData(args.Op, args.Key, args.Value)
		pb.requestServed[args.RequestId] = true
		reply.Err = OK

	}
	//fmt.Println("primary",pb.data)
	//remove
	//pb.mu.Unlock()
	//time.Sleep(10 * time.Millisecond)
	return nil
}
func (pb *PBServer) updateData(op string, key string, value string) {
	//pb.mu.Lock()
	if op == "Put" {
		pb.data[key] = value
	} else if op == "Append" {
		pb.data[key] = pb.data[key] + value
	}
	//pb.mu.Unlock()
	//fmt.Println(pb.data)
}

//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {
	view, error := pb.vs.Ping(pb.view.Viewnum)
	pb.rwmu.Lock()
	defer pb.rwmu.Unlock()
	if error != nil {
		//fmt.Println("Error skip this part")
	} else if pb.me != view.Primary && view.Backup != "" && !pb.isDataBackedUp && view.Backup == pb.me{

		args := TransferDataArgs{}
		reply := &TransferDataReply{}
		//TODO: Understand how it can impact
		done := call(view.Primary, "PBServer.TransferData", args, reply)
		for !done && pb.me != view.Primary && view.Backup != "" && !pb.isDataBackedUp && view.Backup == pb.me{
			view, error = pb.vs.Ping(pb.view.Viewnum)
			done = call(view.Primary, "PBServer.TransferData", args, reply)

		}
		//fmt.Println("Recieved data", reply.Data)
		pb.data = reply.Data
		pb.isDataBackedUp = reply.BackUpDone

	}

	pb.view = view

}

func (pb *PBServer) TransferData(args TransferDataArgs, reply *TransferDataReply) error {

	// Your code here.
	//pb.mu.Lock()
	if pb.me==pb.view.Primary{
		reply.Data = pb.data
		//fmt.Println("Transfer",pb.data)
		reply.BackUpDone = true
	}

	//pb.mu.Unlock()
	return nil
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
	fmt.Println(me)
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)

	// Your pb.* initializations here.
	if pb.data == nil {
		pb.data = make(map[string]string)
		pb.requestServed = make(map[int64]bool)
		pb.requestResponse = make(map[int64]string)
		pb.isDataBackedUp = false
	}
	pb.view = viewservice.View{Viewnum: 0, Primary: "", Backup: ""}

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
