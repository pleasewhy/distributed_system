package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"net"
	"net/rpc"
	"os"
)
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type Reply struct {
	WorkerId   int32
	NRecude    int32
	IsFinished bool
	Test       bool
	TaskInfo   *Task
}

type Args struct {
	WorkerId int32
	IFiles   []string
	TaskInfo *Task
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

const (
	masterIP   = "127.0.0.1"
	masterPort = 1234
	clientAddr = "127.0.0.1"
)

func newRpcClient(port int) (*rpc.Client, error) {
	masterAddr := net.TCPAddr{
		IP:   []byte(masterIP),
		Port: masterPort,
		Zone: "",
	}
	clientAddr := net.TCPAddr{
		IP:   []byte(clientAddr),
		Port: port,
		Zone: "",
	}
	conn, err := net.DialTCP("tcp", &masterAddr, &clientAddr)
	if err != nil {
		return nil, err
	}
	return rpc.NewClient(conn), nil
}
