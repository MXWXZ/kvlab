package main

import (
	"context"
	"kv/proto"
	"os"
	"strconv"
	"time"

	"kv/utils"

	"github.com/samuel/go-zookeeper/zk"
)

var dZoo utils.ZooState

func DataRegister() {
	basePath := "/data/" + os.Getenv("name")

	_, _ = dZoo.Conn.Create("/data", []byte(""), 0, zk.WorldACL(zk.PermAll))
	_, _ = dZoo.Conn.Create(basePath, []byte(""), 0, zk.WorldACL(zk.PermAll))
	_, _ = dZoo.Conn.Create(basePath+"/primary", []byte("0"), 0, zk.WorldACL(zk.PermAll))
	lock := zk.NewLock(dZoo.Conn, "/data/lock", zk.WorldACL(zk.PermAll))
	if err := lock.Lock(); err != nil {
		utils.Fatal(err)
	}
	defer lock.Unlock()

	// check top exist
	e, _, err := dZoo.Conn.Exists(basePath + "/top")
	if err != nil {
		utils.Fatal(err)
	}
	if !e {
		_, err := dZoo.Conn.Create(basePath+"/top", []byte("0"), 0, zk.WorldACL(zk.PermAll))
		if err != nil {
			utils.Fatal(err)
		}
	}

	// get/set id
	r, s, err := dZoo.Conn.Get(basePath + "/top")
	if err != nil {
		utils.Fatal(err)
	}
	zooID, err := strconv.Atoi(string(r))
	if err != nil {
		utils.Fatal(err)
	}
	utils.Info("New data ", os.Getenv("name"), " id: ", zooID)
	_, err = dZoo.Conn.Create(basePath+"/"+strconv.Itoa(zooID), []byte(utils.GetIPAddr()+":"+utils.GetPort()), zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
	if err != nil {
		utils.Fatal(err)
	}
	_, err = dZoo.Conn.Set(basePath+"/top", []byte(strconv.Itoa(zooID+1)), s.Version)
	if err != nil {
		utils.Fatal(err)
	}
}

type data struct{}

func (s *data) Put(ctx context.Context, in *proto.PutRequest) (*proto.PutReply, error) {

	//return &proto.LocateReply{Addr: string(ret)}, nil
}

func (s *data) Read(ctx context.Context, in *proto.ReadRequest) (*proto.ReadReply, error) {

	//return &proto.LocateReply{Addr: string(ret)}, nil
}

func (s *data) Delete(ctx context.Context, in *proto.DeleteRequest) (*proto.DeleteReply, error) {

	//return &proto.LocateReply{Addr: string(ret)}, nil
}

func main() {
	utils.Info("Data node start.")
	utils.Info("Connecting to zookeeper...")
	var err error
	dZoo.Conn, _, err = zk.Connect([]string{os.Getenv("zooaddr")}, time.Second*5, zk.WithLogInfo(false))
	if err != nil {
		utils.Fatal(err)
	}
	defer dZoo.Conn.Close()

	utils.Info("Register to zookeeper...")
	DataRegister()

	for {
		time.Sleep(time.Millisecond * 100)
	}

	//listen, err := net.Listen("tcp", ":"+utils.GetPort())
	//if err != nil {
	//	utils.Fatal(err)
	//}
	//
	//s := grpc.NewServer()
	//proto.RegisterKVSServer(s, &server{})
	//utils.Info("RPC server running on " + utils.GetPort())
	//err = s.Serve(listen)
	//if err != nil {
	//	utils.Fatal(err)
	//}
}
