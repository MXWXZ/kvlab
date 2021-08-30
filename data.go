package main

import (
	"context"
	"kv/proto"
	"net"
	"os"
	"strconv"
	"time"

	"google.golang.org/grpc"

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

	p, _, err := dZoo.Conn.Get(basePath + "/primary")
	if err != nil {
		utils.Fatal(err)
	}
	if string(p) != "" {
		addr, _, err := dZoo.Conn.Get(basePath + "/" + string(p))
		if err != nil {
			utils.Fatal(err)
		}
		if string(addr) != utils.GetIPAddr()+":"+utils.GetPort() {
			conn, err := grpc.Dial(string(addr), grpc.WithInsecure())
			if err != nil {
				utils.Fatal(err)
			}
			defer conn.Close()
			c := proto.NewKVDClient(conn)
			db, err := c.Sync(context.Background(), &proto.SyncRequest{})
			if err != nil {
				utils.Fatal(err)
			}
			utils.DB = db.Kv
			utils.WriteDB()
			utils.Info("DB sync")
		}
	}
}

func CallOther(f func(c proto.KVDClient)) {
	basePath := "/data/" + os.Getenv("name")
	local := utils.GetIPAddr() + ":" + utils.GetPort()
	child, _, err := dZoo.Conn.Children(basePath)
	if err != nil {
		utils.Warn(err)
	}
	for _, i := range child {
		if i != "primary" && i != "top" {
			v, _, err := dZoo.Conn.Get(basePath + "/" + i)
			if err != nil {
				utils.Warn(err)
			}
			if string(v) != local { // other node
				conn, err := grpc.Dial(string(v), grpc.WithInsecure())
				if err != nil {
					utils.Warn(err)
				}
				defer conn.Close()
				o := proto.NewKVDClient(conn)
				f(o)
			}
		}
	}
}

type data struct{}

func (s *data) Put(ctx context.Context, in *proto.PutRequest) (*proto.PutReply, error) {
	dZoo.Mutex.Lock()
	defer dZoo.Mutex.Unlock()
	utils.FSWrite(in.Key, in.Value)
	if utils.DB == nil {
		utils.DB = make(map[string]string)
	}
	utils.DB[in.Key] = in.Value
	if !in.Sync {
		CallOther(func(c proto.KVDClient) {
			r, err := c.Put(context.Background(), &proto.PutRequest{
				Key:   in.Key,
				Value: in.Value,
				Sync:  true,
			})
			if err != nil {
				utils.Warn(err)
			}
			if r.Status != proto.Status_OK {
				utils.Warn("Sync node error")
			}
		})
	}
	return &proto.PutReply{Status: proto.Status_OK}, nil
}

func (s *data) Read(ctx context.Context, in *proto.ReadRequest) (*proto.ReadReply, error) {
	var ret proto.Status
	v, ok := utils.DB[in.Key]
	if !ok {
		v = ""
		ret = proto.Status_NOTFOUND
	} else {
		ret = proto.Status_OK
	}
	return &proto.ReadReply{Status: ret, Value: v}, nil
}

func (s *data) Delete(ctx context.Context, in *proto.DeleteRequest) (*proto.DeleteReply, error) {
	dZoo.Mutex.Lock()
	defer dZoo.Mutex.Unlock()
	var ret proto.Status
	_, ok := utils.DB[in.Key]
	if !ok {
		ret = proto.Status_NOTFOUND
	} else {
		delete(utils.DB, in.Key)
		ret = proto.Status_OK
	}
	utils.WriteDB()
	if !in.Sync {
		CallOther(func(c proto.KVDClient) {
			r, err := c.Delete(context.Background(), &proto.DeleteRequest{
				Key:  in.Key,
				Sync: true,
			})
			if err != nil {
				utils.Warn(err)
			}
			if r.Status != proto.Status_OK {
				utils.Warn("Sync node error")
			}
		})
	}
	return &proto.DeleteReply{Status: ret}, nil
}

func (s *data) Sync(ctx context.Context, in *proto.SyncRequest) (*proto.SyncReply, error) {
	return &proto.SyncReply{Status: proto.Status_OK, Kv: utils.DB}, nil
}

func main() {
	dZoo.HashMap = utils.HashNew(3, nil)
	dZoo.DataList = make(map[string]bool)
	utils.FSInit()
	defer utils.FSFini()
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

	listen, err := net.Listen("tcp", utils.GetIPAddr()+":"+utils.GetPort())
	if err != nil {
		utils.Fatal(err)
	}

	s := grpc.NewServer()
	proto.RegisterKVDServer(s, &data{})
	utils.Info("RPC server running on " + utils.GetIPAddr() + ":" + utils.GetPort())
	err = s.Serve(listen)
	if err != nil {
		utils.Fatal(err)
	}
}
