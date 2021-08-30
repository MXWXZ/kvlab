package main

import (
	"context"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"kv/proto"
	"kv/utils"

	"github.com/samuel/go-zookeeper/zk"
	"google.golang.org/grpc"
)

var sZoo utils.ZooState

func ServerWatchMaster(ech <-chan zk.Event) {
	event := <-ech
	_, _, c, err := sZoo.Conn.ExistsW("/server/0")
	if err != nil {
		utils.Warn(err)
		return
	}
	go ServerWatchMaster(c)
	if event.Type == zk.EventNodeDeleted {
		utils.Warn("Master node fail")
		lock := zk.NewLock(sZoo.Conn, "/server/lock", zk.WorldACL(zk.PermAll))
		if err := lock.Lock(); err != nil {
			utils.Warn(err)
			return
		}
		defer lock.Unlock()

		e, _, err := sZoo.Conn.Exists("/server/0")
		if err != nil {
			utils.Warn(err)
			return
		}
		if !e { // success
			ServerSwitchMaster(sZoo.ZooID)
		}
	}
}

func ServerChangeData(ech <-chan zk.Event) {
	event := <-ech
	_, st, c, err := sZoo.Conn.ChildrenW(event.Path)
	if err != nil {
		utils.Warn(err)
		return
	}
	go ServerChangeData(c)

	if st.NumChildren >= 2 {
		utils.Info("Data node change ", event.Path)
		// prevent new data node
		dLock := zk.NewLock(sZoo.Conn, "/data/lock", zk.WorldACL(zk.PermAll))
		if err := dLock.Lock(); err != nil {
			utils.Warn(err)
			return
		}
		defer dLock.Unlock()

		s, _, err := sZoo.Conn.Children(event.Path)
		if err != nil {
			utils.Warn(err)
			return
		}

		p, _, err := sZoo.Conn.Get(event.Path + "/primary")
		if err != nil {
			utils.Warn(err)
			return
		}
		pathSplit := strings.Split(event.Path, "/")
		sZoo.HashMap.Add(pathSplit[len(pathSplit)-1])
		// check primary
		if len(s) > 2 {
			sel := ""
			flag := false
			for _, i := range s {
				if i == string(p) {
					flag = true
					break
				} else if i != "top" && i != "primary" && sel == "" {
					sel = i
				}
			}
			if !flag {
				utils.Info("Reselected primary ", event.Path)
				_, err := sZoo.Conn.Set(event.Path+"/primary", []byte(sel), -1)
				if err != nil {
					utils.Warn(err)
					return
				}
			}
		} else if string(p) != "" {
			utils.Warn("No primary exist ", event.Path)
			_, err := sZoo.Conn.Set(event.Path+"/primary", []byte(""), -1)
			if err != nil {
				utils.Warn(err)
				return
			}
		}
	}
}

func ServerNewData(ech <-chan zk.Event) {
	event := <-ech
	_, _, c, err := sZoo.Conn.ChildrenW(event.Path)
	if err != nil {
		utils.Warn(err)
		return
	}
	go ServerNewData(c)
	s, _, err := sZoo.Conn.Children(event.Path)
	if err != nil {
		utils.Warn(err)
		return
	}
	for _, i := range s {
		if i != "lock" {
			if _, ok := sZoo.DataList[i]; !ok {
				utils.Info("New data ", i)
				_, _, c, err := sZoo.Conn.ChildrenW("/data/" + i)
				if err != nil {
					utils.Fatal(err)
				}
				sZoo.DataList[i] = true
				go ServerChangeData(c)
			}
		}
	}
}

func ServerSwitchMaster(id int) bool {
	_, err := sZoo.Conn.Create("/server/0", []byte(utils.GetIPAddr()+":"+utils.GetPort()), zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
	if err != nil {
		utils.Warn(err)
		return false
	}
	err = sZoo.Conn.Delete("/server/"+strconv.Itoa(id), -1)
	if err != nil {
		utils.Warn(err)
	}
	if !ServerAddMasterHook() {
		utils.Warn(err)
	}
	sZoo.ZooID = 0
	utils.Info("Switch to master, current id: 0")
	return true
}

func ServerAddMasterHook() bool {
	list, _, c, err := sZoo.Conn.ChildrenW("/data")
	if err != nil {
		utils.Warn(err)
		return false
	}
	go ServerNewData(c)
	for _, i := range list {
		if i != "lock" {
			_, _, c, err := sZoo.Conn.ChildrenW("/data/" + i)
			if err != nil {
				utils.Warn(err)
				return false
			}
			sZoo.DataList[i] = true
			go ServerChangeData(c)
		}
	}
	return true
}

func ServerRegister() {
	_, _ = sZoo.Conn.Create("/server", []byte(""), 0, zk.WorldACL(zk.PermAll))
	sLock := zk.NewLock(sZoo.Conn, "/server/lock", zk.WorldACL(zk.PermAll))
	if err := sLock.Lock(); err != nil {
		utils.Fatal(err)
	}
	defer sLock.Unlock()
	dLock := zk.NewLock(sZoo.Conn, "/data/lock", zk.WorldACL(zk.PermAll))
	if err := dLock.Lock(); err != nil {
		utils.Fatal(err)
	}
	defer dLock.Unlock()

	// check top exist
	e, _, err := sZoo.Conn.Exists("/server/top")
	if err != nil {
		utils.Fatal(err)
	}
	if !e {
		_, err := sZoo.Conn.Create("/server/top", []byte("0"), 0, zk.WorldACL(zk.PermAll))
		if err != nil {
			utils.Fatal(err)
		}
	}

	// get/set id
	r, s, err := sZoo.Conn.Get("/server/top")
	if err != nil {
		utils.Fatal(err)
	}
	sZoo.ZooID, err = strconv.Atoi(string(r))
	if err != nil {
		utils.Fatal(err)
	}
	utils.Info("New server id: ", sZoo.ZooID)
	_, err = sZoo.Conn.Create("/server/"+strconv.Itoa(sZoo.ZooID), []byte(utils.GetIPAddr()+":"+utils.GetPort()), zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
	if err != nil {
		utils.Fatal(err)
	}
	_, err = sZoo.Conn.Set("/server/top", []byte(strconv.Itoa(sZoo.ZooID+1)), s.Version)
	if err != nil {
		utils.Fatal(err)
	}

	// watch
	if sZoo.ZooID != 0 { // not master
		utils.Info("ServerRegister as slave")
		e, _, err = sZoo.Conn.Exists("/server/0")
		if err != nil {
			utils.Fatal(err)
		}
		if !e { // no master
			ServerSwitchMaster(sZoo.ZooID)
		} else {
			_, _, c, err := sZoo.Conn.ExistsW("/server/0")
			if err != nil {
				utils.Fatal(err)
			}
			go ServerWatchMaster(c)
		}
	} else {
		utils.Info("ServerRegister as master")
	}

	if sZoo.ZooID == 0 { // master
		if !ServerAddMasterHook() {
			os.Exit(2)
		}
	}
}

type server struct{}

func (s *server) Locate(ctx context.Context, in *proto.LocateRequest) (*proto.LocateReply, error) {
	hash := sZoo.HashMap.Get(in.Key)
	r, _, err := sZoo.Conn.Get("/data/" + hash + "/primary")
	if err != nil {
		return nil, err
	}
	ret, _, err := sZoo.Conn.Get("/data/" + hash + "/" + string(r))
	if err != nil {
		return nil, err
	}
	return &proto.LocateReply{Addr: string(ret)}, nil
}

func main() {
	sZoo.HashMap = utils.HashNew(3, nil)
	sZoo.DataList = make(map[string]bool)

	utils.Info("Server start.")
	utils.Info("Connecting to zookeeper...")
	var err error
	sZoo.Conn, _, err = zk.Connect([]string{os.Getenv("zooaddr")}, time.Second*5, zk.WithLogInfo(false))
	if err != nil {
		utils.Fatal(err)
	}
	defer sZoo.Conn.Close()

	utils.Info("ServerRegister to zookeeper...")
	ServerRegister()

	listen, err := net.Listen("tcp", utils.GetIPAddr()+":"+utils.GetPort())
	if err != nil {
		utils.Fatal(err)
	}

	s := grpc.NewServer()
	proto.RegisterKVSServer(s, &server{})
	utils.Info("RPC server running on " + utils.GetIPAddr() + ":" + utils.GetPort())
	err = s.Serve(listen)
	if err != nil {
		utils.Fatal(err)
	}
}
