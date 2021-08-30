package main

import (
	"context"
	"fmt"
	"kv/utils"
	"os"
	"strings"

	"kv/proto"

	"google.golang.org/grpc"
)

func locate(c proto.KVSClient, k string) string {
	r, err := c.Locate(context.Background(), &proto.LocateRequest{
		Key: k,
	})
	if err != nil {
		utils.Warn(err)
	}
	return r.Addr
}

func put(c proto.KVSClient, k string, v string) proto.Status {
	conn, err := grpc.Dial(locate(c, k), grpc.WithInsecure())
	if err != nil {
		utils.Warn(err)
	}
	defer conn.Close()
	o := proto.NewKVDClient(conn)
	r, err := o.Put(context.Background(), &proto.PutRequest{
		Key:   k,
		Value: v,
	})
	if err != nil {
		utils.Warn(err)
	}
	return r.Status
}

func read(c proto.KVSClient, k string) (proto.Status, string) {
	conn, err := grpc.Dial(locate(c, k), grpc.WithInsecure())
	if err != nil {
		utils.Warn(err)
	}
	defer conn.Close()
	o := proto.NewKVDClient(conn)
	r, err := o.Read(context.Background(), &proto.ReadRequest{
		Key: k,
	})
	if err != nil {
		utils.Warn(err)
	}
	return r.Status, r.Value
}

func del(c proto.KVSClient, k string) proto.Status {
	conn, err := grpc.Dial(locate(c, k), grpc.WithInsecure())
	if err != nil {
		utils.Warn(err)
	}
	defer conn.Close()
	o := proto.NewKVDClient(conn)
	r, err := o.Delete(context.Background(), &proto.DeleteRequest{
		Key: k,
	})
	if err != nil {
		utils.Warn(err)
	}
	return r.Status
}

func main() {
	utils.Info("Connecting to " + os.Getenv("serverip") + ":" + os.Getenv("serverport") + "...")
	conn, err := grpc.Dial(os.Getenv("serverip")+":"+os.Getenv("serverport"), grpc.WithInsecure())
	if err != nil {
		utils.Fatal(err)
	}
	defer conn.Close()
	utils.Info("Connected")

	c := proto.NewKVSClient(conn)

	var cmd []string
	var rets string
	var reti proto.Status
	for {
		rets = ""
		reti = 0
		fmt.Print("> ")
		cmd = strings.Split(utils.ScanLine(), " ")
		switch cmd[0] {
		case "locate":
			rets = locate(c, cmd[1])
		case "put":
			reti = put(c, cmd[1], cmd[2])
		case "read":
			reti, rets = read(c, cmd[1])
		case "delete":
			reti = del(c, cmd[1])
		case "help":
			println("- locate k")
			println("- put k v")
			println("- read k")
			println("- delete k")
			println("- help")
			println("- quit")
		case "quit":
			os.Exit(0)
		default:
			fmt.Println("Unknown cmd", cmd[0])
			continue
		}
		fmt.Print("ret:")
		if reti != 0 {
			fmt.Print(" ", reti)
		}
		if rets != "" {
			fmt.Print(" ", rets)
		}
		fmt.Print("\n")
	}
}
