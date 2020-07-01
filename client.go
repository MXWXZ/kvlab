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

func main() {
	utils.Info("Connecting to RPC...")
	conn, err := grpc.Dial(os.Getenv("serverip")+":"+os.Getenv("serverport"), grpc.WithInsecure())
	if err != nil {
		utils.Fatal(err)
	}
	defer conn.Close()
	utils.Info("Connected")

	c := proto.NewKVSClient(conn)

	var cmd []string
	var rets string
	for {
		rets = ""
		fmt.Print("> ")
		cmd = strings.Split(utils.ScanLine(), " ")
		switch cmd[0] {
		case "locate":
			rets = locate(c, cmd[1])
		case "help":
			println("- locate k")
			println("- help")
			println("- quit")
		case "quit":
			os.Exit(0)
		default:
			fmt.Println("Unknown cmd", cmd[0])
			continue
		}
		if rets != "" {
			fmt.Println("ret:", rets)
		}
	}
}
