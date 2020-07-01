package utils

import (
	"fmt"
	"net"
	"os"

	"github.com/samuel/go-zookeeper/zk"
	log "github.com/sirupsen/logrus"
)

type ZooState struct {
	Conn     *zk.Conn
	ZooID    int
	DataList map[string]bool
	HashMap  *Map
}

func GetIPAddr() string {
	if os.Getenv("serverip") != "" {
		return os.Getenv("serverip")
	}

	addrs, err := net.InterfaceAddrs()
	if err != nil {
		log.Fatal(err)
	}
	// check localhost
	for _, address := range addrs {
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}
	return ""
}

func GetPort() string {
	if os.Getenv("serverport") == "" {
		return "1926"
	} else {
		return os.Getenv("serverport")
	}
}

func ScanLine() string {
	var c byte
	var err error
	var b []byte
	for err == nil {
		_, err = fmt.Scanf("%c", &c)

		if c != '\n' {
			b = append(b, c)
		} else {
			break
		}
	}
	return string(b)
}
