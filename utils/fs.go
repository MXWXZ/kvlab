package utils

import (
	"bufio"
	"encoding/base64"
	"io"
	"os"
)

var DB = make(map[string]string)
var fp *os.File

func FSInit() {
	Info("Loading DB file...")
	if !PathExists(GetDB()) && PathExists(GetRB()) {
		os.Rename(GetRB(), GetDB())
	}

	file, err := os.OpenFile(GetDB(), os.O_RDONLY|os.O_CREATE, 0644)
	if err != nil {
		Fatal(err)
	}
	reader := bufio.NewReader(file)
	state := 0
	var bufk, bufv string
	for {
		line, err := reader.ReadString('\n')
		if err == io.EOF {
			break
		}
		if err != nil {
			Fatal(err)
			return
		}
		line = line[0 : len(line)-1]
		switch state {
		case 0: //key
			tmp, err := base64.StdEncoding.DecodeString(line)
			if err != nil {
				Fatal(err)
			}
			bufk = string(tmp)
			state++
		case 1: //value
			tmp, err := base64.StdEncoding.DecodeString(line)
			if err != nil {
				Fatal(err)
			}
			bufv = string(tmp)
			state++
		case 2: //commit
			if line != "#" {
				Warn("DB file corrupted, recovering...")
				goto end
			}
			DB[bufk] = bufv
			state = 0
		}
	}
	if state != 0 {
		Warn("DB file corrupted, recovering...")
	}
end:
	file.Close()
	WriteDB()
	Info("Success")
	fp, err = os.OpenFile(GetDB(), os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		Fatal(err)
	}
}

func FSFini() {
	fp.Close()
}

func PathExists(path string) bool {
	_, err := os.Stat(path)
	if err == nil {
		return true
	}
	if os.IsNotExist(err) {
		return false
	}
	return false
}

func WriteDB() {
	f, err := os.OpenFile(GetRB(), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		Fatal(err)
	}
	w := bufio.NewWriter(f)
	for k, v := range DB {
		if _, err = w.WriteString(base64.StdEncoding.EncodeToString([]byte(k)) + "\n"); err != nil {
			Fatal(err)
		}
		if _, err = w.WriteString(base64.StdEncoding.EncodeToString([]byte(v)) + "\n"); err != nil {
			Fatal(err)
		}
		if _, err = w.WriteString("#\n"); err != nil {
			Fatal(err)
		}
	}
	err = w.Flush()
	if err != nil {
		Fatal(err)
	}
	err = f.Close()
	if err != nil {
		Fatal(err)
	}
	os.Remove(GetDB())
	os.Rename(GetRB(), GetDB())
}

func FSWrite(k string, v string) {
	p, err := os.OpenFile(GetDB(), os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		Fatal(err)
	}
	defer p.Close()
	_, err = p.WriteString(base64.StdEncoding.EncodeToString([]byte(k)) + "\n")
	if err != nil {
		Fatal(err)
	}
	_, err = p.WriteString(base64.StdEncoding.EncodeToString([]byte(v)) + "\n")
	if err != nil {
		Fatal(err)
	}
	_, err = p.WriteString("#\n")
	if err != nil {
		Fatal(err)
	}
	err = p.Sync()
	if err != nil {
		Fatal(err)
	}
}
