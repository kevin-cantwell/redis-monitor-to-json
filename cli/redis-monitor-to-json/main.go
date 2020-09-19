package main

import (
	"bufio"
	"encoding/json"
	"log"
	"os"
	"strconv"
)

type MonitorLog struct {
	Timestamp int64    `json:"ts_nano"`
	DB        int      `json:"db"`
	Source    string   `json:"source"` // IP:PORT | lua
	Command   string   `json:"cmd"`
	Args      []string `json:"args"`
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	r := &reader{buf: bufio.NewReader(os.Stdin)}

	// handle initial OK reply line, if exists
	ok := string([]rune{r.read(), r.read(), r.read()})
	if ok != "OK\n" {
		r.unread()
		r.unread()
		r.unread()
	}

	enc := json.NewEncoder(os.Stdout)
	for {
		mon := readMonitorLog(r)
		if err := enc.Encode(mon); err != nil {
			log.Fatalln(err)
		}
	}
}

// parses lines like:
// <secs>.<micros> [<db> <source>] "<command>" "<arg0>" "<arg1>" ... "<argN>"
func readMonitorLog(r *reader) MonitorLog {
	var mon MonitorLog
	mon.Timestamp = readTimestamp(r)
	r.read() // ws
	r.read() // [
	mon.DB = readDB(r)
	r.read() // ws
	mon.Source = readSource(r)
	r.read() // ]
	r.read() // ws
	mon.Command = readCommand(r)
	r.read() // ws
	mon.Args = readArgs(r)
	r.read() // \n
	return mon
}

func readTimestamp(r *reader) int64 {
	var (
		micros string
	)
	for i := 0; i < 17; i++ {
		b := r.read()
		if b == '.' {
			continue
		}
		micros += string(b)
	}
	ts, err := strconv.ParseInt(micros+"000", 10, 64)
	if err != nil {
		log.Fatalln(err)
	}
	return ts
}

func readDB(r *reader) int {
	var db string
	for {
		b := r.read()
		if b == ' ' {
			r.unread()
			n, err := strconv.Atoi(db)
			if err != nil {
				log.Fatalln(err)
			}
			return n
		}
		db += string(b)
	}
}

func readSource(r *reader) string {
	var source string
	for {
		b := r.read()
		if b == ']' {
			r.unread()
			return source
		}
		source += string(b)
	}
}

func readCommand(r *reader) string {
	return readQuoted(r)
}

func readArgs(r *reader) []string {
	var args []string
	for {
		args = append(args, readQuoted(r))
		ws := r.read()
		if ws == '\n' {
			r.unread()
			return args
		}
	}
}

func readQuoted(r *reader) string {
	b := r.read()
	prev := b
	quoted := string(b)
	for {
		b = r.read()
		if b == '"' && prev != '\\' {
			quoted += string(b)
			u, err := strconv.Unquote(quoted)
			if err != nil {
				log.Fatalln(err)
			}
			return u
		}
		quoted += string(b)
		prev = b
	}
}

type reader struct {
	buf *bufio.Reader
}

func (r *reader) read() rune {
	b, _, err := r.buf.ReadRune()
	if err != nil {
		log.Fatalln(err)
	}
	return b
}

func (r *reader) unread() {
	if err := r.buf.UnreadRune(); err != nil {
		log.Fatalln(err)
	}
}
