package main

import (
	"bufio"
	"encoding/json"
	"log"
	"os"
	"strconv"
)

/*
1600535204.739027 [0 147.75.98.182:55466] "EVALSHA" "5526dff96b990cafa337d1313ebb81e251cef801" "1" "usage-stats.count-min-sketch-12h.1600516800.lga04" "10000000" "7" "1603108800"
1600535204.739079 [0 lua] "EXISTS" "usage-stats.count-min-sketch-12h.1600516800.lga04"
1600535204.739447 [0 147.75.98.182:55466] "EVALSHA" "e0eca3c1ee888a67ac312fef42a3ef8d8b2a595b"
1257894000.000000
*/

type Monitor struct {
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
		mon := readMonitor(r)
		if err := enc.Encode(mon); err != nil {
			log.Fatalln(err)
		}
	}
}

func readMonitor(r *reader) Monitor {
	var mon Monitor
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

var eof = byte(0)
