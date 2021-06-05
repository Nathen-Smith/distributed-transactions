package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"sync"
)

// -------
// structs
// -------
type Message struct {
	Timestamp_Id string
	Category     string // read, write, commit
	Account      string
	Amount       int
	Status       int // 1: commit ok, 0: ok, -1: abort, -2: not found
}

type Entry struct {
	Timestamp_Id string
	Value        int // does not matter for rts_list
}

type Account struct {
	Committed_Entry *Entry
	RTS_List        []*Entry // read timestamps list (sorted)
	TW_List         []*Entry // tentative write list (sorted)
}

// ----------------
// global variables
// ----------------
var self_branch string
var branch_address_map = make(map[string]string)
var accounts_lock sync.Mutex                 // lock for accounts_map
var accounts_cond = sync.NewCond(&accounts_lock) 			 // condition broadcasted after commit/abort
var accounts_map = make(map[string]*Account) // all account entries

// ---------
// constants
// ---------
const NUM_BRANCHES int = 5
const ARG_NUM_SERVER int = 2

// ---------
// functions
// ---------
func main() {
	argv := os.Args[1:]
	if len(argv) != ARG_NUM_SERVER {
		fmt.Fprintf(os.Stderr, "usage: ./server <BRANCH_NAME> <CONFIG_FILE>\n")
		os.Exit(1)
	}

	branch, config_file := argv[0], argv[1]
	self_branch = branch

	// start routine: connect to all other servers
	serv_port := _parse_config_file(config_file, branch)

	// accept connections
	ln, err := net.Listen("tcp", ":"+serv_port)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			_handle_err(err, conn)
		}
		go _handle_receive(conn)
	}
}

// -------
// helpers
// -------
func _parse_config_file(file_name string, curr_branch string) (self_serv_port string) {
	f, err := os.Open(file_name)
	defer f.Close()

	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}

	var branch, serv_addr, serv_port string
	reader := bufio.NewReader(f)
	for i := 0; i < NUM_BRANCHES; i++ {
		line, _ := reader.ReadString('\n') // ignoring errors
		fmt.Sscanf(line, "%s %s %s\n", &branch, &serv_addr, &serv_port)
		if branch == curr_branch {
			self_serv_port = serv_port
		}
		branch_address_map[branch] = serv_addr + ":" + serv_port
		DPrintf(branch_address_map[branch])
	}
	return
}

func _handle_err(err error, conn net.Conn) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}
}

// redirect based on client or server connection
func _handle_receive(conn net.Conn) {
	reader := bufio.NewReader(conn)
	connection_type, _ := reader.ReadString('\n')
	connection_type = connection_type[:len(connection_type)-1]
	DPrintf("connection type:"+connection_type)
	if connection_type == "server" {
		//server
		Handle_Receive_Server(conn)
	} else {
		// client
		Handle_Receive_Client(conn, reader)
	}
}
