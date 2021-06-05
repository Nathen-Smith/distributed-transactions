package main

import (
	"bufio"
	"fmt"
	"math/rand"
	"net"
	"os"
	"strings"
)

const ARG_NUM_CLIENT int = 2

func main() {
	argv := os.Args[1:]
	if len(argv) != ARG_NUM_CLIENT {
		fmt.Fprintf(os.Stderr, "usage: ./client <client_id> <config.txt>\n")
		os.Exit(1)
	}

	client_id, config_file := argv[0], argv[1]
	servers := parse_config(config_file)

	var conn net.Conn
	var server_reader *bufio.Reader
	client_reader := bufio.NewReader(os.Stdin)
	in_transaction := false
	for {
		command, _ := client_reader.ReadString('\n')
		if command == "BEGIN\n" {
			in_transaction = true
			conn = initiate_connection(client_id, servers)
			server_reader = bufio.NewReader(conn)
		}

		for in_transaction {
			// 1) read in from stdin
			command, _ := client_reader.ReadString('\n')

			// 2) write to connection
			fmt.Fprintf(conn, command)

			// 3) read from connection (blocking until receive newline)
			status, _ := server_reader.ReadString('\n')
			status = status[:len(status)-1]
			action := ""
			account := ""
			balance := ""
			if len(status) > 2 {
				fmt.Sscanf(command, "%s %s", &action, &account)	
				split_response := strings.Split(status, "|")
				status = split_response[0]
				balance = split_response[1]
			}

			// 4) write to stdout based off server message
			switch status {
			case "1":
				in_transaction = false
				fmt.Fprintln(os.Stdout, "COMMIT OK")
			case "0":
				if command[:7] == "BALANCE" {
					fmt.Fprintln(os.Stdout, account+" = "+balance)
				} else {
					fmt.Fprintln(os.Stdout, "OK")
				}
			case "-1":
				in_transaction = false
				fmt.Fprintln(os.Stdout, "ABORTED")
			case "-2":
				in_transaction = false
				fmt.Fprintln(os.Stdout, "NOT FOUND, ABORTED")
			}

			if !in_transaction {
				conn.Close()
			}
		}
	}
}

func parse_config(config_file string) (servers []string) {
	f, err := os.Open(config_file)
	defer f.Close()

	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}
	
	var branch, addr, port string
	reader := bufio.NewReader(f)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			break
		}
		fmt.Sscanf(line, "%s %s %s\n", &branch, &addr, &port)
		servers = append(servers, addr+":"+port)
	}

	return
}

func initiate_connection(client_id string, servers []string) net.Conn {
	address := servers[rand.Intn(len(servers))]
	conn, err := net.Dial("tcp", address)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}
	fmt.Fprintln(conn, "client")
	fmt.Fprintln(conn, client_id)
	fmt.Fprintln(os.Stdout, "OK")
	return conn
}
