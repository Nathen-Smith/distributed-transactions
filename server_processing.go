package main

import (
	"bufio"
	"encoding/gob"
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ------------------
// exported functions
// ------------------
func Handle_Receive_Server(conn net.Conn) {
	var message Message
	var resp *Message
	enc := gob.NewEncoder(conn)
	dec := gob.NewDecoder(conn)

	// send message to notify ready
	fmt.Fprintln(conn, "ready")
	err := dec.Decode(&message)
	DPrintf("\nRECEIVED: %#v", message)

	if err != nil {
		return
	}

	// process message
	switch message.Category {
	case "read":
		resp = remote_read(&message)
	case "write":
		resp = remote_write(&message)
	case "commit":
		resp = remote_commit_abort(&message)
	default:
		DPrintf("ERROR message category not recognized:" + message.Category)
	}

	// send back response
	DPrintf("\nREPLY: %#v", resp)
	enc.Encode(&resp)
	conn.Close()
}

func Handle_Receive_Client(conn net.Conn, reader *bufio.Reader) {
	defer conn.Close()
	client_id, _ := reader.ReadString('\n')
	client_id = client_id[:len(client_id)-1]
	ts_id := strconv.FormatInt(int64(time.Now().UnixNano()), 10) + client_id
	DPrintf(client_id + " connected at " + ts_id)

	branches_contacted := make(map[string]bool)
	for {
		// receive command
		line, err := reader.ReadString('\n')
		if err != nil {
			DPrintf(client_id + " disconnected")
			abort_transaction(ts_id, branches_contacted)
			return
		}
		line = line[:len(line)-1]
		DPrintf(line)
		status := 0

		// parse message
		action := ""
		branch := ""
		account := ""
		amount := 0
		line = strings.ReplaceAll(line, ".", " ")
		fmt.Sscanf(line, "%s %s %s %d", &action, &branch, &account, &amount)

		// handle message
		switch action {
		case "BALANCE":
			amount, status = get_balance(ts_id, branch, account)
		case "DEPOSIT":
			status = update_balance(ts_id, branch, account, amount)
		case "WITHDRAW":
			status = update_balance(ts_id, branch, account, -amount)
		case "COMMIT":
			status = commit_transaction(ts_id, branches_contacted)
		case "ABORT":
			status = abort_transaction(ts_id, branches_contacted)
		default:
			DPrintf("ERROR action not recognized:%s, line:%s", action, line)
		}

		// add branch to contacted set if valid branch
		if _, ok := branch_address_map[branch]; ok {
			branches_contacted[branch] = true
		}
		
		// return status
		if action == "BALANCE" && status == 0 {
			fmt.Fprintf(conn, "%d|%d\n", status, amount)
		} else {
			fmt.Fprintf(conn, "%d\n", status)
		}

		// safely exit if commit
		if status == 1 {
			return
		}
	}
}

// ---------
// functions
// ---------
func get_balance(ts_id string, branch string, account string) (amount int, status int) {
	amount, status = _read(ts_id, branch, account)
	return
}

func update_balance(ts_id string, branch string, account string, amount int) (status int) {
	balance, status := _read(ts_id, branch, account)
	switch status {
	case 0:
		balance += amount
	case -1:
		return
	case -2: // create new account
		if amount < 0 {
			return
		}
		balance = amount
	default:
		DPrintf("ERROR status not recognized:%d", status)
	}
	status = _write(ts_id, branch, account, balance)
	return
}

// 2PC
// sendes [commit, status: 0] to all (prepare commit).
// If all respond [commit, status: 1] (can commit):
//   send back [commit, status: 1] (confirm commit)
// else (cannot commit):
//   send back [commit, status: -1] (abort)
func commit_transaction(ts_id string, branches_contacted map[string]bool) (status int) {
	message := &Message{
		Timestamp_Id: ts_id,
		Category:     "commit",
	}

	// confirm commit
	n := len(branches_contacted)
	count := 0
	finished := 0
	var mu sync.Mutex
	cond := sync.NewCond(&mu)

	for key := range branches_contacted {
		go func(branch string) {
			resp := _rpc(branch, message)

			mu.Lock()
			defer mu.Unlock()
			if resp.Status == 1 {
				count++
			}
			finished++
			cond.Broadcast()
		}(key)
	}

	// wait for all responses or one says abort
	mu.Lock()
	defer mu.Unlock()
	for finished != n && count == finished {
		cond.Wait()
	}

	// set status
	if count == n {
		status = 1
	} else {
		status = -1
	}

	// send can commit/abort message to branches
	_commit_abort_commands(ts_id, branches_contacted, count == n)

	return
}

func abort_transaction(ts_id string, branches_contacted map[string]bool) (status int) {
	_commit_abort_commands(ts_id, branches_contacted, false)
	status = -1
	return
}

// read
//
// Transaction Tc requests a read operation on object D
//
// if (Tc > write timestamp on committed version of D) {
// 	//search across the committed timestamp and the TW list for object D.
// 	Ds = version of D with the maximum write timestamp that is ≤ Tc
//
// 	if (Ds is committed)
// 	read Ds and add Tc to RTS list (if not already added)
// 	else
// 		if Ds was written by Tc, simply read Ds
// 		else
// 			wait until the transaction that wrote Ds is committed or aborted, and
// 			reapply the read rule.
// 			// if the transaction is committed, Tc will read its value after the wait.
// 			// if the transaction is aborted, Tc will read the value from an older
// 			transaction.
// } else {
// 	abort transaction Tc
// 	//too late; a transaction with later timestamp has already written the object.
// }
func remote_read(message *Message) (response *Message) {
	accounts_lock.Lock()
	defer accounts_lock.Unlock()

	response = &Message{
		message.Timestamp_Id,
		message.Category,
		message.Account,
		message.Amount,
		-1, // default to abort
	}

	var committed_entry *Entry
	
	for {
		// check if account exists
		if _, ok := accounts_map[message.Account]; !ok {
			response.Status = -2
			return
		}
		committed_entry = accounts_map[message.Account].Committed_Entry
		// apply read rule
		if committed_entry == nil || message.Timestamp_Id > committed_entry.Timestamp_Id {
			ds := _find_max_write_lte(message)
			if ds != nil && (ds == committed_entry || ds.Timestamp_Id == message.Timestamp_Id) {
				DPrintf("ds:%v, committed_entry:%v, message.ts_id:%v", ds, committed_entry, message.Timestamp_Id)
				_read_and_add_rts(ds, response)
				response.Status = 0
				break
			}
		} else {
			// abort
			break
		}
		// wait until commits/aborts and reapply read rule
		accounts_cond.Wait()
		DPrintf("reapplying read rule")
	}

	return
}

// write
//
// Transaction Tc requests a write operation on object D
//
// if (Tc ≥ max. read timestamp on D
// 	&& Tc > write timestamp on committed version of D)
//
// 	// tentative write
// 	If Tc already has an entry in the TW list for D, update it.
// 	Else, add Tc and its write value to the TW list.
// else
// 	abort transaction Tc
// 	//too late; a transaction with later timestamp has already read or
// 	written the object.
func remote_write(message *Message) (response *Message) {
	// var accounts = make(map[string]*Entry)
	// var rts_list []*Entry // read timestamps list
	// var tw_list []*Entry  // tentative write list

	// type Entry struct {
	// 	Timestamp_Id string
	// 	Value        string // empty if read
	// }
	accounts_lock.Lock()
	defer accounts_lock.Unlock()

	response = &Message{
		message.Timestamp_Id,
		message.Category,
		message.Account,
		message.Amount,
		-1, // default to abort
	}

	// just create account if account does not exist
	if _, ok := accounts_map[message.Account]; !ok {
		if message.Amount < 0 {
			// should never happen, but just in case
			response.Status = -2
			return
		}
		new_account := &Account{}
		new_entry := &Entry{
			Timestamp_Id: message.Timestamp_Id,
			Value: message.Amount,
		}
		new_account.TW_List = append(new_account.TW_List, new_entry)
		new_account.RTS_List = append(new_account.RTS_List, new_entry)
		accounts_map[message.Account] = new_account
		response.Status = 0
		return
	}

	// apply write rule
	account := accounts_map[message.Account]
	new_entry := &Entry{
		Timestamp_Id: message.Timestamp_Id,
		Value: message.Amount,
	}
	if (len(account.RTS_List) == 0 || message.Timestamp_Id >= account.RTS_List[len(account.RTS_List)-1].Timestamp_Id) &&
	(account.Committed_Entry == nil || message.Timestamp_Id > account.Committed_Entry.Timestamp_Id) {
		idx := _get_item_index(account.TW_List, new_entry)
		if idx != -1 {
			account.TW_List[idx].Value = message.Amount
		} else {
			// append. There can not be a TW inserted before another TW because there is
			// always a read paired with a write.
			account.TW_List = append(account.TW_List, new_entry)
		}
		response.Status = 0
	}

	return
}

// commit (prepare is status==0, commit is status==1, abort is status==-1)
//
// When a transaction is committed, the committed value of the object
// and associated timestamp are updated, and the corresponding write is
// removed from TW list.
//
// Note: there can not be a TW inserted before another TW because there is
// always a read paired with a write.
//
// If commit(status == 1), print the balance of all accounts with non-zero values.
func remote_commit_abort(message *Message) (response *Message) {
	accounts_lock.Lock()
	defer accounts_lock.Unlock()
	
	response = &Message{
		message.Timestamp_Id,
		message.Category,
		message.Account,
		message.Amount,
		-1, // default to abort
	}

	search_entry := &Entry{ Timestamp_Id: message.Timestamp_Id }

	// prepare (status == 0)(check if can commit)
	if message.Status == 0 {
		for _, account := range accounts_map {
			// check if entry exists in tw_list
			idx := _get_item_index(account.TW_List, search_entry)
			if idx != -1 {
				// abort if entry is negative
				if account.TW_List[idx].Value < 0 {
					return
				}
				// continuously check until entry is first
				for {
					if account.TW_List[0].Timestamp_Id == search_entry.Timestamp_Id {
						// can commit for this entry
						break
					}

					// not first entry. Wait until something is committed or aborted
					accounts_cond.Wait()

					if !_contains(account.TW_List, search_entry) {
						// got aborted, just return
						return
					}
				}
			}
		}
		// can commit
		response.Status = 1
		return
	}

	// set status
	response.Status = message.Status // not needed, but for consistency

	// notify all waits after commit/abort
	defer accounts_cond.Broadcast()

	// commit/abort and delete from tw_list
	for account_key, account := range accounts_map {
		idx := _get_item_index(account.TW_List, search_entry)
		if idx == -1 {
			continue
		}
		if message.Status == 1 { // commit
			account.Committed_Entry = account.TW_List[idx]
		} else { // abort
			// if no committed entry, then delete account
			if account.Committed_Entry == nil {
				delete(accounts_map, account_key)
				return
			}
			// delete from rts_list as well
			rts_idx := _get_item_index(account.RTS_List, search_entry)
			if rts_idx != -1 {
				account.RTS_List = _remove_idx(account.RTS_List, rts_idx)
			}
		}
		// delete from tw_list
		account.TW_List = _remove_idx(account.TW_List, idx)
	}

	// print out all balances if commit
	var keys []string
	if message.Status == 1 {
		balances_output := "BALANCES"
		for account_key, account := range accounts_map {
			if account.Committed_Entry != nil && account.Committed_Entry.Value > 0 {
				keys = append(keys, account_key)
			}
		}
		sort.Strings(keys)
		for _, key := range keys {
			balances_output += " "+self_branch+"."+key+":"+strconv.Itoa(accounts_map[key].Committed_Entry.Value)
		}
		fmt.Println(balances_output)
	}

	return
}

func _remove_idx(list []*Entry, idx int) []*Entry {
    return append(list[:idx], list[idx+1:]...)
}

// -------
// helpers
// -------
func _read(ts_id string, branch string, account string) (amount int, status int) {
	// create message
	message := &Message{
		Timestamp_Id: ts_id,
		Category:     "read",
		Account:      account,
	}

	// call rpc
	resp := _rpc(branch, message)
	amount = resp.Amount
	status = resp.Status
	return
}

func _write(ts_id string, branch string, account string, amount int) (status int) {
	message := &Message{
		Timestamp_Id: ts_id,
		Category:     "write",
		Account:      account,
		Amount:       amount,
	}

	// call rpc
	resp := _rpc(branch, message)
	status = resp.Status
	return
}

// commit if true, else abort
func _commit_abort_commands(ts_id string, branches_contacted map[string]bool, commit bool) {
	message := &Message{
		Timestamp_Id: ts_id,
		Category:     "commit",
		Status:       -1,
	}

	if commit {
		message.Status = 1
	}

	for key := range branches_contacted {
		go func(branch string) {
			_rpc(branch, message)
		}(key)
	}
}

func _rpc(branch string, message *Message) Message {
	var response Message
	enc, dec := _create_server_connection(branch)

	// send message
	DPrintf("sending _rpc: %#v", message)
	enc.Encode(message)

	// receive response
	dec.Decode(&response)
	DPrintf("received _rpc: %#v", response)
	return response
}

func _create_server_connection(branch string) (enc *gob.Encoder, dec *gob.Decoder) {
	address := branch_address_map[branch]
	conn, err := net.Dial("tcp", address) // should be no error
	if err != nil {
		DPrintf("ERROR at _create_server_connection:%s, branch:%s, address:%s",err,branch,address)
	}
	fmt.Fprintf(conn, "server\n")
	reader := bufio.NewReader(conn)
	_, _ = reader.ReadString('\n') // make sure server responds back
	enc = gob.NewEncoder(conn)
	dec = gob.NewDecoder(conn)
	return
}

// account exists if this function called
func _find_max_write_lte(message *Message) (entry *Entry) {
	entry = accounts_map[message.Account].Committed_Entry
	for _, e := range accounts_map[message.Account].TW_List {
		if e.Timestamp_Id > message.Timestamp_Id {
			break
		}
		entry = e
	}
	return
}

func _read_and_add_rts(entry *Entry, response *Message) {
	if entry == nil {
		response.Amount = 0
	} else {
		response.Amount = entry.Value
	}
	new_entry := &Entry{Timestamp_Id: response.Timestamp_Id}

	// add if not in rts_list
	if !_contains(accounts_map[response.Account].RTS_List, new_entry) {
		accounts_map[response.Account].RTS_List = _insert_sort(accounts_map[response.Account].RTS_List, new_entry)
	}

}

func _get_item_index(list []*Entry, item *Entry) int {
	for i, v := range list {
		if v.Timestamp_Id == item.Timestamp_Id {
			return i
		}
	}
	return -1
}

func _insert_sort(list []*Entry, item *Entry) []*Entry {
	if len(list) == 0 {
		return append(list, item)
	}
	idx := sort.Search(len(list), func(i int) bool { return list[i].Timestamp_Id > item.Timestamp_Id })
	list = append(list, &Entry{})
	copy(list[idx+1:], list[idx:])
	list[idx] = item
	return list
}

// true if Timestamp_Id of item is in list, else false
func _contains(list []*Entry, item *Entry) bool {
	for _, v := range list {
		if v.Timestamp_Id == item.Timestamp_Id {
			return true
		}
	}
	return false
}
