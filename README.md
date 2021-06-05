## Distributed Transactions
Support for transactions that read and write to distributed objects while ensuring Atomicity, Consistency, Isolation, Deadlock Resolution.
## Building and Running Instructions
Client
```sh
go build -o client client.go
```
```sh
./client <CLIENT_ID> <CONFIG_FILE>
```
Server
```sh
go build -o server server.go server_processing.go server_utils.go
```
```sh
./server <BRANCH_NAME> <CONFIG_FILE>
```
## Design Document
We use timestamped concurrency control and 2PC (2 part commit) to ensure serial equivalence of transactions. We have a map that contains the information needed for all accounts, which includes a committed entry and 2 sorted slices (arrays) that are the RTS (read timestamp) list and TW (tentative write) list. The entries in the contain a Timestamp_Id field, which is the timestamp concatenated with the client id (this concatenation allows for tiebreakers in the extremely rare case where two transactions have the same timestamp).
We followed the lecture implementation of read and write rules. Reads only occur if the TS (timestamp_Id) is greater than the committed object TS, and writes only occur if the TS is greater than the committed object TS and maximum RTS. All reads will wait until there are no entries in the TW list that have smaller TS, and reads will read the largest TS entry that is less than or equal to its own TS. There is also a lock that ensures only one read or write occurs at a time, but the reads that need to wait will wait on a condition variable that is notified when transactions are committed, which ensures that other transactions will not be blocked.
Our 2PC implementation only sends prepare messages to the branches that were contacted, which we keep track of in a slice for each transaction.
