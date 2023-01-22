# Key-Value-Database-Server
## A Key-Value Database Server using RPCs.

This assignment is to develop a simple key-value database server in Go that implements PUT and GET requests concurrently using goroutines and channels. 
The server must also implement a Count() function and handle slow-reading clients by keeping a queue of outgoing messages.

### Requirements:
1) Concurrently handle PUT and GET requests from multiple clients
2) Implement PUT request: put,key,value
3) Implement GET request: get,key
4) Implement Count() function
5) Handle slow-reading clients by keeping a queue of outgoing messages (max 500)
