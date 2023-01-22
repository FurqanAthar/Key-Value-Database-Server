// Implementation of a KeyValueServer. Students should write their code in this file.

package pa1

import (
	"DS_PA1/rpcs"
	"bufio"
	"bytes"
	"fmt"
	"net"
	"net/http"
	"net/rpc"
)

type keyValueServer struct {
	reqChan          chan putGetStruct
	broadcastChan    chan string
	clientHandleChan chan clientHandleStruct
	countResponse    chan int
	clients          map[*bufio.ReadWriter]chan string
	// TODO: implement this!
}

func New() KeyValueServer {
	s := &keyValueServer{make(chan putGetStruct), make(chan string, 1), make(chan clientHandleStruct), make(chan int), make(map[*bufio.ReadWriter]chan string, 0)}
	return s
}

type putGetStruct struct {
	which           string
	key             string
	value           []byte
	getResponseChan *chan []byte
}

type clientHandleStruct struct {
	which             string
	readerWriter      *bufio.ReadWriter
	message           string
	getMessageChannel *chan chan string
}

func (kvs *keyValueServer) StartModel1(port int) error {
	// TODO: implement this!
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", port))

	if err != nil {
		return err
	}

	initDB()
	go kvs.handleClient()
	// go kvs.broadcastValue()
	go kvs.HandleConsistency()

	// listen forever
	go kvs.listen1forever(ln)

	return nil
}

func (kvs *keyValueServer) Close() {
	// TODO: implement this!
	return
}

func (kvs *keyValueServer) Count() int {
	// TODO: implement this!
	kvs.clientHandleChan <- clientHandleStruct{"count", nil, "", nil}
	return <-kvs.countResponse
}

func (kvs *keyValueServer) StartModel2(port int) error {
	initDB()
	go kvs.HandleConsistency()

	rpcServer := rpc.NewServer()
	rpcServer.Register(rpcs.Wrap(kvs))
	http.DefaultServeMux = http.NewServeMux()
	rpcServer.HandleHTTP(rpc.DefaultRPCPath, rpc.DefaultDebugPath)
	// rpcServer

	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return err
	}

	go http.Serve(ln, nil)

	return nil
}

func (kvs *keyValueServer) RecvGet(args *rpcs.GetArgs, reply *rpcs.GetReply) error {
	// TODO: implement this!
	getResponseChannel := make(chan []byte, 1)
	kvs.reqChan <- putGetStruct{"get2", args.Key, []byte(""), &getResponseChannel}
	str := <-getResponseChannel
	reply.Value = str

	return nil
}

func (kvs *keyValueServer) RecvPut(args *rpcs.PutArgs, reply *rpcs.PutReply) error {
	// TODO: implement this!
	kvs.reqChan <- putGetStruct{"put", args.Key, args.Value, nil}
	return nil
}

// TODO: add additional methods/functions below!

func (kvs *keyValueServer) listen1forever(ln net.Listener) error {
	for {
		conn, err := ln.Accept()
		// handle successful connections concurrently
		if err != nil {
			return err
		} else {
			go kvs.handleConnection(conn)
		}
	}
}

func (kvs *keyValueServer) handleConnection(conn net.Conn) {
	// obtain a buffered reader / writer on the connection
	rw := ConnectionToRW(conn)

	defer kvs.Clean(rw, conn)

	kvs.clientHandleChan <- clientHandleStruct{"add", rw, "", nil}

	go kvs.singleClientMessages(rw)

	// kvs.clients = append(kvs.clients, rw)
	var breakLoop = false

	for {
		if breakLoop {
			break
		}
		// get client message
		msg, err := rw.ReadString('\n')
		msg1 := bytes.Trim([]byte(msg), "\n")
		if err != nil {
			breakLoop = true
		} else {
			input := bytes.Split(msg1, []byte(","))
			if string(input[0]) == "put" {
				kvs.reqChan <- putGetStruct{"put", string(input[1]), []byte(input[2]), nil}
			} else if string(input[0]) == "get" {
				kvs.reqChan <- putGetStruct{"get", string(input[1]), []byte(input[1]), nil}
			} else if string(input[0]) == "exit" {
				breakLoop = true
			}
		}
	}
	// conn.Close()
}

func (kvs *keyValueServer) singleClientMessages(readerWriter *bufio.ReadWriter) {
	messageListChan := make(chan chan string)
	kvs.clientHandleChan <- clientHandleStruct{"getChannel", readerWriter, "", &messageListChan}
	messagesList := <-messageListChan

	for {
		select {
		case m := <-messagesList:
			_, err := readerWriter.WriteString(m)
			if err != nil {
				fmt.Printf("There was an error writing to a client connection: %s\n", err)
				// return
			}
			err = readerWriter.Flush()
			if err != nil {
				fmt.Printf("There was an error writing to a client connection 2: %s\n", err)
				// return
			}
		}
	}
}

func (kvs *keyValueServer) HandleConsistency() {
	for {
		select {
		case req := <-kvs.reqChan:
			if req.which == "put" {
				put(req.key, req.value)
			} else if req.which == "get" {
				gValue := string(get(req.key))
				kvs.clientHandleChan <- clientHandleStruct{"broadcastMessage", nil, fmt.Sprintf("%s,%s\n", req.key, gValue), nil}
			} else if req.which == "get2" {
				gValue := get(req.key)
				*req.getResponseChan <- gValue
			}
		}
	}
}

func (kvs *keyValueServer) handleClient() {
	for {
		select {
		case req := <-kvs.clientHandleChan:
			if req.which == "add" {
				kvs.clients[req.readerWriter] = make(chan string, 500)
			} else if req.which == "remove" {
				delete(kvs.clients, req.readerWriter)
			} else if req.which == "count" {
				kvs.countResponse <- len(kvs.clients)
			} else if req.which == "broadcastMessage" {
				for rw, mList := range kvs.clients {
					if len(mList) < 500 {
						kvs.clients[rw] <- req.message
					}
				}
			} else if req.which == "getChannel" {
				*req.getMessageChannel <- kvs.clients[req.readerWriter]
			}
		}
	}
}

// Clean closes a connection
func (kvs *keyValueServer) Clean(rw *bufio.ReadWriter, conn net.Conn) {
	kvs.clientHandleChan <- clientHandleStruct{"remove", rw, "", nil}
	conn.Close()
}

// ConnectionToRW takes a connection and returns a buffered reader / writer on it
func ConnectionToRW(conn net.Conn) *bufio.ReadWriter {
	return bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
}