package main

import (
	"bufio"
	"fmt"
	"net"

	// Uncomment this block to pass the first stage

	"os"
	"io" // check if this one is needed
	"github.com/jessevdk/go-flags"
	"strconv"
	"time"
	// "errors"
	"strings"
	"encoding/hex"
	"errors"
	"sync"
	// "log"
	// "bytes"
	// "bufio"
)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	var opts Opts
	_, err := flags.Parse(&opts)
	if err != nil {
		fmt.Println("Error parsing flags %v", err)
		os.Exit(1)
	}

	opts.Config()
	if opts.Role != "master" {
		fmt.Println("Connecting with master....")
		go handleReplica(opts)
		// if err != nil {
		// 	os.Exit(1)
		// }
	}
	handleMaster(opts)
}
type SimpleString string
func handleMaster(opts Opts) {
	listener, err := net.Listen("tcp", "0.0.0.0:" + opts.Port)
	if err != nil {
		fmt.Println("Failed to bind to port", opts.Port)
		os.Exit(1)
	}
	masterConf := &MasterConfig{
		slaves: 	NewSlaves(),
		propOffset: 0,
	}
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting the connection: ", err)
			os.Exit(1)
		}
		//TODO: create func for this
		connection := &Connection{
			conn: conn,
			reader: bufio.NewReader(conn),
			offset: 0,
		}

		server := NewMaster(connection, opts, masterConf)
		go server.handle()
	}
}

func handleReplica(opts Opts) {
	conn, err := net.Dial("tcp", opts.MasterHost+":"+opts.MasterPort)
	if err != nil {
		fmt.Println("Error connecting to the master....", err, " ", opts.MasterPort)
		os.Exit(1)
	}

	connection := &Connection{
		conn:   conn,
		reader: bufio.NewReader(conn),
		offset: 0,
	}
	server := NewReplica(connection, opts)
	//handshake
	server.sendHandshake(opts)

	//handle
	server.handle()
}

func (conn *Connection) Read() (int, []string, error) {
	var offset int
	numBytes, numElements, err := conn.readLine()
	if errors.Is(err, io.EOF) {
		fmt.Println("Got EOF error", numBytes, " ", numElements)
		return 0, nil, io.EOF
	}
	if err != nil {
		return 0, nil, fmt.Errorf("readLine failed with error %v\n", err)
	}
	offset += numBytes

	if numElements[0] != '*' {
		return 0, nil, fmt.Errorf("Failure due to array format * not found")
	}

	len, err := strconv.Atoi(numElements[1:])
	if err != nil {
		return 0, nil, fmt.Errorf("Atoi call failed with %v", err)
	}
	var request []string
	for i :=0; i<len; i++ {
		bytes, line, err := conn.readLine()
		if err != nil {
			return 0, nil, fmt.Errorf("readLine failed with %v", err)
		}

		offset += bytes

		//check bulkstring
		if line[0] != '$' {
			return 0, nil, fmt.Errorf("not a bulkstring...")
		}
		_, err = strconv.Atoi(line[1:])
		if err != nil {
			return 0, nil, fmt.Errorf("failed during Atoi operation with %v", err)
		}

		bytes, str, err := conn.readLine()
		if err != nil {
			return 0, nil, fmt.Errorf("readLine failed with error %v", err)
		}

		offset += bytes

		request = append(request, str)
	}
	return offset, request, nil
}
func (server *Server) handlePing() (error) {
	if server.opts.Role == "master" {
		_, err :=server.conn.conn.Write([]byte("+PONG\r\n"))
		if err != nil {
			return fmt.Errorf("HandlePing::Write to connection failed with %v", err)
		}
		return nil
	}
	return nil
	// return fmt.Errorf("PING received on non master node")
}

func EncodeBulkString(str string) string {
	return fmt.Sprintf("$%d\r\n%s\r\n", len(str), str)
}

func (server *Server) handleEcho(message string) (error) {
	_, err :=server.conn.conn.Write([]byte(EncodeBulkString(message)))
	if err != nil {
		return fmt.Errorf("Echo failed to write with %v", err)
	}
	return nil
}

func (server *Server) handleInfo(argument string) (error) {
	var info strings.Builder
	if argument != "replication" {
		return fmt.Errorf("Error: incorrect arguments....")
	}
	info.WriteString("# Replication\r\n")
	info.WriteString("role:" + server.opts.Role + "\r\n")
	info.WriteString("master_replid:" + server.opts.ReplicaId + "\r\n")
	info.WriteString("master_repl_offset:" + strconv.FormatInt(int64(server.mc.propOffset), 10) + "\r\n")
	_, err :=server.conn.conn.Write([]byte(EncodeBulkString(info.String())))
	if err != nil {
		return fmt.Errorf("Write to connection failed with %v", err)
	}
	return nil
}

func (server *Server) handlePsync(request []string) (error) {
	emptyrdb, err := hex.DecodeString("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2")
	if err != nil {
		return fmt.Errorf("Error while decoding the hex string with rdb file content")
	}
	if request[0] == "?" {
		// initially the offset will be zero
		// _, err =server.conn.conn.Write([]byte(EncodeBulkString(string("FULLRESYNC " + server.opts.ReplicaId + " " + strconv.Itoa(server.mc.propOffset)))))
		_, err = server.conn.conn.Write([]byte(string("+FULLRESYNC " + server.opts.ReplicaId + " 0\r\n")))
		if err != nil {
			return fmt.Errorf("Write to connection failed with %v", err)
		}
	}
	_, err = server.conn.conn.Write([]byte("$" + strconv.Itoa(len(emptyrdb)) + "\r\n" + string(emptyrdb)))
	// _, err =server.conn.conn.Write([]byte(EncodeBulkString(string(emptyrdb))))
	if err != nil {
		return fmt.Errorf("Write to connection failed with %v", err)
	}
	//Add yo slaves
	fmt.Println("Adding to the slaves......")
	server.mc.slaves.AddToSlaves(server.conn.conn.RemoteAddr(), server.conn)
	return nil
}

func (server *Server) handleReplconf(request []string) (error) {
	fmt.Println("got handleReplconf......")
	switch strings.ToUpper(request[0]) {
	case "ACK":
		// For the master
		ack, err := strconv.Atoi(request[1])
		if err != nil {
			return fmt.Errorf("error with strconv.Atoi %v", err)
		}
		err = server.mc.slaves.HandleAck(server.conn.conn.RemoteAddr(), ack)
		if err != nil {
			return fmt.Errorf("ack handling failed with error %v", err)
		}
		if server.mc.wg != nil {
			fmt.Println("sending done on waitgroup......")
			server.mc.wg.Done()
		}
	case "GETACK":
		// For the slaves
		offset := server.conn.offset
		fmt.Println("Responding to the getAck command......")
		response := fmt.Sprintf("*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$%d\r\n%d\r\n", len(strconv.Itoa(offset)), offset)
		_, err := server.conn.conn.Write([]byte(response))
		if err != nil {
			return fmt.Errorf("send ACK failed with error %v", err)
		}
		server.conn.offset += 37
	default:
		_, err := server.conn.conn.Write([]byte("+OK\r\n"))
		if err != nil {
			return fmt.Errorf("Error writing to connection %v", err)
		}
	}
	return nil
}

func (server *Server) handleGet(request []string) (error) {
	value, err := server.storage.GetFromDataBase(request[0])
	var response string
	if err != nil {
		response = "$-1\r\n"
	} else {
		response = EncodeBulkString(*value)
	}
	_, err = server.conn.conn.Write([]byte(response))
	if err != nil {
		fmt.Errorf("connection write failed with %v", err)
	}
	return nil
}
//TODO: Check again below 2 functions
func ToBulkString(s string) string {
	return fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)
}
func ToRespArray(arr []string) string {
	respArr := fmt.Sprintf("*%d\r\n", len(arr))
	for _, s := range arr {
		respArr += ToBulkString(s)
	}

	return respArr
}
func (server *Server) handleSet(request []string) (error) {
	// EX for second, PX for milli second
	key := request[0]
	value := request[1]
	var expiry time.Time
	if len(request) == 4 {
		val, _ := strconv.Atoi(request[3])
		if strings.ToUpper(request[2]) == "PX" {
			expiry = time.Now().Add(time.Duration(val) * time.Millisecond)
		} else { //EX
			expiry = time.Now().Add(time.Duration(val) * time.Second)
		}
	}
	server.storage.AddToDataBase(key, value, expiry)
	if server.opts.Role == "master" {
		_, err := server.conn.conn.Write([]byte("+OK\r\n"))
		if err != nil {
			return fmt.Errorf("writing to connection failed with error %v", err)
		}
	}
	return nil
}

// TODO: Should we keep this or move it somewhere else??
var (
	waitLock = sync.Mutex{}
)

// should be called olny by the master, should add a check??
func (slaves *Slaves) getSynchronizedSlavesCount(server *Server) (int, error) {
	if server.opts.Role != "master" {
		return 0, fmt.Errorf("Illegal call to getSynchronizedSlavesCount by non master server")
	}
	slaves.lock.RLock()
	defer slaves.lock.RUnlock()

	count := 0
	for _, slave := range slaves.list {
		if slave.offset == server.mc.propOffset {
			count++
		}
	}
	return count, nil
}

func (server *Server) handleWait(request []string) (error) {
	numReplicas, err := strconv.Atoi(request[0])
	if err != nil {
		return fmt.Errorf("Atoi fialed with error %v", err)
	}

	timeout, err := strconv.Atoi(request[1])
	// timeout /= 10

	if err != nil {
		return fmt.Errorf("Atoi failed with error %v", err)
	}

	syncCount, err := server.mc.slaves.getSynchronizedSlavesCount(server)
	if syncCount == server.mc.slaves.Count() {
		server.conn.conn.Write([]byte(":" + strconv.Itoa(server.mc.slaves.Count()) + "\r\n"))
		return nil
	}
	server.mc.slaves.lock.RLock()
	for _, slave := range server.mc.slaves.list {
		_, err = slave.conn.Write([]byte("*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n"))
		if err != nil {
			server.mc.slaves.lock.RUnlock()
			return fmt.Errorf("Error while writing to connection %v", err)
		}
	}
	server.mc.slaves.lock.RUnlock()

	server.mc.wg = &sync.WaitGroup{}
	server.mc.wg.Add(min(numReplicas, server.mc.slaves.Count()))

	ch := make(chan struct{})
	go func() {
		defer close(ch)
		server.mc.wg.Wait()
	}()
	
	select {
	case <-ch:
	case <-time.After(time.Duration(timeout) * time.Millisecond):
	}

	syncCount, err = server.mc.slaves.getSynchronizedSlavesCount(server)
	if err != nil {
		return fmt.Errorf("getSynchronizedSlavesCount failed with %v", err)
	}
	_, err = server.conn.conn.Write([]byte(":" + strconv.Itoa(syncCount) + "\r\n"))
	if err != nil {
		return fmt.Errorf("Write to conn failed with %v", err)
	}
	server.mc.propOffset += 37
	return nil
}

func (server *Server) handleConfig(request []string) error {
	switch(strings.ToUpper(request[0])) {
	case "GET":
		query := request[1]
		if query == "dir" {
			_, err := server.conn.conn.Write([]byte("*2\r\n" + EncodeBulkString("dir") + EncodeBulkString(server.opts.Dir)))
			if err != nil {
				return fmt.Errorf("config handling failed with %v", err)
			}
		} else if query == "dbfilename" {
			_, err := server.conn.conn.Write([]byte("*2\r\n" + EncodeBulkString("dbfilename") + EncodeBulkString(server.opts.DbFileName)))
			if err != nil {
				return fmt.Errorf("config handling failed with %v", err)
			}
		}
	}
	return nil
}

func (server *Server) handle() {
	defer server.conn.conn.Close()

	for {
		offset, request, err := server.conn.Read()
		if err != nil {
			fmt.Printf("Read failed with %v", err)
			return
		}
		fmt.Println("Offset here", server.conn.conn.RemoteAddr(), " offset = ", offset)
		//handle the requests here....

		switch strings.ToUpper(request[0]) {
		case "PING":
			err = server.handlePing()
			offset = 14
		case "REPLCONF":
			fmt.Println("got REPLCONF......")
			if len(request) < 2 {
				fmt.Println("Ill formed command REPLCONF")
				return
			}
			err = server.handleReplconf(request[1:])
			//handle replconf
		case "PSYNC":
			if len(request) != 3 {
				fmt.Println("Ill formed command PSYNC")
				return
			}
			err = server.handlePsync(request[1:])
			//handle psync
		case "ECHO":
			if len(request) != 2 {
				fmt.Println("%s expects at least 1 argument", request[0]) 
				return
			}
			err = server.handleEcho(request[1])
		case "SET":
			if len(request) < 2 {
				fmt.Println("Ill formed command SET")
				return
			}
			err = server.handleSet(request[1:])
			fmt.Println("Handled Set on master........")
			if server.opts.Role == "master" {
				server.mc.slaves.lock.RLock()
				fmt.Println("starting the loop....", len(server.mc.slaves.list))
				for _, slave := range server.mc.slaves.list {
					fmt.Println("....sending request to slaves : ", ToRespArray(request))
					slave.conn.Write([]byte(ToRespArray(request)))
				}
				fmt.Println("done sending to slaves.....")
				server.mc.slaves.lock.RUnlock()
				server.mc.propOffset += len(ToRespArray(request))
			}
		case "GET":
			if len(request) != 2 {
				fmt.Println("Ill formed command GET")
				return
			}
			err = server.handleGet(request[1:])
		case "INFO":
			if len(request) != 2 {
				fmt.Println("%s expects at least 1 argument", request[0]) 
				return
			}
			err = server.handleInfo(request[1])
		case "WAIT":
			//handle wait
			waitLock.Lock()
			err = server.handleWait(request[1:])
			waitLock.Unlock()
		case "CONFIG":
			if len(request) != 3 {
				fmt.Println("Ill formed command", request[0])
				return
			}
			err = server.handleConfig(request[1:])
		default:
			//handle default
		}
		if err != nil {
			fmt.Println("command %s failed %v", request[0], err)
			return
		}

		if server.opts.Role != "master" && (len(request) <= 1 || request[1] != "GETACK") {
			server.conn.offset += offset
		}
	}
}

func (conn *Connection) readLine() (int, string, error) {
	// str, err := conn.reader.ReadString('\n')
	// str1, _, err := conn.reader.ReadLine()
	// str := string(str1)
	str, err := conn.reader.ReadString('\n')
	fmt.Println("ReadLine string: ", str)
	bytesRead := len(str)
	if len(str) > 0 {
		//removing /r/n from the end
		if str[len(str) - 1] == '\n' {
			str = str[:len(str) - 1]
		}

		if str[len(str) - 1] == '\r' {
			str = str[:len(str) - 1]
		}
	}

	return bytesRead, str, err
}

func (server *Server) sendHandshake(opts Opts) error {
	fmt.Println("sending ping")

	_, err := server.conn.conn.Write([]byte("*1\r\n$4\r\nPING\r\n"))
	if err != nil {
		fmt.Println("Error while connecting to the master %v", err)
		os.Exit(1)
	}
	
	_, response, err := server.conn.readLine()
	if err != nil {
		//TODO: what is %v here??
		return fmt.Errorf("reading from connection failed %v", err)
	}
	
	if response != "+PONG" {
		return fmt.Errorf("didn't receive \"PONG\": received %s", response);
	}

	_, err = server.conn.conn.Write([]byte("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n6380\r\n"))
	if err != nil {
		fmt.Println("Error while connecting to the master %v", err)
		os.Exit(1)
	}

	_, response, err =server.conn.readLine()
	if err != nil {
		return fmt.Errorf("reading from connection failed %v", err)
	}

	if response != "+OK" {
		return fmt.Errorf("didn't receive \"OK\", instead received %s", response);
	}

	fmt.Println("sending second replconf")

	_, err =server.conn.conn.Write([]byte("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"))
	if err != nil {
		fmt.Println("Error while connecting to the master .....")
		os.Exit(1)
	}

	_, response, err =server.conn.readLine()
	if err != nil {
		return fmt.Errorf("reading from connection failed %v", err)
	}

	if response != "+OK" {
		return fmt.Errorf("didn't receive \"OK\", instead received %s", response);
	}

	fmt.Println("sending psync")

	_, err = server.conn.conn.Write([]byte("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"))
	if err != nil {
		fmt.Println("Error while connecting to the master %v", err)
		os.Exit(1)
	}

	_, response, err = server.conn.readLine()
	if err != nil {
		return fmt.Errorf("reading from connection failed %v", err)
	}

	if response[:11] != "+FULLRESYNC" {
		return fmt.Errorf("didn't receive \"FULLRESYNC\", instead received %s", response);
	}

	_, response, err = server.conn.readLine()
	fmt.Println("Recieved RDB File", response)
	if err != nil {
		return fmt.Errorf("reading from connection failed %v", err)
	}

	if response[0] != '$' {
		return fmt.Errorf("Expected $, got %c", response[0])
	}

	rdbFileLen, err := strconv.Atoi(response[1:])
	if err != nil {
		return fmt.Errorf("Atoi failed with %v", err)
	}

	//TODO: should we store this content somewhere??
	temp := make([]byte, rdbFileLen)
	rdbContent, err := io.ReadFull(server.conn.reader, temp)
	if err != nil {
		return fmt.Errorf("ReadFull fialed with error %v", err)
	}
	fmt.Println("Read RDB content length", rdbContent)

	return nil
}
