package main

import (
	"bufio"
	"fmt"
	"net"

	// Uncomment this block to pass the first stage

	"io" // check if this one is needed
	"os"
	"strconv"
	"time"

	"github.com/jessevdk/go-flags"

	// "errors"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"strings"
	"sync"
	// "log"
	// "bytes"
	// "bufio"
	//rdb parser library
)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	var opts Opts
	_, err := flags.Parse(&opts)
	if err != nil {
		fmt.Printf("error parsing flags %v\n", err)
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
	listener, err := net.Listen("tcp", "0.0.0.0:"+opts.Port)
	if err != nil {
		fmt.Println("Failed to bind to port", opts.Port)
		os.Exit(1)
	}
	masterConf := &MasterConfig{
		slaves:     NewSlaves(),
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
			conn:   conn,
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
		return 0, nil, fmt.Errorf("readLine failed with error %v", err)
	}
	offset += numBytes

	if numElements[0] != '*' {
		return 0, nil, fmt.Errorf("failure due to array format * not found")
	}

	len, err := strconv.Atoi(numElements[1:])
	if err != nil {
		return 0, nil, fmt.Errorf("atoi call failed with %v", err)
	}
	var request []string
	for i := 0; i < len; i++ {
		bytes, line, err := conn.readLine()
		if err != nil {
			return 0, nil, fmt.Errorf("readLine failed with %v", err)
		}

		offset += bytes

		//check bulkstring
		if line[0] != '$' {
			return 0, nil, fmt.Errorf("not a bulkstring")
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
func (server *Server) handlePing() error {
	if server.opts.Role == "master" {
		_, err := server.conn.conn.Write([]byte("+PONG\r\n"))
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

func (server *Server) handleEcho(message string) error {
	_, err := server.conn.conn.Write([]byte(EncodeBulkString(message)))
	if err != nil {
		return fmt.Errorf("echo failed to write with %v", err)
	}
	return nil
}

func (server *Server) handleInfo(argument string) error {
	var info strings.Builder
	if argument != "replication" {
		return fmt.Errorf("error: incorrect arguments")
	}
	info.WriteString("# Replication\r\n")
	info.WriteString("role:" + server.opts.Role + "\r\n")
	info.WriteString("master_replid:" + server.opts.ReplicaId + "\r\n")
	info.WriteString("master_repl_offset:" + strconv.FormatInt(int64(server.mc.propOffset), 10) + "\r\n")
	_, err := server.conn.conn.Write([]byte(EncodeBulkString(info.String())))
	if err != nil {
		return fmt.Errorf("write to connection failed with %v", err)
	}
	return nil
}

func (server *Server) handlePsync(request []string) error {
	emptyrdb, err := hex.DecodeString("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2")
	if err != nil {
		return fmt.Errorf("wrror while decoding the hex string with rdb file content")
	}
	if request[0] == "?" {
		// initially the offset will be zero
		// _, err =server.conn.conn.Write([]byte(EncodeBulkString(string("FULLRESYNC " + server.opts.ReplicaId + " " + strconv.Itoa(server.mc.propOffset)))))
		_, err = server.conn.conn.Write([]byte(string("+FULLRESYNC " + server.opts.ReplicaId + " 0\r\n")))
		if err != nil {
			return fmt.Errorf("write to connection failed with %v", err)
		}
	}
	_, err = server.conn.conn.Write([]byte("$" + strconv.Itoa(len(emptyrdb)) + "\r\n" + string(emptyrdb)))
	// _, err =server.conn.conn.Write([]byte(EncodeBulkString(string(emptyrdb))))
	if err != nil {
		return fmt.Errorf("write to connection failed with %v", err)
	}
	//Add yo slaves
	fmt.Println("Adding to the slaves......")
	server.mc.slaves.AddToSlaves(server.conn.conn.RemoteAddr(), server.conn)
	return nil
}

func (server *Server) handleReplconf(request []string) error {
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
			return fmt.Errorf("error writing to connection %v", err)
		}
	}
	return nil
}

func (server *Server) handleGet(request []string) error {
	value, err := server.storage.GetFromDataBase(request[0])
	var response string
	if err != nil {
		response = "$-1\r\n"
	} else {
		response = EncodeBulkString(*value)
	}
	_, err = server.conn.conn.Write([]byte(response))
	if err != nil {
		return fmt.Errorf("connection write failed with %v ", err)
	}
	return nil
}

// TODO: Check again below 2 functions
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
func (server *Server) handleSet(request []string) error {
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
		return 0, fmt.Errorf("illegal call to getSynchronizedSlavesCount by non master server")
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

func (server *Server) handleWait(request []string) error {
	numReplicas, err := strconv.Atoi(request[0])
	if err != nil {
		return fmt.Errorf("atoi fialed with error %v", err)
	}

	timeout, err := strconv.Atoi(request[1])
	// timeout /= 10

	if err != nil {
		return fmt.Errorf("atoi failed with error %v", err)
	}

	syncCount, err := server.mc.slaves.getSynchronizedSlavesCount(server)
	if err != nil {
		return fmt.Errorf("syncCount failed with error %v", err)
	}

	if syncCount == server.mc.slaves.Count() {
		server.conn.conn.Write([]byte(":" + strconv.Itoa(server.mc.slaves.Count()) + "\r\n"))
		return nil
	}
	server.mc.slaves.lock.RLock()
	for _, slave := range server.mc.slaves.list {
		_, err = slave.conn.Write([]byte("*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n"))
		if err != nil {
			server.mc.slaves.lock.RUnlock()
			return fmt.Errorf("error while writing to connection %v", err)
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
		return fmt.Errorf("write to conn failed with %v", err)
	}
	server.mc.propOffset += 37
	return nil
}

func (server *Server) handleConfig(request []string) error {
	switch strings.ToUpper(request[0]) {
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

func (server *Server) handleKeys(key string) error {
	if key == "*" {
		keys, err := server.storage.getAllKeysFromRDB()
		if err != nil {
			return fmt.Errorf("error getting keys: %v", err)
		}
		outputstring := ""
		for key_i := range len(keys) {
			outputstring += EncodeBulkString(keys[key_i])
		}
		_, err = server.conn.conn.Write([]byte("*" + string(len(keys)) + "\r\n" + outputstring))
		return err
	} else {
		return fmt.Errorf("command %s not supported", key)
	}
}

const (
	dbStartKey         byte = 0xFE
	metadataStartKey   byte = 0xFA
	hashtableSizeKey   byte = 0xFB
	hasExpiryInSecKey  byte = 0xFC
	hasExpiryInMSecKey byte = 0xFD
	EOF                byte = 0xFF
)

// Length encoding is used to store the length of the next object in the stream.
// Length encoding is a variable byte encoding designed to use as few bytes as possible.
// Numbers up to and including 63 can be stored in 1 byte
// Numbers up to and including 16383 can be stored in 2 bytes
// Numbers up to 2^32 -1 can be stored in 4 bytes
func parseLength(length byte, reader *bufio.Reader) (int, error) {
	len := int(length)
	msb := len >> 6
	switch msb {
	// 00	The next 6 bits represent the length
	case 0b00:
		len = int(length & 0b00111111)
		return len, nil
	// 01	Read one additional byte. The combined 14 bits represent the length
	case 0b01:
		len = int(length & 0b00111111)
		nextbyte, err := reader.ReadByte()
		if err != nil {
			return 0, fmt.Errorf("error reading the next byte %v", err)
		}
		totallen := len<<8 | int(nextbyte)
		return int(totallen), nil
	// 10	Discard the remaining 6 bits. The next 4 bytes from the stream represent the length
	case 0b10:
		lenBytes, err := reader.ReadBytes(4)
		if err != nil {
			return 0, fmt.Errorf("error reading the next 4 bytes for length %v", err)
		}
		finallen := int(lenBytes[0])
		for i := 1; i < 4; i++ {
			finallen = finallen<<8 | int(lenBytes[i])
		}
		return int(finallen), nil
	// TODO: Revisit to check difference between below cases and case 0b10
	// 11	The next object is encoded in a special format.
	// The remaining 6 bits indicate the format. May be used to store numbers or Strings, see String Encoding
	case 0b11:
		specialType := length & 0b00111111
		switch specialType {
		// 0 indicates that an 8 bit integer follows
		case 0:
			length, err := reader.ReadByte()
			if err != nil {
				return 0, fmt.Errorf("error reading the length bytes for 4 byte string: %v", err)
			}
			return int(length), nil
		// 1 indicates that a 16 bit integer follows
		case 1:
			length, err := reader.ReadBytes(2)
			if err != nil {
				return 0, fmt.Errorf("error reading the length bytes for 4 byte string: %v", err)
			}
			finallen := int(length[0])
			for i := 1; i < 2; i++ {
				finallen = finallen<<8 | int(length[i])
			}
			return int(finallen), nil
		// 2 indicates that a 32 bit integer follows
		case 2:
			length, err := reader.ReadBytes(4)
			if err != nil {
				return 0, fmt.Errorf("error reading the length bytes for 4 byte string: %v", err)
			}
			finallen := int(length[0])
			for i := 1; i < 4; i++ {
				finallen = finallen<<8 | int(length[i])
			}
			return int(finallen), nil
		default:
			return 0, fmt.Errorf("bad encoding")
		}
	default:
		return 0, fmt.Errorf("bad encoding")
	}
}

func (server *Server) loadRdb() error {
	filepath := server.opts.Dir + server.opts.DbFileName
	fmt.Printf("filepath for rdb file = %s", filepath)
	file, err := os.Open(filepath)
	if err != nil {
		return fmt.Errorf("error while opening the rdb file: %v", err)
	}
	defer file.Close()
	reader := bufio.NewReader(file)
SkipToDB:
	for {
		rbyte, err := reader.ReadByte()
		if err != nil {
			return fmt.Errorf("failed reading the bytes from the file %v", err)
		}
		// ignore lines till you find the database section
		if rbyte == dbStartKey {
			break SkipToDB
		}
	}
	dbIndexByte, err := reader.ReadByte()
	if err != nil {
		return fmt.Errorf("error reading the db index %v", err)
	}
	//parse the index byte
	// dbIndex := int(dbIndexByte)
	dbIndex, err := parseLength(dbIndexByte, reader)
	if err != nil {
		return fmt.Errorf("error parsing the dbIndex %v", err)
	}
	fmt.Printf("Index of the current DB: %d \n", dbIndex)
	// read the Database now
	var expiryVal time.Time
	for {
		rbyte, err := reader.ReadByte()
		if err != nil {
			return fmt.Errorf("failed reading the database section bytes from the file %v", err)
		}
		switch rbyte {
		case hashtableSizeKey:
			hashTableSizeByte, err := reader.ReadByte()
			if err != nil {
				return fmt.Errorf("error reading hash table size %v\n ", err)
			}
			// hashTableSize := int(hashTableSizeByte)
			hashTableSize, err := parseLength(hashTableSizeByte, reader)
			if err != nil {
				return fmt.Errorf("error parsing hashTableSize %v", err)
			}
			fmt.Printf("Hash Table size: = %d\n", hashTableSize)

			expiryTableSizeByte, err := reader.ReadByte()
			if err != nil {
				return fmt.Errorf("error reading expiry table size %v\n ", err)
			}
			// expiryTableSize := int(expiryTableSizeByte)
			expiryTableSize, err := parseLength(expiryTableSizeByte, reader)
			if err != nil {
				return fmt.Errorf("error parsing expiry table size %v", err)
			}
			fmt.Printf("Hash Table size: = %d\n", expiryTableSize)
		case hasExpiryInSecKey:
			var expiryBytes []byte
			_, err := reader.Read(expiryBytes)
			if err != nil {
				return fmt.Errorf("error reading the expiry value%v ", err)
			}
			expiryBytesVal := int64(binary.LittleEndian.Uint32(expiryBytes)) * 1000
			expiryVal = time.Now().Add(time.Duration(expiryBytesVal) * time.Second)
		case hasExpiryInMSecKey:
			var expiryBytes []byte
			_, err := reader.Read(expiryBytes)
			if err != nil {
				return fmt.Errorf("error reading the expiry value%v ", err)
			}
			expiryBytesVal := int64(binary.LittleEndian.Uint32(expiryBytes)) * 1000
			expiryVal = time.Now().Add(time.Duration(expiryBytesVal) * time.Millisecond)
		case metadataStartKey:
			fmt.Println("encountered metadata start")
			return fmt.Errorf("encountered metadata start in middle of the database section")
		case EOF:
			return fmt.Errorf("encountered EOF")
		default:
			// below is assuming the type flag was 00 => string
			keyByte, err := reader.ReadByte()
			if err != nil {
				return fmt.Errorf("error reading key %v\n ", err)
			}
			key := string(keyByte)

			valueByte, err := reader.ReadByte()
			if err != nil {
				return fmt.Errorf("error reading value %v\n ", err)
			}
			value := string(valueByte)
			if !expiryVal.IsZero() && !time.Now().After(expiryVal) {
				//skip adding already expired keys, this is possible as we are reading from the rdb file
				server.storage.AddToDataBase(key, value, expiryVal)
			}
			expiryVal = time.Time{}
		}
	}
	// return nil
}

func (server *Server) handle() {
	defer server.conn.conn.Close()
	//load the rdb file

	if server.opts.Dir != "" && server.opts.DbFileName != "" {
		err := server.loadRdb()
		if err != nil {
			fmt.Printf("Error occurred while reading form RDB file %v", err)
			return
		}
	}
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
				fmt.Printf("%s expects at least 1 argument\n", request[0])
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
				fmt.Printf("%s expects at least 1 argument \n", request[0])
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
		case "KEYS":
			if len(request) != 2 {
				fmt.Printf("%s Command expects an argument\n", request[0])
			}
			err = server.handleKeys(request[1])

		default:
			//handle default
		}
		if err != nil {
			fmt.Println("command", request[0], "failed", err)
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
		if str[len(str)-1] == '\n' {
			str = str[:len(str)-1]
		}

		if str[len(str)-1] == '\r' {
			str = str[:len(str)-1]
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
		return fmt.Errorf("didn't receive \"PONG\": received %s", response)
	}

	_, err = server.conn.conn.Write([]byte("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n6380\r\n"))
	if err != nil {
		fmt.Println("error while connecting to the master %v", err)
		os.Exit(1)
	}

	_, response, err = server.conn.readLine()
	if err != nil {
		return fmt.Errorf("reading from connection failed %v", err)
	}

	if response != "+OK" {
		return fmt.Errorf("didn't receive \"OK\", instead received %s", response)
	}

	fmt.Println("sending second replconf")

	_, err = server.conn.conn.Write([]byte("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"))
	if err != nil {
		fmt.Println("Error while connecting to the master .....")
		os.Exit(1)
	}

	_, response, err = server.conn.readLine()
	if err != nil {
		return fmt.Errorf("reading from connection failed %v", err)
	}

	if response != "+OK" {
		return fmt.Errorf("didn't receive \"OK\", instead received %s", response)
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
		return fmt.Errorf("didn't receive \"FULLRESYNC\", instead received %s", response)
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
