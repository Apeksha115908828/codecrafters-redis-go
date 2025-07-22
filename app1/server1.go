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
func (server *Server) handlePing() (string, error) {
	if server.opts.Role == "master" {
		// _, err := server.conn.conn.Write([]byte("+PONG\r\n"))
		return fmt.Sprintf("+PONG\r\n"), nil
		// if err != nil {
		// 	return fmt.Errorf("HandlePing::Write to connection failed with %v", err)
		// }
		// return nil
	}
	return "", nil
	// return fmt.Errorf("PING received on non master node")
}

func EncodeBulkString(str string) string {
	return fmt.Sprintf("$%d\r\n%s\r\n", len(str), str)
}

func (server *Server) handleEcho(message string) (string, error) {
	// _, err := server.conn.conn.Write([]byte(EncodeBulkString(message)))
	return fmt.Sprintf(EncodeBulkString(message)), nil
	// if err != nil {
	// 	return fmt.Errorf("echo failed to write with %v", err)
	// }
	// return nil
}

func (server *Server) handleInfo(argument string) (string, error) {
	var info strings.Builder
	if argument != "replication" {
		return "", fmt.Errorf("error: incorrect arguments")
	}
	info.WriteString("# Replication\r\n")
	info.WriteString("role:" + server.opts.Role + "\r\n")
	info.WriteString("master_replid:" + server.opts.ReplicaId + "\r\n")
	info.WriteString("master_repl_offset:" + strconv.FormatInt(int64(server.mc.propOffset), 10) + "\r\n")
	// _, err := server.conn.conn.Write([]byte(EncodeBulkString(info.String())))
	return fmt.Sprintf(EncodeBulkString(info.String())), nil
	// if err != nil {
	// 	return fmt.Errorf("write to connection failed with %v", err)
	// }
	// return nil
}

func (server *Server) handlePsync(request []string) (string, error) {
	emptyrdb, err := hex.DecodeString("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2")
	if err != nil {
		return "", fmt.Errorf("wrror while decoding the hex string with rdb file content")
	}
	response := ""
	if request[0] == "?" {
		// initially the offset will be zero
		// _, err =server.conn.conn.Write([]byte(EncodeBulkString(string("FULLRESYNC " + server.opts.ReplicaId + " " + strconv.Itoa(server.mc.propOffset)))))
		// _, err = server.conn.conn.Write([]byte(string("+FULLRESYNC " + server.opts.ReplicaId + " 0\r\n")))
		response = fmt.Sprintf(string("+FULLRESYNC " + server.opts.ReplicaId + " 0\r\n"))
		// if err != nil {
		// 	return "", fmt.Errorf("write to connection failed with %v", err)
		// }
	}
	// _, err = server.conn.conn.Write([]byte("$" + strconv.Itoa(len(emptyrdb)) + "\r\n" + string(emptyrdb)))
	response += fmt.Sprintf("$" + strconv.Itoa(len(emptyrdb)) + "\r\n" + string(emptyrdb))
	// _, err =server.conn.conn.Write([]byte(EncodeBulkString(string(emptyrdb))))
	// if err != nil {
	// 	return fmt.Errorf("write to connection failed with %v", err)
	// }
	//Add yo slaves
	fmt.Println("Adding to the slaves......")
	server.mc.slaves.AddToSlaves(server.conn.conn.RemoteAddr(), server.conn)
	return response, nil
}

func (server *Server) handleReplconf(request []string) (string, error) {
	fmt.Println("got handleReplconf......")
	response := ""
	switch strings.ToUpper(request[0]) {
	case "ACK":
		// For the master
		ack, err := strconv.Atoi(request[1])
		if err != nil {
			return "", fmt.Errorf("error with strconv.Atoi %v", err)
		}
		err = server.mc.slaves.HandleAck(server.conn.conn.RemoteAddr(), ack)
		if err != nil {
			return "", fmt.Errorf("ack handling failed with error %v", err)
		}
		if server.mc.wg != nil {
			fmt.Println("sending done on waitgroup......")
			server.mc.wg.Done()
		}
	case "GETACK":
		// For the slaves
		offset := server.conn.offset
		fmt.Println("Responding to the getAck command......")
		response = fmt.Sprintf("*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$%d\r\n%d\r\n", len(strconv.Itoa(offset)), offset)
		// _, err := server.conn.conn.Write([]byte(response))
		// if err != nil {
		// 	return "", fmt.Errorf("send ACK failed with error %v", err)
		// }
		server.conn.offset += 37
	default:
		response = fmt.Sprintf("+OK\r\n")
		// _, err := server.conn.conn.Write([]byte("+OK\r\n"))
		// if err != nil {
		// 	return fmt.Errorf("error writing to connection %v", err)
		// }
	}
	return response, nil
}

func (server *Server) handleGet(request []string) (string, error) {
	value, err := server.storage.GetFromDataBase(request[0])
	var response string
	if err != nil {
		response = "$-1\r\n"
	} else {
		response = EncodeBulkString(*value)
	}
	// _, err = server.conn.conn.Write([]byte(response))
	// if err != nil {
	// 	return "", fmt.Errorf("connection write failed with %v ", err)
	// }
	return fmt.Sprintf(response), nil
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
func (server *Server) handleSet(request []string) (string, error) {
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
		// _, err := server.conn.conn.Write([]byte("+OK\r\n"))
		// if err != nil {
		// 	return fmt.Errorf("writing to connection failed with error %v", err)
		// }
		return "+OK\r\n", nil
	}
	return "", nil
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

func (server *Server) handleWait(request []string) (string, error) {
	numReplicas, err := strconv.Atoi(request[0])
	if err != nil {
		return "", fmt.Errorf("atoi fialed with error %v", err)
	}

	timeout, err := strconv.Atoi(request[1])
	// timeout /= 10

	if err != nil {
		return "", fmt.Errorf("atoi failed with error %v", err)
	}

	syncCount, err := server.mc.slaves.getSynchronizedSlavesCount(server)
	if err != nil {
		return "", fmt.Errorf("syncCount failed with error %v", err)
	}

	if syncCount == server.mc.slaves.Count() {
		// server.conn.conn.Write([]byte(":" + strconv.Itoa(server.mc.slaves.Count()) + "\r\n"))
		return fmt.Sprintf(":" + strconv.Itoa(server.mc.slaves.Count()) + "\r\n"), nil
	}
	server.mc.slaves.lock.RLock()
	for _, slave := range server.mc.slaves.list {
		_, err = slave.conn.Write([]byte("*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n"))
		if err != nil {
			server.mc.slaves.lock.RUnlock()
			return "", fmt.Errorf("error while writing to connection %v", err)
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
		return "", fmt.Errorf("getSynchronizedSlavesCount failed with %v", err)
	}
	// _, err = server.conn.conn.Write([]byte(":" + strconv.Itoa(syncCount) + "\r\n"))
	// if err != nil {
	// 	return fmt.Sprintf(":" + strconv.Itoa(syncCount) + "\r\n"), fmt.Errorf("write to conn failed with %v", err)
	// }
	server.mc.propOffset += 37
	return fmt.Sprintf(":" + strconv.Itoa(syncCount) + "\r\n"), nil
}

func (server *Server) handleConfig(request []string) (string, error) {
	response := ""
	switch strings.ToUpper(request[0]) {
	case "GET":
		query := request[1]
		if query == "dir" {
			// _, err := server.conn.conn.Write([]byte("*2\r\n" + EncodeBulkString("dir") + EncodeBulkString(server.opts.Dir)))
			// if err != nil {
			// 	return "", fmt.Errorf("config handling failed with %v", err)
			// }
			return fmt.Sprintf("*2\r\n" + EncodeBulkString("dir") + EncodeBulkString(server.opts.Dir)), nil
		} else if query == "dbfilename" {
			// _, err := server.conn.conn.Write([]byte("*2\r\n" + EncodeBulkString("dbfilename") + EncodeBulkString(server.opts.DbFileName)))
			// if err != nil {
			// 	return "", fmt.Errorf("config handling failed with %v", err)
			// }
			return fmt.Sprintf("*2\r\n" + EncodeBulkString("dbfilename") + EncodeBulkString(server.opts.DbFileName)), nil
		}
	}
	return response, nil
}

func (server *Server) handleKeys(key string) (string, error) {
	if key == "*" {
		keys, err := server.storage.getAllKeysFromRDB()
		if err != nil {
			return "", fmt.Errorf("error getting keys: %v", err)
		}
		fmt.Printf("number of keys = %d", len(keys))
		outputstring := ""
		for key_i := range len(keys) {
			outputstring += EncodeBulkString(keys[key_i])
		}
		return fmt.Sprintf(ToRespArray(keys)), err
	} else {
		return "", fmt.Errorf("command %s not supported", key)
	}
}

func (server *Server) handleIncr(key string) (string, error) {
	fmt.Println("HandleIncr called for key = ", key)
	// var value string = ""
	var valueint int = 0
	val, err := server.storage.GetFromDataBase(key)
	if err != nil {
		fmt.Println("Value not present in the database", valueint)
		// server.storage.AddToDataBase(key, string(1), time.Time{})
		// value = "0"
	} else {
		value := *val
		fmt.Printf("calling strconv on val %s\n", value)
		valueint, err = strconv.Atoi(value)
		if err != nil {
			// _, err = server.conn.conn.Write([]byte("-ERR value is not an integer or out of range\r\n"))
			// if err != nil {
			// 	return fmt.Errorf("error writing to conn %v", err)
			// }
			// return fmt.Errorf("error while converting the value to string")
			return "-ERR value is not an integer or out of range\r\n", nil
		}
	}
	fmt.Println("Got value for key = ", valueint)
	// valueint, err := strconv.Atoi(value)

	valueint += 1
	server.storage.AddToDataBase(key, strconv.Itoa(valueint), time.Time{})
	// _, err = server.conn.conn.Write([]byte(":" + strconv.Itoa(valueint) + "\r\n"))
	// if err != nil {
	// 	return "", fmt.Errorf("error writing to conn %v", err)
	// }

	return fmt.Sprintf(":" + strconv.Itoa(valueint) + "\r\n"), nil
}

func (server *Server) handleType(key string) (string, error) {
	if server.storage.findKeyInStream(key) {
		return "+stream\r\n", nil
	}
	value, err := server.storage.GetFromDataBase(key)
	if err != nil {
		return "+none\r\n", nil
	}
	fmt.Printf("got value = %s from database", *value)
	return "+string\r\n", nil
}

func (server *Server) handleRPush(request []string) (string, error) {
	// for i, req := range request {
	// 	print("rpush request[%d] = %s", i, req)
	// }
	listsize, err := server.storage.rpush(request[0], request[1:])
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(":" + strconv.Itoa(listsize) + "\r\n"), nil
}
func (server *Server) handleLPush(request []string) (string, error) {
	listsize, err := server.storage.lpush(request[0], request[1:])
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(":" + strconv.Itoa(listsize) + "\r\n"), nil
}
func (server *Server) handleLRange(request []string) (string, error) {
	list_elements, err := server.storage.lrange(request[0], request[1], request[2])
	if err != nil {
		fmt.Println("..............error in lrange = ", err)
		return "*0\r\n", nil
		// return ToRespArray(list_elements), err
	}
	return ToRespArray(list_elements), nil
}
func (server *Server) handleLLen(key string) (string, error) {
	llen, err := server.storage.llen(key)
	if err != nil {
		fmt.Println("........error in llen = ", err)
		return ":0\r\n", nil
	}
	return fmt.Sprintf(":" + strconv.Itoa(llen) + "\r\n"), nil
}
func (server *Server) handlelpop(key string) (string, error) {
	// return encoded bulkstring
	pop_element, err := server.storage.lpop(key)
	if err != nil {
		return "$-1\r\n", nil
	}
	return ToBulkString(pop_element), nil
}

func (server *Server) handleMultiPop(key string, count string) (string, error) {
	pop_element, err := server.storage.multilpop(key, count)
	if err != nil {
		return "*0\r\n", nil
	}
	return ToRespArray(pop_element), nil
}

func (server *Server) handleXADD(request []string) (string, error) {
	fmt.Printf("len(request) = %d", len(request))

	key := request[0]
	id := request[1]
	if strings.Contains(id, "*") {
		id = server.storage.autoGenerateID(key, id)
		fmt.Printf("generated id = %s", id)
	}
	err, isValid := server.storage.checkIDValidity(key, id)
	if isValid {
		kvpairs := make(map[string]string, 0)
		for i := 2; i < len(request); i++ {
			key := request[i]
			value := request[i+1]
			i += 1
			kvpairs[key] = value
		}
		id = server.storage.AddToStream(key, id, kvpairs)
		if server.mc.wg != nil {
			server.mc.wg.Done()
		}
		return fmt.Sprintf("$" + strconv.Itoa(len(id)) + "\r\n" + id + "\r\n"), nil
	} else {
		return fmt.Sprintf("-ERR The ID specified in XADD " + err + "\r\n"), nil
	}
}

func (server *Server) handleXRANGE(request []string) (string, error) {
	key := request[0]
	lower := request[1]
	upper := request[2]
	if lower == "-" {
		lower = "0-0"
	} else if upper == "+" {
		currentTime := time.Now()
		unixTimestampMillis := currentTime.UnixNano() / int64(time.Millisecond)
		timestampStr := strconv.FormatInt(unixTimestampMillis, 10)
		fmt.Printf("For * Generated id = %s", timestampStr)
		upper = timestampStr + "-0"
	}

	streams := server.storage.getAllKVsInRangeStream(key, lower, upper)
	fmt.Printf("Length of streams = %d\n", len(streams))
	length := strconv.Itoa(len(streams))
	responses := []string{}
	// response := fmt.Sprintf("*" + length + "\r\n")
	responses = append(responses, "*"+length+"\r\n")
	fmt.Printf("response, length = %d \n", len(responses))
	for i := 0; i < len(streams); i++ {
		stream := streams[i]
		// response += "*2\r\n"
		responses = append(responses, "*2\r\n")
		// response += fmt.Sprintf("$" + strconv.Itoa(len(stream.id)) + "\r\n" + stream.id + "\r\n")
		responses = append(responses, "$"+strconv.Itoa(len(stream.id))+"\r\n"+stream.id+"\r\n")
		num_kvpairs := strconv.Itoa(2 * len(stream.kvpairs))
		// response += "*" + strconv.Itoa(len(num_kvpairs)) + "\r\n" + num_kvpairs + "\r\n"
		responses = append(responses, "*"+num_kvpairs+"\r\n")
		for key, value := range stream.kvpairs {
			// fmt.Printf("For id = %s, key = %s, value = %s", stream.id, key, value)
			// response += fmt.Sprintf("$" + strconv.Itoa(len(key)) + "\r\n" + key + "\r\n")
			responses = append(responses, "$"+strconv.Itoa(len(key))+"\r\n"+key+"\r\n")
			// response += fmt.Sprintf("$" + strconv.Itoa(len(value)) + "\r\n" + value + "\r\n")
			responses = append(responses, "$"+strconv.Itoa(len(value))+"\r\n"+value+"\r\n")
		}
		fmt.Printf("response = %s \n", responses[len(responses)-1])
	}

	response := ""
	for i := 0; i < len(responses); i++ {
		response += responses[i]
	}
	fmt.Printf("response = %s \n", response)
	return response, nil
}

func (server *Server) handleXREAD(request []string) (string, error) {
	prev_entries := 0
	if request[0] == "block" {
		blockTime, _ := strconv.Atoi(request[1])
		fmt.Printf("blockTime = %d\n", blockTime)
		prev_entries = len(server.storage.stream[request[3]])
		if blockTime == 0 {
			server.mc.wg = &sync.WaitGroup{}
			server.mc.wg.Add(1)

			ch := make(chan struct{})
			go func() {
				defer close(ch)
				server.mc.wg.Wait()
			}()

			select {
			case <-ch:
			}
		} else {
			time.Sleep(time.Duration(blockTime) * time.Millisecond)
		}
		request = request[2:]
	}
	fmt.Println("got past the block")
	if request[0] == "streams" {
		shouldReturnEmpty := true
		request = request[1:]
		num_streams := (len(request)) / 2
		responses := []string{}
		responses = append(responses, "*"+strconv.Itoa(num_streams)+"\r\n")
		keys := []string{}
		ids := []string{}
		for j := 0; j < num_streams; j++ {
			keys = append(keys, request[j])
		}
		for j := 0; j < num_streams; j++ {
			ids = append(ids, request[j+num_streams])
		}
		fmt.Println("processing num_streams = ", num_streams)
		for j := 0; j < num_streams; j++ {
			key := keys[j]
			id := ids[j]
			fmt.Printf("prev_entries = %d\n", prev_entries)
			if id == "$" {
				new_entries := len(server.storage.stream[key])
				if prev_entries == new_entries {
					return "$-1\r\n", nil
				}
				id = server.storage.stream[key][prev_entries-1-(prev_entries-new_entries)-1].id
			}
			// if strings.Contains(id, "*") {
			// 	id = server.storage.autoGenerateID(key, id)
			// 	fmt.Printf("generated id = %s", id)
			// }
			// err, isValid := server.storage.checkIDValidity(key, id)
			// if !isValid {
			// 	return "", fmt.Errorf("error in id %s", err)
			// }
			fmt.Printf("key = %s and id = %s\n", key, id)
			minor_ver, err := strconv.Atoi(strings.Split(id, "-")[1])
			if err != nil {
				return "", fmt.Errorf("error retrieving version %v \n ", err)
			}
			lower := strings.Split(id, "-")[0] + "-" + strconv.Itoa(minor_ver+1)

			// as we need all entries with ids > id,
			currentTime := time.Now()
			unixTimestampMillis := currentTime.UnixNano() / int64(time.Millisecond)
			timestampStr := strconv.FormatInt(unixTimestampMillis, 10)
			upper := timestampStr + "-0"

			streams := server.storage.getAllKVsInRangeStream(key, lower, upper)
			fmt.Printf("Length of streams = %d\n", len(streams))
			length := strconv.Itoa(len(streams))

			responses = append(responses, "*2\r\n")
			responses = append(responses, "$"+strconv.Itoa(len(key))+"\r\n"+key+"\r\n")
			responses = append(responses, "*"+length+"\r\n")
			fmt.Printf("response = %d \n", len(responses))
			for i := 0; i < len(streams); i++ {
				shouldReturnEmpty = false
				stream := streams[i]
				responses = append(responses, "*2\r\n")
				responses = append(responses, "$"+strconv.Itoa(len(stream.id))+"\r\n"+stream.id+"\r\n")
				num_kvpairs := strconv.Itoa(2 * len(stream.kvpairs))
				responses = append(responses, "*"+num_kvpairs+"\r\n")
				for key, value := range stream.kvpairs {
					responses = append(responses, "$"+strconv.Itoa(len(key))+"\r\n"+key+"\r\n")
					responses = append(responses, "$"+strconv.Itoa(len(value))+"\r\n"+value+"\r\n")
				}
				fmt.Printf("response = %s \n", responses[len(responses)-1])
			}
		}
		if shouldReturnEmpty {
			return "$-1\r\n", nil
		}
		response := ""
		for i := 0; i < len(responses); i++ {
			response += responses[i]
		}

		return response, nil
	} else {
		return "", fmt.Errorf("ill formed request")
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
		fmt.Println("reading one more byte")
		if err != nil {
			return 0, fmt.Errorf("error reading the next byte %v", err)
		}
		totallen := len<<8 | int(nextbyte)
		return int(totallen), nil
	// 10	Discard the remaining 6 bits. The next 4 bytes from the stream represent the length
	case 0b10:
		lenBytes, err := reader.ReadBytes(4)
		fmt.Println("reading 4 more bytes")
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
			fmt.Println("reading one more byte")
			if err != nil {
				return 0, fmt.Errorf("error reading the length bytes for 4 byte string: %v", err)
			}
			return int(length), nil
		// 1 indicates that a 16 bit integer follows
		case 1:
			length, err := reader.ReadBytes(2)
			fmt.Println("reading 2 more bytes")
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
			fmt.Println("reading 4 more bytes")
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
		return 0, fmt.Errorf("invalid length encoding")
	}
}

func (server *Server) handleMulti() error {

	_, err := server.conn.conn.Write([]byte("+OK\r\n"))
	return err
}

func (server *Server) handleExec(offset int) error {
	// currQueue := server.queue[length-1]
	responses := []string{}
	fmt.Printf("num of command to execute = %d", len(server.queue))
	for index := range server.queue {
		entry := server.queue[index]
		response, offset, err := server.handleRequest(entry, offset)
		if err != nil {
			fmt.Println("error occurred with this command", entry[0])
			// if response != "" {
			// 	responses = append(responses, response)
			// }
			continue
		}

		responses = append(responses, response)
		fmt.Printf("got offset = %d and request = %s response = %s \n", offset, entry[0], response)
	}
	server.queue = make([][]string, 0)
	fmt.Printf("num of responses = %d", len(responses))
	resp := fmt.Sprintf("*%d\r\n", len(responses))
	for _, s := range responses {
		// resp += responses[index]
		resp += s
	}
	_, err := server.conn.conn.Write([]byte(resp))
	if err != nil {
		return fmt.Errorf("error writing to the conn")
	}
	return nil
}

func (server *Server) handleDiscard() error {
	return nil
}

func (server *Server) loadRdb() error {
	filepath := server.opts.Dir + "/" + server.opts.DbFileName
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
		fmt.Println("Before reading next byte expiryVal = ", expiryVal)
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
			fmt.Printf("Expiry Hash Table size: = %d\n", expiryTableSize)
		case hasExpiryInSecKey:
			expiryBytes := make([]byte, 4)
			// expiryBytes, err := reader.ReadBytes(4)
			_, err := reader.Read(expiryBytes)
			if err != nil {
				return fmt.Errorf("error reading the expiry value in sec%v ", err)
			}
			expiryBytesVal := int64(binary.LittleEndian.Uint32(expiryBytes))
			// expiryVal = time.Now().Add(time.Duration(expiryBytesVal) * time.Second)
			expiryVal = time.Unix(expiryBytesVal, 0)
			fmt.Println("Read the expiry val as ", expiryVal)
		case hasExpiryInMSecKey:
			expiryBytes := make([]byte, 8)
			_, err := reader.Read(expiryBytes)
			// expiryBytes, err := reader.ReadBytes(8)
			if err != nil {
				return fmt.Errorf("error reading the expiry value in msec %v ", err)
			}
			expiryBytesVal := int64(binary.LittleEndian.Uint64(expiryBytes))
			// expiryVal = time.Now().Add(time.Duration(expiryBytesVal) * time.Millisecond)
			expiryVal = time.Unix(expiryBytesVal/1000, 0)
			fmt.Println("Read the expiry val as ms converted to time", expiryVal)
		case metadataStartKey:
			fmt.Println("encountered metadata start")
			continue
			// return fmt.Errorf("encountered metadata start in middle of the database section")
		case EOF:
			fmt.Println("encountered EOF")
			return nil
		default:
			// below is assuming the type flag was 00 => string
			// _, err := reader.ReadByte()
			// keyByte, err = reader.ReadByte()
			keyByte, err := reader.ReadByte()
			if err != nil {
				return fmt.Errorf("error reading key %v\n ", err)
			}
			key, err := server.parseString(keyByte, reader)
			if err != nil {
				return fmt.Errorf("error reading key %v\n ", err)
			}
			fmt.Println("reading the key", key)

			valueByte, err := reader.ReadByte()
			if err != nil {
				return fmt.Errorf("error reading value %v\n ", err)
			}
			value, err := server.parseString(valueByte, reader)
			if err != nil {
				return fmt.Errorf("error parsing value %v\n ", err)
			}

			fmt.Println("reading the value", value)
			if expiryVal.IsZero() || !time.Now().After(expiryVal) {
				//skip adding already expired keys, this is possible as we are reading from the rdb file
				fmt.Print("Adding to DB....\n", expiryVal)
				fmt.Print(" expiryVal.IsZero()", expiryVal.IsZero())
				server.storage.AddToDataBase(key, value, expiryVal)
			}
			// expiryVal = time.Time{}
		}
	}
	// return nil
}
func (server *Server) parseString(b byte, reader *bufio.Reader) (string, error) {
	length, err := parseLength(b, reader)
	if err != nil {
		return "", fmt.Errorf("parseLength failed: %v", err)
	}
	fmt.Printf("String length: %d\n", length)

	if length < 0 {
		return "", fmt.Errorf("invalid string length: %d", length)
	}

	// Handle empty string case
	if length == 0 {
		return "", nil
	}

	str := make([]byte, length)
	n, err := reader.Read(str)
	if err != nil {
		return "", fmt.Errorf("Read failed: %v", err)
	}
	if n != length {
		return "", fmt.Errorf("read string length mismatch: expected %d, got %d", length, n)
	}
	fmt.Printf("Parsed string: %s\n", string(str[:n]))
	return string(str[:n]), nil
}

func (server *Server) handleRequest(request []string, offset int) (string, int, error) {
	var err error
	response := ""
	switch strings.ToUpper(request[0]) {
	case "PING":
		response, err = server.handlePing()
		offset = 14
	case "REPLCONF":
		fmt.Println("got REPLCONF......")
		if len(request) < 2 {
			fmt.Println("Ill formed command REPLCONF")
			break
		}
		response, err = server.handleReplconf(request[1:])
		//handle replconf
	case "PSYNC":
		if len(request) != 3 {
			fmt.Println("Ill formed command PSYNC")
			break
		}
		response, err = server.handlePsync(request[1:])
		//handle psync
	case "ECHO":
		if len(request) != 2 {
			fmt.Printf("%s expects at least 1 argument\n", request[0])
			break
		}
		response, err = server.handleEcho(request[1])
	case "SET":
		if len(request) < 2 {
			fmt.Println("Ill formed command SET")
			break
		}
		response, err = server.handleSet(request[1:])
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
			break
		}
		response, err = server.handleGet(request[1:])
	case "INFO":
		if len(request) != 2 {
			fmt.Printf("%s expects at least 1 argument \n", request[0])
			break
		}
		response, err = server.handleInfo(request[1])
	case "WAIT":
		//handle wait
		waitLock.Lock()
		response, err = server.handleWait(request[1:])
		waitLock.Unlock()
	case "CONFIG":
		if len(request) != 3 {
			fmt.Println("Ill formed command", request[0])
			break
		}
		response, err = server.handleConfig(request[1:])
	case "KEYS":
		if len(request) != 2 {
			fmt.Printf("%s Command expects an argument\n", request[0])
			break
		}
		response, err = server.handleKeys(request[1])
	case "INCR":
		if len(request) != 2 {
			fmt.Printf("%s Command expects an argument\n", request[0])
			break
		}
		response, err = server.handleIncr(request[1])
	case "TYPE":
		if len(request) != 2 {
			fmt.Printf("%s Command expects an argument\n", request[0])
			break
		}
		response, err = server.handleType(request[1])

	case "RPUSH":
		if len(request) < 3 {
			fmt.Printf("%s Command expects an argument\n", request[0])
			break
		}
		response, err = server.handleRPush(request[1:])
	case "LPUSH":
		if len(request) < 3 {
			fmt.Printf("%s Command expects an argument\n", request[0])
			break
		}
		response, err = server.handleLPush(request[1:])
	case "LRANGE":
		if len(request) != 4 {
			fmt.Printf("%s Command expects an argument\n", request[0])
			break
		}
		response, err = server.handleLRange(request[1:])
	case "LLEN":
		if len(request) != 2 {
			fmt.Printf("%s Command expects an argument\n", request[0])
			break
		}
		response, err = server.handleLLen(request[1])
	case "LPOP":
		if len(request) < 2 || len(request) > 3 {
			fmt.Printf("%s Incorrect number of arguments\n", request[0])
			break
		}
		if len(request) == 2 {
			response, err = server.handlelpop(request[1])
		} else {
			response, err = server.handleMultiPop(request[1], request[2])
		}
	case "XADD":
		if len(request) < 4 {
			fmt.Printf("%s Command expects an argument\n", request[0])
			break
		}
		response, err = server.handleXADD(request[1:])
	case "XRANGE":
		if len(request) < 4 {
			fmt.Printf("%s Command expects an argument\n", request[0])
			break
		}
		response, err = server.handleXRANGE(request[1:])
	case "XREAD":
		if len(request) < 4 {
			fmt.Printf("%s Command expects an argument\n", request[0])
			break
		}
		response, err = server.handleXREAD(request[1:])
	default:
		//handle default
	}
	return response, offset, err
}

func (server *Server) handle() {
	defer server.conn.conn.Close()
	//load the rdb file

	if server.opts.Dir != "" && server.opts.DbFileName != "" {
		err := server.loadRdb()
		if err != nil {
			fmt.Printf("Error occurred while reading form RDB file %v", err)
			// return
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
		case "MULTI":
			server.isqueuing = true
			err = server.handleMulti()
		case "EXEC":
			if !server.isqueuing {
				server.conn.conn.Write([]byte("-ERR EXEC without MULTI\r\n"))
				break
			}
			if len(server.queue) == 0 {
				server.isqueuing = false
				_, err = server.conn.conn.Write([]byte("*0\r\n"))
				if err != nil {
					fmt.Printf("error writing to conn %v \n", err)
				}
				break
			}
			server.isqueuing = false
			err = server.handleExec(offset)
		case "DISCARD":
			if server.isqueuing {
				_, err := server.conn.conn.Write([]byte("+OK\r\n"))
				if err != nil {
					fmt.Printf("error writing to the conn %v \n", err)
				}
				server.queue = make([][]string, 0)
			} else {
				_, err := server.conn.conn.Write([]byte("-ERR DISCARD without MULTI\r\n"))
				if err != nil {
					fmt.Printf("error writing to the conn %v \n", err)
				}
			}
			server.isqueuing = false
			err = server.handleDiscard()
		default:
			if server.isqueuing {
				server.queue = append(server.queue, request)
				server.conn.conn.Write([]byte("+QUEUED\r\n"))
			} else {
				response, _, err := server.handleRequest(request, offset)
				if err != nil {
					return
				}
				_, err = server.conn.conn.Write([]byte(response))
				if err != nil {
					fmt.Printf("error writing to the conn %v \n", err)
					return
				}
				fmt.Printf("offset = %d request = %s\n", offset, request[0])
			}
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
		fmt.Printf("Error while connecting to the master %v\n", err)
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
		fmt.Printf("error while connecting to the master %v\n", err)
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
		fmt.Printf("Error while connecting to the master %v\n", err)
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
		return fmt.Errorf("expected $, got %c", response[0])
	}

	rdbFileLen, err := strconv.Atoi(response[1:])
	if err != nil {
		return fmt.Errorf("atoi failed with %v", err)
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
