package main

import (
	"fmt"
	// Uncomment this block to pass the first stage
	"net"
	"os"
	"io"
	"errors"
	// "log"
	// "bytes"
	// "bufio"
	"strconv"
	"strings" 
	"time"
	"encoding/hex"
)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	// Uncomment this block to pass the first stage
	info := make(map[string]string)
	port := "6379"
	if len(os.Args) > 2 {
		port = os.Args[2]
	}
	info["port"] = port
	info["role"] = "master"
	info["master_host"] = "0.0.0.0"
	info["master_port"] = port
	info["master_replid"] = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
	info["master_repl_offset"] = "0"
	replicas := map[int]net.Conn{}
	store := NewStore()
	if len(os.Args) > 4 {
		go handleReplica(store, info)
	}
	l, err := net.Listen("tcp", "0.0.0.0:" + port)
	if err != nil {
		fmt.Println("Failed to bind to port", port)
		os.Exit(1)
	}
	for {
		conn, err := l.Accept()
		fmt.Println("Received a connection request.......")
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go handleConn(store, conn, info, replicas);
	}
	// }
}

func handleReplica(store *Storage, info map[string]string) {
	info["role"] = "slave"
	// fmt.Println("strings.Split(os.Args[4] = ", strings.Split(os.Args[4], " "))
	info["master_host"] = strings.Split(os.Args[4], " ")[0]
	info["master_port"] = strings.Split(os.Args[4], " ")[1]
	fmt.Println("sending handshake message........")
	conn := sendHandshake(info)
	
	// replicas := map[int]net.Conn{}
	// handleConn(store, conn, info, replicas);
	for {
		buffer := make([]byte, 1024)
		n, err := conn.Read(buffer)
		if err != nil {
			conn.Write([]byte(err.Error()))
		}
		command := strings.Split(string(buffer[:n]), "\r\n")
		
		for i := 2; i < len(command); i++ {
			if len(command) > i && command[i] == "SET" {
				// rdb[command[i+2]] = command[i+4]
				fmt.Println("processing SET on replica..........")
				key := command[i+2]
				value := command[i+4]
				expiry := 100000000
				r := 4
				if len(command) > i+8 {
					if strings.ToUpper(command[i+6]) == "PX" {
						expiry, _ = strconv.Atoi(command[i+8])
					} else {  // case with EX 
						expiry, _ = strconv.Atoi(command[i+8])
						// expiry = expiry * 1000
					}
					r = 8
				}
				i = i + r + 2
				AddToDataBase(store, key, value, expiry)
			} else if len(command) > i && command[i] == "GET" {
				// value := rdb[command[i+2]]
				key := command[i+2]
				i = i + 4
				val, found := GetFromDataBase(store, key);
				if found {
					val = SimpleString(val).Encode()
				}
				response := "+" + val + "\r\n"
				conn.Write([]byte(response))
			} else if len(command) > i && command[i] == "REPLCONF" {
				response := "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$1\r\n0\r\n"
				conn.Write([]byte(response))
				i = i + 9
			} else {
				fmt.Print("in else block command =", command)
			}
		}
	}
}

func sendHandshake(info map[string]string) (net.Conn){
	replica, err := net.Dial("tcp", info["master_host"] + ":" + info["master_port"])
	if err != nil {
		fmt.Println("Error while connecting to the master .....")
		os.Exit(1)
	}
	fmt.Println("sending ping")
	_, err = replica.Write([]byte("*1\r\n$4\r\nPING\r\n"))
	if err != nil {
		fmt.Println("Error while connecting to the master .....")
		os.Exit(1)
	}
	time.Sleep(1 * time.Second)
	fmt.Println("sending Replconf 1")
	_, err = replica.Write([]byte("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n6380\r\n"))
	if err != nil {
		fmt.Println("Error while connecting to the master .....")
		os.Exit(1)
	}
	time.Sleep(1 * time.Second)
	fmt.Println("sending replconf 2")
	_, err = replica.Write([]byte("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"))
	if err != nil {
		fmt.Println("Error while connecting to the master .....")
		os.Exit(1)
	}
	time.Sleep(1 * time.Second)
	fmt.Println("sending psync")
	_, err = replica.Write([]byte("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"))
	if err != nil {
		fmt.Println("Error while connecting to the master .....")
		os.Exit(1)
	}
	// fmt.Println("Done with the handshake.....replica = ", replica)
	return replica
}

func (d SimpleString) Encode() string {
	return fmt.Sprintf("+%s\r\n", d)
}

func handlePing(conn net.Conn) {
	_, err := conn.Write([]byte("+PONG\r\n"))
	if err != nil {
		fmt.Println(err, "Write response")
		// return err
		os.Exit(1)
	}
}

func handleGet(store *Storage, conn net.Conn, args Array) {
	key := args[0].(BulkString).Value
	val, found := GetFromDataBase(store, key);
	fmt.Println("Value in get %s", val)
	value := "$-1\r\n"
	if found {
		value = SimpleString(val).Encode()
	}
	// value := found ? SimpleString(val).Encode() : SimpleString("").Encode()
	fmt.Println("Value in get %s", value)
	_, err := conn.Write([]byte(value))
	if err != nil {
		fmt.Println("Error while writing", err)
		os.Exit(1)
	}
}

func handleSet(store *Storage, conn net.Conn, args Array) {
	key := args[0].(BulkString).Value
	value := args[1].(BulkString).Value
	expiry := 100000000
	if len(args) > 3 {
		if strings.ToUpper(args[2].(BulkString).Value) == "PX" {
			expiry, _ = strconv.Atoi(args[3].(BulkString).Value)
		} else {  // case with EX 
			expiry, _ = strconv.Atoi(args[3].(BulkString).Value)
			// expiry = expiry * 1000
		}
		
	}
	AddToDataBase(store, key, value, expiry)
	_, err := conn.Write([]byte(SimpleString("OK").Encode()))
	if err != nil {
		fmt.Println(err, "Write Response")
		os.Exit(1)
	}
}

func handleEcho(conn net.Conn, args Array) {
	arr, ok := args[0].(BulkString)
	if !ok {
		fmt.Println("args[0] should be a BulkString")
		os.Exit(1)
	}
	_, err := conn.Write([]byte(SimpleString(arr.Value).Encode()))
	if err != nil {
		fmt.Println(err, "Write Response")
		os.Exit(1)
	}
}

func handleConn(store *Storage, conn net.Conn, info map[string]string, replicas map[int]net.Conn) {
	defer conn.Close()
	for {
		buffer := make([]byte, 1024)
		n, err := conn.Read(buffer)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			fmt.Println("Error reading from connection:", err.Error())
			// return err
			os.Exit(1)
		}
		parsedData, _, err := Parse(buffer)
		if err != nil {
			fmt.Println("Unable to parse the data", string(buffer))
			os.Exit(1)
		}
		// if len(data) != 1 {
		// 	fmt.Println("not all data are processed, data left: %b", data)
		// 	os.Exit(1)
		// }
		command := BulkString {
			Value:  "",
			IsNull: false,
		}
		args := Array{}
		arr, ok := parsedData.(Array)
		if ok {
			// fmt.Println("data should be an array", parsedData)
			// os.Exit(1)
			command, ok = arr[0].(BulkString)
			if !ok {
				fmt.Println("Command should be a string")
				os.Exit(1)
			}
			
			if len(arr) > 1 {
				args = arr[1:]
			}
		} else {
			val, ok := parsedData.(string)
			if !ok {
				val1, ok := parsedData.(BulkString)
				if !ok {
					fmt.Println("Something is wrong with the parsed data")
					os.Exit(1)
				}
				command = val1
			} else {
				command.Value = val
			}
		}
		
		
		fmt.Println("Processing command = ", command.Value)
		switch strings.ToUpper(command.Value) {
		case "PING":
			fmt.Println("Received Ping from a replica", conn)

			handlePing(conn)
			break
		case "REPLCONF":
			conn.Write([]byte(SimpleString("OK").Encode()))
			break
		case "PSYNC":
			// fmt.Println(SimpleString("FULLRESYNC" + info["master_replid"] + info["master_repl_offset"]).Encode())
			// *replicas = append(*replicas, conn)
			replicas[len(replicas)] = conn
			fmt.Println("sending RDB file to complete synchronization.....",)
			conn.Write([]byte(SimpleString("FULLRESYNC " + info["master_replid"] + " " + info["master_repl_offset"]).Encode()))
			// res := fmt.Sprintf("+FULLRESYNC %s %d\r\n", info["master_replid"], info["master_repl_offset"])
			// conn.Write([]byte(res))
			emptyrdb, err := hex.DecodeString("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2")
			if err != nil {
				fmt.Println("Error while decoding the hex string with rdb file content")
				os.Exit(1)
			}
			time.Sleep(1 * time.Second)
			// res = fmt.Sprintf("$%d\r\n%s\r\n", len(emptyrdb), emptyrdb)
			// conn.Write([]byte(res))
			conn.Write([]byte("$" + strconv.Itoa(len(emptyrdb)) + "\r\n" + string(emptyrdb)))
			// time.Sleep(1 * time.Second)
			conn.Write([]byte("*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n"))
			break
		case "ECHO":
			handleEcho(conn, args)
			break
		case "SET":
			handleSet(store, conn, args)
			fmt.Println("Set called on current server", string(buffer[:n]))
			if info["role"] == "master" {
				// fmt.Println("Set called on current server len(replicas)", len(replicas))
				for _, replica := range replicas {
					// fmt.Println("calling set on server *rep = ", replica)
					_, err := replica.Write([]byte(string(buffer[:n])))
					if err != nil {
						fmt.Print(err)
					}
					time.Sleep(time.Millisecond * 20)
				}
			}
			
			break
		case "GET":
			handleGet(store, conn, args)
			if info["role"] == "master" {
					for _, replica := range replicas {
					_, err := replica.Write([]byte(string(buffer[:n])))
					if err != nil {
						fmt.Print(err)
					}
					time.Sleep(time.Millisecond * 20)
				}
			}
			break
		case "INFO":

			// info := SimpleString("role:"+role).Encode()
			// info_string := ""
			var info_string strings.Builder
			// info_string.WriteString("")
			// fmt.Println("len(info) = ", len(info))
			for key, value := range info {
				info_string.WriteString(key + ":" + value)
				// info_string = info_string+SimpleString(key + ":" + value).Encode()
				// fmt.Println("info_string = ", info_string.String())
			}
			info_string_value := SimpleString(info_string.String()).Encode()
			_, err := conn.Write([]byte(info_string_value))
			if err != nil {
				fmt.Println(err, "Write response")
				// return err
				os.Exit(1)
			}
		case "default":
			conn.Write([]byte("-Err Unknown Command\r\n"))
		}
		// if command.Value == "PING" {
		// 	fmt.Println("came here.........")
		// 	// fmt.Println("data: ", string(data))
		// 	handlePing(conn)
		// } else if command.Value == "ECHO" {
		// 	handleEcho(conn, args)
		// } else {
		// 	conn.Write([]byte("-Err Unknown Command\r\n"))
		// }
	}
}