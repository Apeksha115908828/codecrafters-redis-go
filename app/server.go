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
	info["role"] = "master"
	// master_host := "0.0.0.0"
	// master_port := port
	info["master_replid"] = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
	info["master_repl_offset"] = "0"
	if len(os.Args) > 4 {
		info["role"] = "slave"
		// master, _, err := ParseString(os.Args[4])
		// fmt.Println(master)
		// master_host, _, err = ParseString(os.Args[4][0])
		// if err != nil {
		// 	fmt.Println("Error while parsing the master data")
		// 	os.Exit(1)
		// }
		// master_port, _, err = ParseString(os.Args[4][1])
		// if err != nil {
		// 	fmt.Println("Error while parsing the master data")
		// 	os.Exit(1)
		// }
		// master_host = os.Args[4][0]
		// master_port = os.Args[4][1]
	}
	l, err := net.Listen("tcp", "0.0.0.0:" + port)
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	store := NewStore()
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go handleConn(store, conn, info);
	}
	// return nil
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
	expiry := 1000
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

func handleConn(store *Storage, conn net.Conn, info map[string]string) {
	defer conn.Close()
	for {
		buffer := make([]byte, 1024)
		_, err := conn.Read(buffer)
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
			fmt.Println("Unable to parse the data")
			os.Exit(1)
		}
		// if len(data) != 1 {
		// 	fmt.Println("not all data are processed, data left: %b", data)
		// 	os.Exit(1)
		// }
		arr, ok := parsedData.(Array)
		if !ok {
			fmt.Println("data should be an array")
			os.Exit(1)
		}
		command, ok := arr[0].(BulkString)
		if !ok {
			fmt.Println("Command should be a string")
			os.Exit(1)
		}
		args := Array{}
		if len(arr) > 1 {
			args = arr[1:]
		}
		switch strings.ToUpper(command.Value) {
		case "PING":
			handlePing(conn)
			break
		case "ECHO":
			handleEcho(conn, args)
			break
		case "SET":
			handleSet(store, conn, args)
			break
		case "GET":
			handleGet(store, conn, args)
			break
		case "INFO":

			// info := SimpleString("role:"+role).Encode()
			// info_string := ""
			var info_string strings.Builder
			// info_string.WriteString("")
			fmt.Println("len(info) = ", len(info))
			for key, value := range info {
				info_string.WriteString(SimpleString(key + ":" + value).Encode())
				// info_string = info_string+SimpleString(key + ":" + value).Encode()
				fmt.Println("info_string = ", info_string.String())
			}
			_, err := conn.Write([]byte(info_string.String()))
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