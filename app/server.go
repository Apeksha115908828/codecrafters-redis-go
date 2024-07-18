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
	// "strconv"
	// "strings" 
)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	// Uncomment this block to pass the first stage
	//
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go handleConn(conn);
		

	}
	// return nil
}
func (d SimpleString) Encode() string {
	return fmt.Sprintf("+%s\r\n", d)
}
func handleConn(conn net.Conn) {
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
		// command := string(buffer)
		// if string(buffer[0]) == "*" {
		// 	array, _, err := ParseArray(buffer)
		// 	if err != nil {
		// 		os.Exit(1)
		// 	}
		// 	_, err = conn.Write([]byte("+" + array[1] + "\r\n"))
		// 	// return array[1]
		// }
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
		if command.Value == "PING" {
			fmt.Println("came here.........")
			// fmt.Println("data: ", string(data))
			_, err = conn.Write([]byte("+PONG\r\n"))
			if err != nil {
				fmt.Println(err, "Write response")
				// return err
				os.Exit(1)
			}
		} else if command.Value == "ECHO" {
			// if len(command) < 2 {
			// 	os.Exit(1)
			// }
			// _, err = conn.Write([]byte("+PONG\r\n"))
			// fmt.Println("data: ", string(data))
			arr, ok := args[0].(BulkString)
			if !ok {
				fmt.Println("args[0] should be a BulkString")
				os.Exit(1)
			}
			_, err = conn.Write([]byte(SimpleString(arr.Value).Encode()))
			if err != nil {
				fmt.Println(err, "Write Response")
				os.Exit(1)
			}
		} else {
			conn.Write([]byte("-Err Unknown Command\r\n"))
		}
		// if string(buffer[8:12]) == "PING" {
		// 	_, err = conn.Write([]byte("+PONG\r\n"))
		// 	if err != nil {
		// 		fmt.Println(err, "Write response")
		// 		// return err
		// 		os.Exit(1)
		// 	}
		// } else {
		// 	conn.Write([]byte("-Err Unknown Command\r\n"))
		// }
	}
	// return err
}