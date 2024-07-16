package main

import (
	"fmt"
	// Uncomment this block to pass the first stage
	"net"
	"os"
	"log"
	// "bufio"
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
	conn, err := l.Accept()
	if err != nil {
		fmt.Println("Error accepting connection: ", err.Error())
		os.Exit(1)
	}
	defer conn.Close()
	// for {
		// reader := bufio.NewReader(conn)
		// command, err := reader.ReadString('\n')
		// command := make([]byte, 6)
		// _, err := conn.Read(command)
	buffer := make([]byte, 1024)
	_, err = conn.Read(buffer)
	if err != nil {
		fmt.Println("Error reading from connection:", err.Error())
		return
	}
	log.Printf("command: \n%s", string(buffer[8:12]))
	// command = strings.TrimSpace(command)
	// fmt.Println([]byte(buffer[:n]))
	// fmt.Println(string(buffer))
	if string(buffer[8:12]) == "PING" {
		conn.Write([]byte("+PONG\r\n"))
	} else {
		conn.Write([]byte("-Err Unknown Command\r\n"))
	}
	// }
}
