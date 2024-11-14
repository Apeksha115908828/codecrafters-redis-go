package main

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"sync"
)

type Opts struct {
	Port       string `short:"p" long:"port" description:"Port Number" default:"6379"`
	ReplicaOf  string `long:"replicaof" description:"Replica of <MASTER_HOST> <MASTER_PORT>"`
	Dir        string `long:"dir" description:"the path to the directory where the RDB file is stored"`
	DbFileName string `long:"dbfilename" description:"the name of the RDB file"`

	Role       string
	ReplicaId  string
	MasterHost string
	MasterPort string
}

func (options *Opts) Config() {
	options.Role = "master"
	options.ReplicaId = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb" // can write a function generate random replica id
	master := strings.Split(options.ReplicaOf, " ")
	if options.ReplicaOf != "" {
		options.Role = "slave"
		options.ReplicaId = ""
		options.MasterHost = master[0]
		options.MasterPort = master[1]
	}
}

type Connection struct {
	conn   net.Conn
	reader *bufio.Reader
	offset int
}

type MasterConfig struct {
	slaves     *Slaves // change to slaves class
	wg         *sync.WaitGroup
	propOffset int
}

type Server struct {
	conn    *Connection
	opts    Opts
	storage *Storage

	mc *MasterConfig
}

type Slaves struct {
	list map[string]*Connection
	lock sync.RWMutex
}

func NewSlaves() *Slaves {
	return &Slaves{
		list: make(map[string]*Connection),
		lock: sync.RWMutex{},
	}
}

func (slaves *Slaves) AddToSlaves(addr net.Addr, conn *Connection) {
	slaves.lock.Lock()
	defer slaves.lock.Unlock()
	fmt.Println("Add to slaves called........")
	slaves.list[addr.String()] = conn
}

func (slaves *Slaves) HandleAck(addr net.Addr, ack int) error {
	slaves.lock.Lock()
	defer slaves.lock.Unlock()

	slave, ok := slaves.list[addr.String()]
	if !ok {
		return fmt.Errorf("slave retrieval failed for %s", addr.String())
	}
	slave.offset = ack
	fmt.Println("Setting offset of", slave, " to ack = ", ack)
	return nil
}

func (slaves *Slaves) Count() int {
	slaves.lock.RLock()
	defer slaves.lock.RUnlock()

	return len(slaves.list)
}

var storage = NewStore()

func NewMaster(conn *Connection, opts Opts, mc *MasterConfig) *Server {
	return &Server{
		conn:    conn,
		opts:    opts,
		storage: storage, //TODO: what to put here??

		mc: mc,
	}
}

func NewReplica(conn *Connection, opts Opts) *Server {
	return &Server{
		conn:    conn,
		opts:    opts,
		storage: storage, //TODO: what to put here??
	}
}
