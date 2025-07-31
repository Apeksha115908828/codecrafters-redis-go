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
	conn              map[string]*Connection
	issubscribed      map[string]bool
	subscribedClients map[string][]string // map of topics to list of clients subscribed to that topic
	subscribedTopics  map[string][]string // map of clients to list of topics subscribed to that client
	opts              Opts
	storage           *Storage

	mc *MasterConfig
	// isqueuing       bool
	// queue           [][]string
	isqueuing       map[string]bool
	queue           map[string][][]string
	blockingClients map[string][]*BlockingClient
	mutex           sync.RWMutex
	blockingResults map[string](map[string]*BlockingResult) // map of key to map of blockingresult
	resultmutex     sync.RWMutex
}

type BlockingClient struct {
	conn       *Connection
	key        string
	resultChan chan string
	timeout    float64
}

type BlockingResult struct {
	conn   *Connection
	key    string
	result string
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
	connections := make(map[string]*Connection)
	connections[conn.conn.RemoteAddr().String()] = conn
	isqueuingval := make(map[string]bool)
	isqueuingval[conn.conn.RemoteAddr().String()] = false
	queueval := make(map[string][][]string)
	queueval[conn.conn.RemoteAddr().String()] = make([][]string, 0)
	issubscribedval := make(map[string]bool)
	issubscribedval[conn.conn.RemoteAddr().String()] = false
	return &Server{
		conn:    connections,
		opts:    opts,
		storage: storage,

		mc: mc,
		// 		isqueuing:       false,
		// queue:           make([][]string, 0),
		isqueuing:         isqueuingval,
		queue:             queueval,
		blockingClients:   make(map[string][]*BlockingClient),
		issubscribed:      issubscribedval,
		subscribedTopics:  make(map[string][]string),
		subscribedClients: make(map[string][]string),
		mutex:             sync.RWMutex{},
		blockingResults:   make(map[string](map[string]*BlockingResult)), // map of key to map of blockingresult
		resultmutex:       sync.RWMutex{},
	}
}

func NewReplica(conn *Connection, opts Opts) *Server {
	connections := make(map[string]*Connection)
	connections[conn.conn.RemoteAddr().String()] = conn
	return &Server{
		conn:              connections,
		opts:              opts,
		storage:           storage, //TODO: what to put here??
		blockingClients:   make(map[string][]*BlockingClient),
		issubscribed:      make(map[string]bool),
		subscribedTopics:  make(map[string][]string),
		subscribedClients: make(map[string][]string),
		mutex:             sync.RWMutex{},
		blockingResults:   make(map[string](map[string]*BlockingResult)), // map of key to map of blockingresult
		resultmutex:       sync.RWMutex{},
	}
}
