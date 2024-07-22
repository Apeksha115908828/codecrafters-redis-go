package main

import (
	"time"
	"fmt"
	// "os"
	// "strconv"
	"log"
	// "strings"
)

// type Duration int64 nanosecond

type dataEntry struct {
	Value string
	// HasExpiry bool
	Expiry time.Time
}
// use ParseDuration to get duration from string
// func ParseDuration(s string) (Duration, error)
// ParseDuration parses a duration string. A duration string is a possibly signed sequence of decimal numbers, 
// each with optional fraction and a unit suffix, such as "300ms", "-1.5h" or "2h45m". Valid time units are "ns", 
// "us" (or "µs"), "ms", "s", "m", "h".

type Storage struct {
	db map[string] dataEntry
}


func GetFromDataBase(store *Storage, key string) (string) {
	log.Println("came to get from database......")

	if time.Now().After(store.db[key].Expiry) {
		log.Println("figured out it is expired")
		delete(store.db, key)
		return ""
	}
	_, ok := store.db[key]
	if !ok {
		fmt.Errorf("Error while retrieving the value of the entry")
		return ""
	}
	return store.db[key].Value
}

// func AddToDataBase(store *Storage, args Array) {
func AddToDataBase(store *Storage, key string, value string, expiry int) {
	data := dataEntry{
		Value: value,
		Expiry: time.Now().Add(time.Duration(expiry) * time.Millisecond),
	}
	store.db[key] = data
}

func NewStore() *Storage{
	fmt.Println("Came to create a new store")
	return &Storage{
		db: make(map[string]dataEntry),
	}
}
