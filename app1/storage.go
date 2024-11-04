package main

import (
	"fmt"
	"time"
	"log"
)

// type Duration int64 nanosecond

type DataEntry struct {
	value string
	// HasExpiry bool
	expiry time.Time
}

// use ParseDuration to get duration from string
// func ParseDuration(s string) (Duration, error)
// ParseDuration parses a duration string. A duration string is a possibly signed sequence of decimal numbers,
// each with optional fraction and a unit suffix, such as "300ms", "-1.5h" or "2h45m". Valid time units are "ns",
// "us" (or "µs"), "ms", "s", "m", "h".

type Storage struct {
	db map[string]DataEntry
}

func (store *Storage) GetFromDataBase(key string) (string, error) {
	log.Println("came to get from database......")

	if !store.db[key].expiry.IsZero() && time.Now().After(store.db[key].expiry) {
		log.Println("figured out it is expired")
		delete(store.db, key)
		return "", fmt.Errorf("Expired key...")
	}
	value, ok := store.db[key]
	if !ok {
		return "", fmt.Errorf("Error while retrieving the value of the entry")
	}
	return value.value, nil
}

// func AddToDataBase(store *Storage, args Array) {
func (store *Storage) AddToDataBase(key string, value string, expiryVal time.Time) {
	data := DataEntry{
		value:  value,
		expiry: expiryVal,
	}
	store.db[key] = data
}

func NewStore() *Storage {
	fmt.Println("Came to create a new store")
	return &Storage{
		db: make(map[string]DataEntry),
	}
}
