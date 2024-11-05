package main

import (
	"fmt"
	"time"
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
	db map[string]*DataEntry
}

func (store *Storage) GetFromDataBase(key string) (*string, error) {
	if store.db == nil {
        return nil, fmt.Errorf("database is not initialized")
    }
	fmt.Println("size of the store: ", len(store.db))
	for k, v := range store.db {
        fmt.Printf("Key: %s, Value: %+v\n", k, v)
    }
	value, ok := store.db[key]
	if !ok {
		return nil, fmt.Errorf("Error while retrieving the value of the entry %v for key %s", ok, key)
	}

	if !value.expiry.IsZero() && time.Now().After(value.expiry) {
		fmt.Println("figured out it is expired")
		delete(store.db, key)
		return nil, fmt.Errorf("Expired key...")
	}
	
	return &value.value, nil
}

// func AddToDataBase(store *Storage, args Array) {
func (store *Storage) AddToDataBase(key string, value string, expiryVal time.Time) {
	store.db[key] = &DataEntry{
		value:  value,
		expiry: expiryVal,
	}
	fmt.Println("AddtoDatabase storing data", store.db[key].value)
	fmt.Println("In set function size of the store: ", len(store.db))
}

func NewStore() *Storage {
	fmt.Println("Came to create a new store")
	return &Storage{
		db: make(map[string]*DataEntry),
	}
}
