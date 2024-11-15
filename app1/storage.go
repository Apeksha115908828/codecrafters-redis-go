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

type StreamEntry struct {
	id      string
	kvpairs map[string]string
}

// use ParseDuration to get duration from string
// func ParseDuration(s string) (Duration, error)
// ParseDuration parses a duration string. A duration string is a possibly signed sequence of decimal numbers,
// each with optional fraction and a unit suffix, such as "300ms", "-1.5h" or "2h45m". Valid time units are "ns",
// "us" (or "µs"), "ms", "s", "m", "h".

type Storage struct {
	db     map[string]*DataEntry
	stream map[string]*StreamEntry
}

func (store *Storage) AddToStream(key string, id string, stream_vals map[string]string) {
	store.stream[key] = &StreamEntry{
		id:      id,
		kvpairs: stream_vals,
	}
}

func (store *Storage) findKeyInStream(key string) bool {
	_, ok := store.stream[key]
	if !ok {
		return false
	}
	return true
}

func (store *Storage) GetFromDataBase(key string) (*string, error) {
	if store.db == nil {
		return nil, fmt.Errorf("database is not initialized")
	}
	fmt.Println("size of the store: ", len(store.db))
	// for k, v := range store.db {
	// 	fmt.Printf("Key: %s, Value: %+v\n", k, v)
	// }
	value, ok := store.db[key]
	if !ok {
		return nil, fmt.Errorf("error while retrieving the value of the entry %v for key %s", ok, key)
	}

	if !value.expiry.IsZero() && time.Now().After(value.expiry) {
		fmt.Println("figured out it is expired")
		delete(store.db, key)
		return nil, fmt.Errorf("expired key")
	}

	return &value.value, nil
}

// func AddToDataBase(store *Storage, args Array) {
func (store *Storage) AddToDataBase(key string, value string, expiryVal time.Time) {
	_, err := store.GetFromDataBase(key)
	if err != nil {
		store.db[key] = &DataEntry{
			value:  value,
			expiry: expiryVal,
		}
	} else {
		entry, ok := store.db[key]
		if !ok {
			fmt.Println("Something happened while fetching existing entry")
			return
		}
		store.db[key] = &DataEntry{
			value:  value,
			expiry: entry.expiry,
		}
	}
	fmt.Println("AddtoDatabase storing data", store.db[key].value)
	fmt.Println("In set function size of the store: ", len(store.db))
}

func (store *Storage) getAllKeysFromRDB() ([]string, error) {
	var keys []string
	fmt.Printf("came here, store.db size = %d", len(store.db))
	for key, value := range store.db {
		keys = append(keys, key)
		fmt.Printf("Key: %s, Value: %s\n", key, value.value)
	}
	return keys, nil
}

func NewStore() *Storage {
	fmt.Println("Came to create a new store")
	return &Storage{
		db:     make(map[string]*DataEntry),
		stream: make(map[string]*StreamEntry),
	}
}
