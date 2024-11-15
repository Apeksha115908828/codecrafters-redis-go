package main

import (
	"fmt"
	"strconv"
	"strings"
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
	stream map[string][]*StreamEntry
}

func (store *Storage) AddToStream(key string, id string, stream_vals map[string]string) string {
	entry := &StreamEntry{
		id:      id,
		kvpairs: stream_vals,
	}
	store.stream[key] = make([]*StreamEntry, 0)
	entries := store.stream[key]
	entries = append(entries, entry)
	store.stream[key] = entries
	return id
}

// Append

func (store *Storage) findKeyInStream(key string) bool {
	_, ok := store.stream[key]
	return ok
}

func (store *Storage) autoGenerateID(key string, id string) string {
	stream, ok := store.stream[key]
	if !ok {
		if id == "*" {
			return "0-1"
		} else {
			major_version := strings.Split(id, "-")[0]
			if major_version == "0" {
				return major_version + "-1"
			} else {
				return major_version + "-0"
			}
		}
	}

	// partial
	if id == "*" {
		max_id := strings.Split(stream[0].id, "-")
		for i := 1; i < len(stream); i++ {
			curr_id := strings.Split(stream[i].id, "-")
			if curr_id[0] > max_id[0] {
				max_id = curr_id
			} else if curr_id[0] == max_id[0] && curr_id[1] > max_id[1] {
				max_id = curr_id
			}
		}
		minor, err := strconv.Atoi(max_id[1])
		if err != nil {
			fmt.Printf("Error using Atoi %v \n", err)
		}
		return max_id[0] + "-" + strconv.Itoa(minor+1)
	} else {
		curr_major := strings.Split(id, "-")[0]
		curr_minor := "0"
		fmt.Printf("curr_major = %s curr_minor = %s\n", curr_major, curr_minor)
		for i := 0; i < len(stream); i++ {
			curr_id := strings.Split(stream[i].id, "-")
			if curr_id[0] == curr_major {
				if curr_id[1] >= curr_minor {
					minor, err := strconv.Atoi(curr_id[1])
					if err != nil {
						fmt.Printf("Error using Atoi %v \n", err)
					}
					curr_minor = strconv.Itoa(minor + 1)
					fmt.Printf("curr_major = %s curr_minor = %s\n", curr_major, curr_minor)
				}
			}
		}
		if curr_major == "0" && curr_minor == "0" {
			return "0-1"
		}
		return curr_major + "-" + curr_minor
	}
}

func (store *Storage) checkIDValidity(key string, id string) (string, bool) {
	if id == "0-0" {
		return "must be greater than 0-0", false
	}
	stream, ok := store.stream[key]
	if !ok {
		return "", true
	} else {
		new_id := strings.Split(id, "-")
		isStillValid := true

		for index := range stream {
			entry := stream[index]
			old_id := strings.Split(entry.id, "-")
			if new_id[0] > old_id[0] || (new_id[0] == old_id[0] && new_id[1] > old_id[1]) {
				isStillValid = true
			} else {
				return "is equal or smaller than the target stream top item", false
			}
			fmt.Printf("old_id = %s-%s, new_id = %s-%s", old_id[0], old_id[1], new_id[0], new_id[1])
		}
		fmt.Printf("\nlen(stream) = %d\n", len(stream))
		return "", isStillValid
	}
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
		stream: make(map[string][]*StreamEntry),
	}
}
