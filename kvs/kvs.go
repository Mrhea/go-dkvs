// Package kvs provides a simple JSON database in the form of a slice of Entry
// data types, an API to access it, and an exported Entry data type.
package kvs

import (
	"log"
)

// Database is a simple key-value store used to store Entry structs.
type Database struct {
	entrydb       map[string]*Entry
	latestVersion int
}

// InitDB returns a reference to a key-value store database
func InitDB() *Database {
	var db Database
	db.entrydb = make(map[string]*Entry)
	db.latestVersion = 0
	return &db
}

// Entry data structure that contains a key and value
// as JSON formated strings.
// Entries now contain a metadata field for storing versions
type Entry struct {
	Key     string `json:"key"`
	Val     string `json:"value"`
	Version int    `json:"version"`
	Meta    []int  `json:"causal-metadata"` //keep this slice sorted
}

func GetVer(db *Database) int {
	return db.latestVersion
}

func UpdateVer(v int, db *Database) {
	db.latestVersion = v
}

type Transfer struct {
	Entries []Entry `json:"entries"`
	Version int     `json:"version"`
}

// Database of keys mapped to Entry structs
// var entrydb = make(map[string]*Entry)

// ConvertMapToSlice flattens map data into an array of
// Entry structs with JSON formatted fields
func ConvertMapToSlice(db *Database) Transfer {
	valueSlice := []Entry{}
	for _, value := range db.entrydb {
		valueSlice = append(valueSlice, *value)
	}
	ret := Transfer{valueSlice, db.latestVersion}
	return ret
}

// AddAllKVPairs - takes the slice of entries from announce() and adds each one to the store
// FROM: rest/announce()
func AddAllKVPairs(t Transfer, db *Database) {
	log.Println("Adding the entries to db on start up of new replica this is the key of first entry: ")
	db.latestVersion = t.Version
	for _, e := range t.Entries {
		log.Println("An entry received from announce: ", e)
		db.entrydb[e.Key] = &e
	}
}

// InsertExampleData loads example entries into kvs.
func InsertExampleData(db *Database) {
	e1 := Entry{Key: "abc", Val: "a"}
	e2 := Entry{Key: "def", Val: "b"}
	db.entrydb[e1.Key] = &e1
	db.entrydb[e2.Key] = &e2
}

// InsertEntry places a key-value pair (Entry) into KVS.
func InsertEntry(e Entry, db *Database) {
	log.Println("Key-Value-Store: Inserting Entry into slice")
	db.entrydb[e.Key] = &e // Pass in mutable reference to the entry
}

// RemoveEntry deletes a key-value pair from KVS.
// Returns true if succes, false if failed.
func RemoveEntry(key string, db *Database) bool {
	log.Println("Key-Value-Store: Attempting to delete entry from kvs")
	ok := CheckIfKeyExists(key, db)
	if !ok {
		log.Println("Key-Value-Store: Failed remove Entry from kvs")
		return false
	}
	log.Println("Key-Value-Store: Deleting Entry from kvs")
	delete(db.entrydb, key)
	return true
}

func EraseEntry(key string, db *Database) {
	db.entrydb[key].Val = "" //NULL VALUE CURRENTLY 0
}

// GetValueOfEntry returns the value associated with a key
// in KVS. Should be used after confirming if key exists within kvs as
// there is no error handling for this case at the moment.
func GetValueOfEntry(key string, db *Database) string {
	log.Println("Key-Value-Store: Getting value of key from Entry")
	e := db.entrydb[key]
	return e.Val
}

func GetEntryStruct(key string, db *Database) Entry {
	log.Println("Key-Value-Store")
	return *db.entrydb[key]
}

// CheckIfKeyExists returns true if the key inputted exists
// or false if the key is not in the KVS.
func CheckIfKeyExists(key string, db *Database) bool {
	log.Println("Key-Value-Store: Checking if key exists within entries slice")

	_, ok := db.entrydb[key]
	if ok {
		return true
	}

	return false
}
