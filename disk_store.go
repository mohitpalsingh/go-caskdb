package caskdb

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"time"
)

// DiskStore is a Log-Structured Hash Table as described in the BitCask paper. We
// keep appending the data to a file, like a log. DiskStorage maintains an in-memory
// hash table called KeyDir, which keeps the row's location on the disk.
//
// The idea is simple yet brilliant:
//   - Write the record to the disk
//   - Update the internal hash table to point to that byte offset
//   - Whenever we get a read request, check the internal hash table for the address,
//     fetch that and return
//
// KeyDir does not store values, only their locations.
//
// The above approach solves a lot of problems:
//   - Writes are insanely fast since you are just appending to the file
//   - Reads are insanely fast since you do only one disk seek. In B-Tree backed
//     storage, there could be 2-3 disk seeks
//
// However, there are drawbacks too:
//   - We need to maintain an in-memory hash table KeyDir. A database with a large
//     number of keys would require more RAM
//   - Since we need to build the KeyDir at initialisation, it will affect the startup
//     time too
//   - Deleted keys need to be purged from the file to reduce the file size
//
// Read the paper for more details: https://riak.com/assets/bitcask-intro.pdf
//
// DiskStore provides two simple operations to get and set key value pairs. Both key
// and value need to be of string type, and all the data is persisted to disk.
// During startup, DiskStorage loads all the existing KV pair metadata, and it will
// throw an error if the file is invalid or corrupt.
//
// Note that if the database file is large, the initialisation will take time
// accordingly. The initialisation is also a blocking operation; till it is completed,
// we cannot use the database.
//
// Typical usage example:
//
//		store, _ := NewDiskStore("books.db")
//	   	store.Set("othello", "shakespeare")
//	   	author := store.Get("othello")
type DiskStore struct {
	file          *os.File
	keyDir        map[string]KeyEntry
	writePosition uint32
}

func isFileExists(fileName string) bool {
	// https://stackoverflow.com/a/12518877
	if _, err := os.Stat(fileName); err == nil || errors.Is(err, fs.ErrExist) {
		return true
	}
	return false
}

func NewDiskStore(fileName string) (*DiskStore, error) {
	ds := &DiskStore{keyDir: make(map[string]KeyEntry)}

	if isFileExists(fileName) {
		ds.initKeyDir(fileName)
	}

	file, err := os.OpenFile(fileName, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	ds.file = file
	return ds, nil
}

func (d *DiskStore) Get(key string) string {
	keyEntry, ok := d.keyDir[key]
	if !ok {
		return ""
	}

	d.file.Seek(int64(keyEntry.position), 0)
	data := make([]byte, keyEntry.totalSize)
	_, err := io.ReadFull(d.file, data)
	if err != nil {
		panic("read error!")
	}
	_, _, value := decodeKV(data)
	return value
}

func (d *DiskStore) Set(key string, value string) {
	timestamp := uint32(time.Now().Unix())
	size, data := encodeKV(timestamp, key, value)
	d.write(data)
	d.keyDir[key] = NewKeyEntry(timestamp, uint32(d.writePosition), uint32(size))
	d.writePosition += uint32(size)
}

func (d *DiskStore) Close() bool {
	d.file.Sync()
	if err := d.file.Close(); err != nil {
		return false
	}
	return true
}

func (d *DiskStore) write(data []byte) {
	if _, err := d.file.Write(data); err != nil {
		panic(err)
	}
	if err := d.file.Sync(); err != nil {
		panic(err)
	}
}

func (d *DiskStore) initKeyDir(existingFile string) {
	file, _ := os.Open(existingFile)
	defer file.Close()
	for {
		header := make([]byte, headerSize)
		_, err := io.ReadFull(file, header)
		if err == io.EOF {
			break
		}
		if err != nil {
			break
		}
		timestamp, keySize, valueSize := decodeHeader(header)
		key := make([]byte, keySize)
		value := make([]byte, valueSize)
		_, err = io.ReadFull(file, key)
		if err != nil {
			break
		}
		_, err = io.ReadFull(file, value)
		if err != nil {
			break
		}
		totalSize := headerSize + keySize + valueSize
		d.keyDir[string(key)] = NewKeyEntry(timestamp, uint32(d.writePosition), totalSize)
		d.writePosition += uint32(totalSize)
		fmt.Printf("loaded key=%s, value=%s\n", key, value)
	}
}
