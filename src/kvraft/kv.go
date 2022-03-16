package kvraft

import (
	"errors"
	"sync"
)

var ErrKeyNotFound = errors.New("kvstore: errkeynotfound")

type KVInterface interface {
	Put(string, string) error
	Get(string) (string, error)
	Append(string, string) error
}

type SimpleKVStore struct {
	data map[string]string
	mu   sync.Mutex
}

func NewKVStore() KVInterface {
	sks := &SimpleKVStore{}
	return sks
}

func (sk *SimpleKVStore) Get(key string) (string, error) {
	sk.mu.Lock()
	defer sk.mu.Unlock()
	data, ok := sk.data[key]
	if ok {
		return data, nil
	}
	return "", ErrKeyNotFound
}

func (sk *SimpleKVStore) Put(key string, value string) error {
	sk.mu.Lock()
	defer sk.mu.Unlock()
	sk.data[key] = value
	return nil
}

func (sk *SimpleKVStore) Append(key string, value string) error {
	sk.mu.Lock()
	defer sk.mu.Unlock()

	// todo: 这里如果不存在key，应该报错还是依旧append？
	data, ok := sk.data[key]
	if ok {
		sk.data[key] = data + value
		return nil
	} else {
		return ErrKeyNotFound
	}
}
