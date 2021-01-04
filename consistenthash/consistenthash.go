package consistenthash

import (
	"errors"
	"hash/crc32"
	"sort"
	"strconv"
	"sync"
)

type HashFunc func([]byte) uint32

type Map struct {
	sync.RWMutex
	hashFunc HashFunc
	keyMap   map[int]string
	sortKeys []int
	replicas int
}

func New(replicas int, fn HashFunc) *Map {
	m := &Map{
		hashFunc: fn,
		replicas: replicas,
	}
	m.hashFunc = crc32.ChecksumIEEE
	return m
}

func (m Map) Add(keys ...string) {
	m.Lock()
	defer m.Unlock()
	for _, key := range keys {
		for i := 0; i < m.replicas; i++ {
			hash := int(m.hashFunc([]byte(key + strconv.Itoa(i))))
			m.sortKeys = append(m.sortKeys, hash)
			m.keyMap[hash] = key
		}
	}
	sort.Ints(m.sortKeys)
}

func (m Map) Get(key string) (string, error) {
	m.RLock()
	defer m.RUnlock()
	if m.sortKeys == nil || len(m.sortKeys) == 0 {
		return "", errors.New("add key first")
	}
	hash := m.hashFunc([]byte(key))
	i := sort.SearchInts(m.sortKeys, int(hash))
	if i >= len(m.sortKeys) {
		return m.keyMap[m.sortKeys[0]], nil
	}
	return m.keyMap[m.sortKeys[i]], nil
}

func (m Map) Remove(key string) {
	m.Lock()
	defer m.Unlock()
	for i := 0; i < m.replicas; i++ {
		hash := int(m.hashFunc([]byte(key + strconv.Itoa(i))))
		delete(m.keyMap, hash)

		i := sort.SearchInts(m.sortKeys, hash)
		if i <= len(m.sortKeys) {
			m.sortKeys = append(m.sortKeys[:i], m.sortKeys[i+1:]...)
		}
	}
}
