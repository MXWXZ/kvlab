package utils

import (
	"hash/crc32"
	"sort"
	"strconv"
)

type UInt32Slice []uint32

func (s UInt32Slice) Len() int {
	return len(s)
}

func (s UInt32Slice) Less(i, j int) bool {
	return s[i] < s[j]
}

func (s UInt32Slice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

type Hash func(data []byte) uint32

type Map struct {
	hash     Hash
	replicas int
	keys     UInt32Slice
	hashMap  map[uint32]string
}

func HashNew(replicas int, fn Hash) *Map {
	m := &Map{
		replicas: replicas,
		hash:     fn,
		hashMap:  make(map[uint32]string),
	}

	if m.hash == nil {
		m.hash = crc32.ChecksumIEEE
	}
	return m
}

func (m *Map) IsEmpty() bool {
	return len(m.keys) == 0
}

func (m *Map) Exist(key string) bool {
	hash := m.hash([]byte("0" + key))
	if v, ok := m.hashMap[hash]; ok && v == key {
		return true
	}
	return false
}

func (m *Map) Add(keys ...string) {
	for _, key := range keys {
		if m.Exist(key) {
			continue
		}
		for i := 0; i < m.replicas; i++ {
			hash := m.hash([]byte(strconv.Itoa(i) + key))
			m.keys = append(m.keys, hash)
			m.hashMap[hash] = key
		}
	}
	sort.Sort(m.keys)
}

func (m *Map) Get(key string) string {
	if m.IsEmpty() {
		return ""
	}

	hash := m.hash([]byte(key))
	idx := sort.Search(len(m.keys), func(i int) bool { return m.keys[i] >= hash })
	if idx == len(m.keys) {
		idx = 0
	}

	return m.hashMap[m.keys[idx]]
}
