package main

import (
	"fmt"
	"hash/crc32"
	"sort"
	"strconv"
	"sync"
)

//hashRing store sorted hashed value
type hashRing []uint32

//implement sort interface
func (h hashRing) Len() int {
	return len(h)
}
func (h hashRing) Less(i, j int) bool {
	return h[i] < h[j]
}
func (h hashRing) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

//Node interface defines elements can be added to consistent hashing
type Node interface {
	JoinStr(i int) string
	Weight() int
}

type Server struct {
	weight int
	Id     int
	Ip     string
	Port   int
}

func (s Server) JoinStr(i int) string {
	return strconv.Itoa(s.Id) + "-" + s.Ip + ":" + strconv.Itoa(s.Port) + "-" + strconv.Itoa(s.weight) + "-" + strconv.Itoa(i)
}
func (s Server) Weight() int {
	return s.weight
}

type ConsistentHashing struct {
	sync.RWMutex
	ring         hashRing
	Nodes        map[uint32]Node
	ReplicaCount int
}

func NewConsistentHashing(repCounts int) *ConsistentHashing {
	return &ConsistentHashing{
		ring:         hashRing{},
		Nodes:        make(map[uint32]Node),
		ReplicaCount: repCounts,
	}
}

//AddNode add Node to ConsistentHashing
func (c *ConsistentHashing) AddNode(node Node) {

	//add nodes
	count := node.Weight() * c.ReplicaCount
	c.Lock()
	for i := 0; i < count; i++ {
		hashKey := c.hash(node.JoinStr(i))
		c.Nodes[hashKey] = node
		c.ring = append(c.ring, hashKey)
	}
	//sort ring as low to high
	sort.Sort(c.ring)
	c.Unlock()
}

//GetNode return a node by searching the key in hash ring
func (c *ConsistentHashing) GetNode(key string) Node {
	hashKey := crc32.ChecksumIEEE([]byte(key))
	//search
	n := len(c.ring)
	c.RLock()
	defer c.RUnlock()
	i := sort.Search(n,
		func(i int) bool {
			return c.ring[i] >= hashKey
		})
	if i < n {
		return c.Nodes[c.ring[i]]
	}
	return c.Nodes[c.ring[0]]
}

func (c *ConsistentHashing) hash(s string) uint32 {
	return crc32.ChecksumIEEE([]byte(s))
}

func main() {

	ch := NewConsistentHashing(100)

	for i := 1; i < 20; i++ {
		ser := Server{
			weight: 1,
			Id:     i,
			Ip:     "192.168.0." + strconv.Itoa(i),
			Port:   1080,
		}
		ch.AddNode(ser)
	}

	ipMap := make(map[string]int, 0)
	for i := 0; i < 1000; i++ {
		si := fmt.Sprintf("ke123456y%d", i)
		k := (ch.GetNode(si)).(Server)
		if _, ok := ipMap[k.Ip]; ok {
			ipMap[k.Ip] += 1
		} else {
			ipMap[k.Ip] = 1
		}
	}

	for k, v := range ipMap {
		fmt.Println("Node IP:", k, " count:", v)
	}

}
