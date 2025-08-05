package utils

import (
	"hash/fnv"

	enc "github.com/named-data/ndnd/std/encoding"
)

// Hash returns the hash of a name
// Future: make hash function configurable
func Hash(name string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(name))
	return h.Sum32()
}

// PartitionIdFromName returns the partition ID for a given name
func PartitionIdFromName(name string, totalPartitions int) uint64 {
	return uint64(Hash(name) % uint32(totalPartitions))
}

// PartitionIdFromEncName returns the partition ID for a given encoded name
func PartitionIdFromEncName(name enc.Name, totalPartitions int) uint64 {
	return name.Hash() % uint64(totalPartitions)
}

// Note: unit test
// TODO: put this in a test file
// func main() {
// 	data := make([]string, 0)
// 	data = append(data, "/ndn/")
// 	data = append(data, "/ndn/calendar/day1")
// 	data = append(data, "/ndn/calendar/day2")
// 	data = append(data, "/ndn/self-host/vscode")
// 	data = append(data, "/ndn/management/building")
// 	data = append(data, "/ndn/random/news2")
// 	data = append(data, "/ndn/haotian/repository")
// 	data = append(data, "/ndn/wksp/tianyuan")
// 	data = append(data, "/ndn/edu/ucla/g/yi2021")
// 	data = append(data, "/ndn/edu/ucla/g/yi2021/object1")
// 	data = append(data, "/ndn/edu/ucla/g/yi2021/object2")
// 	data = append(data, "/ndn/edu/ucla/g/yi2021/object3")
// 	data = append(data, "/ndn/edu/ucla/g/yi2021/object4")
// 	data = append(data, "/ndn/edu/ucla/g/yi2021/object5")
// 	data = append(data, "/ndn/edu/ucla/g/yi2021/object6")
// 	data = append(data, "/ndn/testbed/cert")

// 	totalPartitions := 32

// 	for _, entry := range data {
// 		fmt.Printf("Data name: %s -> Partition: %d\n", entry, PartitionIdFromName(entry, totalPartitions))
// 	}
// }
