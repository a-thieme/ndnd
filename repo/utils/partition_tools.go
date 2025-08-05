package utils

import (
	enc "github.com/named-data/ndnd/std/encoding"
)

// IdFromDataName calculates the partition ID for a given data name.
// It uses a hash function to map the name to a partition ID.
// The total number of partitions is used to ensure the partition ID is within the valid range.
//
// Parameters:
// - name: The data name to calculate the partition ID for.
// - hash: A function that takes an enc.Name and returns a uint.
// - totalPartitions: The total number of partitions to use for the hash function.
//
// Returns:
// - The partition ID for the given data name.
func IdFromDataName(name enc.Name, hash func(enc.Name) uint, totalPartitions int) uint {
	return hash(name) % uint(totalPartitions)
}
