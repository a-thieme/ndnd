package storage

import (
	"github.com/named-data/ndnd/std/log"
	"github.com/named-data/ndnd/std/ndn"
)

// RepoStorage is the storage of the repo
// it is responsible for managing the partitions and the data in the store
// if it doesn't enough space, it may drop partitions with/without coordination of the auction module (TODO)
type RepoStorage struct {
	store      ndn.Store             // the store of the repo
	partitions map[uint64]*Partition // the partitions owned by the repo node

	client ndn.Client
}

// NewRepoStorage creates a new repo storage
// TODO: more parameters
func NewRepoStorage(store ndn.Store, client ndn.Client) *RepoStorage {
	return &RepoStorage{
		store:      store,
		partitions: make(map[uint64]*Partition),
		client:     client,
	}
}

func (s *RepoStorage) String() string {
	return "repo-storage"
}

// RegisterPartition registers a new partition
func (s *RepoStorage) RegisterPartition(id uint64) {
	// TODO: we can infer SVS prefix from partition id
	//  this method handles svs part of partition registration, i.e. joining the group and fetch the right data

	partition := NewPartition(id, s.client, s)
	s.partitions[id] = partition

	if err := partition.Start(); err != nil {
		log.Error(s, "Failed to start partition", "err", err)
		return
	}
}

// UnregisterPartition unregisters a partition
func (s *RepoStorage) UnregisterPartition(id uint64) {
	partition, ok := s.partitions[id]
	if !ok {
		log.Error(s, "Partition not found", "id", id)
		return
	}

	if err := partition.Stop(); err != nil {
		log.Error(s, "Failed to stop partition", "err", err)
		return
	}

	delete(s.partitions, id)
}
