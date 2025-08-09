package storage

import (
	enc "github.com/named-data/ndnd/std/encoding"
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

	repoNameN enc.Name // name of the repo node
	nodeNameN enc.Name // name of the local node
}

// NewRepoStorage creates a new repo storage
// TODO: more parameters
func NewRepoStorage(repoNameN enc.Name, nodeNameN enc.Name, store ndn.Store, client ndn.Client) *RepoStorage {
	log.Info(nil, "Created Repo Storage")

	return &RepoStorage{
		store:      store,
		partitions: make(map[uint64]*Partition),
		client:     client,
		repoNameN:  repoNameN,
		nodeNameN:  nodeNameN,
	}
}

func (s *RepoStorage) String() string {
	return "repo-storage"
}

// RegisterPartition registers a new partition
func (s *RepoStorage) RegisterPartition(id uint64) (err error) {
	// TODO: we can infer SVS prefix from partition id
	//  this method handles svs part of partition registration, i.e. joining the group and fetch the right data

	partition := NewPartition(id, s.client, s)
	s.partitions[id] = partition

	if err := partition.Start(); err != nil {
		log.Error(s, "Failed to start partition", "err", err)
		delete(s.partitions, id) // rollback
		return err
	}

	return nil
}

// UnregisterPartition unregisters a partition
func (s *RepoStorage) UnregisterPartition(id uint64) (err error) {
	partition, ok := s.partitions[id]
	if !ok {
		log.Error(s, "Partition not found", "id", id)
		return
	}

	if err := partition.Stop(); err != nil {
		log.Error(s, "Failed to stop partition", "err", err)
		return err
	}

	delete(s.partitions, id)
	return nil
}

// Close stops all partitions and deletes them from the storage
func (s *RepoStorage) Close() (err error) {
	log.Info(s, "Closed Repo Storage")

	for id, partition := range s.partitions {
		if err := partition.Stop(); err != nil {
			log.Error(s, "Failed to stop partition", "err", err)
			return err
		}
		delete(s.partitions, id)
	}

	return nil
}

// Put puts data into the storage
func (s *RepoStorage) Put(name enc.Name, data []byte) (err error) {
	return s.store.Put(name, data)
}

// Remove removes data from the storage
func (s *RepoStorage) Remove(name enc.Name) (err error) {
	return s.store.Remove(name)
}
