package storage

import (
	"sync"

	"github.com/named-data/ndnd/repo/tlv"
	"github.com/named-data/ndnd/repo/types"
	"github.com/named-data/ndnd/repo/utils"
	enc "github.com/named-data/ndnd/std/encoding"
	"github.com/named-data/ndnd/std/log"
)

// RepoStorage is the storage of the repo
// it is responsible for managing the partitions and the data in the store
// if it doesn't enough space, it may drop partitions with/without coordination of the auction module (TODO)
type RepoStorage struct {
	mutex sync.RWMutex

	repo       *types.RepoShared
	partitions map[uint64]*Partition // the partitions owned by the repo node
}

// NewRepoStorage creates a new repo storage
// TODO: more parameters
func NewRepoStorage(repo *types.RepoShared) *RepoStorage {
	log.Info(nil, "Created Repo Storage")

	return &RepoStorage{
		repo:       repo,
		partitions: make(map[uint64]*Partition),
	}
}

func (s *RepoStorage) String() string {
	return "repo-storage"
}

// RegisterPartition registers a new partition
// Thread-safe
func (s *RepoStorage) RegisterPartition(id uint64) (err error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// TODO: we can infer SVS prefix from partition id
	//  this method handles svs part of partition registration, i.e. joining the group and fetch the right data

	partition := NewPartition(id, s.repo)
	s.partitions[id] = partition

	if err := partition.Start(); err != nil {
		log.Error(s, "Failed to start partition", "err", err)
		delete(s.partitions, id) // rollback
		return err
	}

	log.Info(s, "Registered partition", "id", id)
	return nil
}

// UnregisterPartition unregisters a partition
// Thread-safe
func (s *RepoStorage) UnregisterPartition(id uint64) (err error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	partition, ok := s.partitions[id]
	if !ok {
		log.Error(s, "Partition not found", "id", id)
		return
	}

	if err := partition.Stop(); err != nil {
		log.Error(s, "Failed to stop partition", "err", err)
		return err
	}

	log.Info(s, "Unregistered partition", "id", id)
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

// Handle command commits a command to the responsible partition
// Thread-safe
func (s *RepoStorage) HandleCommand(command *tlv.RepoCommand) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	partitionId := utils.PartitionIdFromEncName(command.CommandName.Name, s.repo.NumPartitions)
	log.Info(s, "Handling command", "command", command, "partition", partitionId)

	s.partitions[partitionId].HandleCommand(command)
}

// Put puts data into the storage
func (s *RepoStorage) Put(name enc.Name, data []byte) (err error) {
	return s.repo.Store.Put(name, data)
}

// Remove removes data from the storage
func (s *RepoStorage) Remove(name enc.Name) (err error) {
	return s.repo.Store.Remove(name)
}
