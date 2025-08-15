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

	fetchDataHandler func(name enc.Name)
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

	// check if partition already exists
	if _, exists := s.partitions[id]; exists {
		log.Info(s, "Partition already exists, no need to register", "id", id)
		return nil
	}

	partition := NewPartition(id, s.repo, s) // TODO: very ugly design, need to refactor
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

	partitionId := utils.PartitionIdFromEncName(command.SrcName.Name, s.repo.NumPartitions)
	log.Info(s, "Handling command", "command", command, "partition", partitionId)

	partition, exists := s.partitions[partitionId]
	if !exists {
		log.Error(s, "Partition not found", "partitionId", partitionId)
		return
	}

	partition.CommitCommand(command)
}

// Handle status request checks local state and reply with the result
// Thread-safe
func (s *RepoStorage) HandleStatus(statusRequest *tlv.RepoStatus) tlv.RepoStatusReply {
	partition := s.GetPartition(statusRequest.Name.Name)

	reply := tlv.RepoStatusReply{
		Name:  statusRequest.Name,
		Nonce: statusRequest.Nonce,
	}

	if partition == nil {
		reply.Status = 400 // TODO: enumeration
		log.Error(s, "Can't handle status request: partition not found", "status request", statusRequest)
		return reply
	}

	log.Info(s, "Handling status request", "status request", statusRequest, "partitionId", partition.id)

	// TODO: ideally we should know if the check is about a sync group or a data object. However, we will just handle both cases here by checking both storage, given that data objects and svs groups can not share the same name
	wire, _ := s.repo.Store.Get(statusRequest.Name.Name, false)
	if wire != nil || partition.OwnsSvsGroup(statusRequest.Name.Name.String()) {
		reply.Status = 200
	}

	return reply
}

// Put puts data into the storage
func (s *RepoStorage) Put(name enc.Name, data []byte) (err error) {
	return s.repo.Store.Put(name, data)
}

// Remove removes data from the storage
func (s *RepoStorage) Remove(name enc.Name) (err error) {
	return s.repo.Store.Remove(name)
}

// GetPartition gets the partition that owns the resource
// Thread-safe
func (s *RepoStorage) GetPartition(name enc.Name) *Partition {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	partitionId := utils.PartitionIdFromEncName(name, s.repo.NumPartitions)
	partition, exists := s.partitions[partitionId]

	if !exists {
		return nil
	} else {
		return partition
	}
}

// CommitInsert puts an insertion command and relevant data into the storage, and updates partition state
func (s *RepoStorage) CommitInsert(name enc.Name, command *tlv.RepoCommand, data enc.Wire) (err error) {
	partition := s.GetPartition(name)
	err = partition.CommitInsert(command)

	// All incoming data are automatically stored in the store (setupEngineHook), so we don't need to do it explicitly here.
	// s.Put(name, data.Join())

	return err
}

func (s *RepoStorage) CommitDelete(name enc.Name, command *tlv.RepoCommand) (err error) {
	partition := s.GetPartition(name)
	err = partition.CommitDelete(command)

	s.repo.Store.Remove(name) // remove data from the store

	return err
}

func (s *RepoStorage) CommitJoin(name enc.Name, command *tlv.RepoCommand) (err error) {
	partition := s.GetPartition(name)
	err = partition.CommitJoin(command)

	// User sync groups are handled by each partition independently, and only the data uses shared storage

	return err
}

func (s *RepoStorage) CommitLeave(name enc.Name, command *tlv.RepoCommand) (err error) {
	partition := s.GetPartition(name)
	err = partition.CommitLeave(command)

	// TODO: remove all data in the user sync group from the store

	return err
}

// OwnsResource checks if the storage owns the resource (data or svs group)
func (s *RepoStorage) OwnsResource(name enc.Name) bool {
	partition := s.GetPartition(name)
	if partition == nil {
		return false
	}

	wire, _ := s.repo.Store.Get(name, false)
	return partition.OwnsSvsGroup(name.String()) || wire != nil
}

// Handlers
func (s *RepoStorage) SetFetchDataHandler(handler func(name enc.Name)) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.fetchDataHandler = handler
}
