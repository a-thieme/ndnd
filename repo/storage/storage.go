package storage

import (
	"sync"

	enc "github.com/named-data/ndnd/std/encoding"
	spec "github.com/named-data/ndnd/std/ndn/spec_2022"

	"github.com/named-data/ndnd/repo/tlv"
	"github.com/named-data/ndnd/repo/types"
	"github.com/named-data/ndnd/std/log"
)

// RepoStorage is the storage of the repo
// it is responsible for managing the partitions and the data in the store
// if it doesn't enough space, it may drop partitions with/without coordination of the auction module
type RepoStorage struct {
	mutex sync.RWMutex

	repo *types.RepoShared
	// map target to the job
	jobs map[*enc.Name]*tlv.RepoCommand

	fetchDataHandler func(name enc.Name)
}

// NewRepoStorage creates a new repo storage
func NewRepoStorage(repo *types.RepoShared) *RepoStorage {
	log.Info(nil, "Created Repo Storage")

	return &RepoStorage{
		repo: repo,
		jobs: make(map[*enc.Name]*tlv.RepoCommand),
	}
}

func (s *RepoStorage) String() string {
	return "repo-storage"
}

// Thread-safe
func (s *RepoStorage) doCommand(command *tlv.RepoCommand) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	log.Info(s, "Handling command", "command", command)
	// FIXME: this needs to either consume data or join an svs group
	// FIXME: for testing, maybe this can be....unused
}

// Handle status request checks local state and reply with the result
// Thread-safe
func (s *RepoStorage) HandleStatus(statusRequest *spec.NameContainer) tlv.RepoStatusResponse {
	log.Info(s, "Handling status request", "status request", statusRequest, "name", statusRequest.Name)
	reply := tlv.RepoStatusResponse{
		Target: statusRequest,
		Status: 200, // TODO: actually do some checking
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
