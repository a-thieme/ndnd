package storage

import (
	"maps"
	"slices"
	"sync"

	enc "github.com/named-data/ndnd/std/encoding"

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
	// both have the same thing
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

func (r *RepoStorage) GetJobs() []*tlv.RepoCommand {
	// TODO: if this is called multiple times between changes in r.jobs, have DoCommand() do the collection (pre-computation)
	r.mutex.Lock()
	defer r.mutex.Unlock()

	// https://stackoverflow.com/questions/13422578/in-go-how-to-get-a-slice-of-values-from-a-map
	return slices.Collect(maps.Values(r.jobs))
}

func (r *RepoStorage) DoingJob(job *tlv.RepoCommand) bool {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	return r.jobs[job.Target] != nil
}

// Thread-safe
func (s *RepoStorage) DoCommand(command *tlv.RepoCommand) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	log.Info(s, "Handling command", "command", command)
	s.jobs[command.Target] = command
	// FIXME: this needs to either consume data or join an svs group
	// FIXME: for testing, maybe this can be....unused?
}

// Handle status request checks local state and reply with the result
// Thread-safe
func (s *RepoStorage) HandleStatus(statusRequest *enc.Name) tlv.RepoStatusResponse {
	log.Info(s, "Handling status request", "status request", statusRequest, "name", statusRequest)
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
