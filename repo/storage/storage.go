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
	jobs map[*enc.Name]*tlv.RepoCommand

	fetchDataHandler func(name *enc.Name)
	joinSyncHandler  func(name *enc.Name, threshold *uint64)
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

func (s *RepoStorage) SetFetchDataHandler(f func(*enc.Name)) {
	s.fetchDataHandler = f
}

func (s *RepoStorage) SetJoinSyncHandler(f func(*enc.Name, *uint64)) {
	s.joinSyncHandler = f
}

// get all jobs that i'm doing
func (r *RepoStorage) GetJobs() []*tlv.RepoCommand {
	// TODO: if this is called multiple times between changes in r.jobs, have DoCommand() do the collection (pre-computation)
	r.mutex.Lock()
	defer r.mutex.Unlock()

	// https://stackoverflow.com/questions/13422578/in-go-how-to-get-a-slice-of-values-from-a-map
	return slices.Collect(maps.Values(r.jobs))
}

// either do or release job, depending on the type of command
func (s *RepoStorage) DoCommand(command *tlv.RepoCommand) {
	log.Info(s, "Handling command", "command", command)
	if command.Type == "LEAVE" || command.Type == "REMOVE" {
		log.Info(s, "stopping command target", command.Target)
		s.ReleaseJob(command)
	} else {
		log.Info(s, "doing command target", command.Target)
		s.addJob(command)
	}
}

// start doing job
func (s *RepoStorage) AddJob(job *tlv.RepoCommand) {
	// FIXME: this needs to either consume data or join a sync group
	log.Info(s, "AddJob", job)
	t := job.Target
	s.mutex.Lock()
	s.jobs[&t] = job
	s.mutex.Unlock()
	if job.Type == "JOIN" {
		log.Debug(s, "joining sync group", t)
		s.joinSyncHandler(&t, &job.SnapshotThreshold)
	} else if job.Type == "INSERT" {
		log.Debug(s, "consuming data", t)
		s.fetchDataHandler(&t)
	} else {
		log.Warn(s, "repo command is of invalid type", job.Type, "and somehow got all the way down to storage")
	}
	s.updateAwareness()
}

// release (stop doing) job
func (s *RepoStorage) ReleaseJob(job *tlv.RepoCommand) {
	log.Info(s, "Releasing job", job)
	if !s.DoingJob(job) {
		log.Warn(s, "tried to release job i'm not doing", job.Target)
		return
	}
	// FIXME: this needs to either remove data or leave a sync group
	s.mutex.Lock()
	s.jobs[&job.Target] = nil
	s.mutex.Unlock()
	t := job.Target
	if job.Type == "LEAVE" {
		log.Debug(s, "leaving sync group", t)
		s.leaveSyncHandler(&t, &job.SnapshotThreshold)
	} else if job.Type == "REMOVE" {
		log.Debug(s, "removing data", t)
		err := s.Remove(t)
		if err != nil {
			log.Warn(s, "tried to remove data but it didn't work", err)
		}
	} else {
		log.Warn(s, "repo command is of invalid type", job.Type, "and somehow got all the way down to storage")
		return // cheap way to not update awareness
	}
	s.updateAwareness()
}

// returns whether the node is doing the job
func (r *RepoStorage) DoingJob(job *tlv.RepoCommand) bool {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	return r.jobs[&job.Target] != nil
}

// Put puts data into the storage
func (s *RepoStorage) Put(name enc.Name, data []byte) (err error) {
	return s.repo.Store.Put(name, data)
}

// Remove removes data from the storage
func (s *RepoStorage) Remove(name enc.Name) (err error) {
	return s.repo.Store.Remove(name)
}
