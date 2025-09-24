package storage

import (
	"errors"
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

	// svs groups
	// groups[target]->user_svs
	groups map[*enc.Name]*UserSvs

	fetchDataHandler func(name *enc.Name)

	updateAwareness func([]*tlv.RepoCommand)
}

// NewRepoStorage creates a new repo storage
func NewRepoStorage(repo *types.RepoShared) *RepoStorage {
	log.Info(nil, "Created Repo Storage")

	return &RepoStorage{
		repo: repo,
		jobs: map[*enc.Name]*tlv.RepoCommand{},
	}
}

func (s *RepoStorage) String() string {
	return "repo-storage"
}

func (s *RepoStorage) SetFetchDataHandler(f func(*enc.Name)) {
	s.fetchDataHandler = f
}

// get all jobs that i'm doing
func (r *RepoStorage) GetJobs() []*tlv.RepoCommand {
	// TODO: if this is called multiple times between changes in r.jobs, have DoCommand() do the collection (pre-computation)
	r.mutex.Lock()
	defer r.mutex.Unlock()

	// https://stackoverflow.com/questions/13422578/in-go-how-to-get-a-slice-of-values-from-a-map
	return slices.Collect(maps.Values(r.jobs))
}

// start doing job
// TODO: check to see if you have the availability for this
func (s *RepoStorage) AddJob(job *tlv.RepoCommand) error {
	log.Info(s, "AddJob", job)
	// FIXME: this needs to either consume data or join a sync group
	t := job.Target
	s.mutex.Lock()
	s.jobs[&t] = job
	s.mutex.Unlock()
	if job.Type == "JOIN" {
		log.Debug(s, "joining sync group", t)
		s.joinSync(s.repo, job)
	} else if job.Type == "INSERT" {
		log.Debug(s, "consuming data", t)
		log.Warn(s, "should fetch data here but not implemented")
		// s.fetchDataHandler(&t)
	} else {
		msg := "repo command is of invalid type and somehow got all the way down to storage"
		log.Warn(s, "repo command is of invalid type", job.Type, "and somehow got all the way down to storage")
		return errors.New(msg)
	}
	return nil
}

// release (stop doing) job
func (s *RepoStorage) ReleaseJob(job *tlv.RepoCommand) error {
	log.Info(s, "Releasing job", job)
	if !s.DoingJob(job) {
		msg := "tried to release job i'm not doing"
		log.Warn(s, msg, job.Target)
		return errors.New(msg)
	}
	// FIXME: this needs to either remove data or leave a sync group
	s.mutex.Lock()
	s.jobs[&job.Target] = nil
	s.mutex.Unlock()
	t := job.Target
	if job.Type == "LEAVE" {
		log.Debug(s, "leaving sync group", t)
		s.leaveSync(job)
	} else if job.Type == "REMOVE" {
		log.Debug(s, "removing data", t)
		err := s.remove(job.Target)
		if err != nil {
			msg := "tried to remove data but it didn't work"
			log.Warn(s, msg, err)
			return errors.New(msg)
		}
	} else {
		msg := "repo command is of invalid type and somehow got all the way down to storage"
		log.Warn(s, "repo command is of invalid type", job.Type, "and somehow got all the way down to storage")
		return errors.New(msg)
	}
	return nil
}

// returns whether the node is doing the job
func (r *RepoStorage) DoingJob(job *tlv.RepoCommand) bool {
	log.Debug(r, "asked for DoingJob()")
	r.mutex.Lock()
	log.Debug(r, "after first mutex")
	defer r.mutex.Unlock()
	log.Debug(r, "after second mutex")
	value, contains := r.jobs[&job.Target]
	log.Debug(r, "after grabbed")
	if contains {
		log.Debug(r, "contains key")
		log.Debug(r, value.Target.String())
	} else {
		log.Debug(r, "does not contain key")
	}
	log.Debug(r, "returning from DoingJob()")
	return contains
}

func (r *RepoStorage) joinSync(repo *types.RepoShared, job *tlv.RepoCommand) {
	log.Info(r, "joining sync group", job.Target)
	// NewUserSvs(r.repo, job)
}

func (r *RepoStorage) leaveSync(command *tlv.RepoCommand) {
	log.Info(r, "leaving sync group", command.Target)
}

// Put puts data into the storage
func (r *RepoStorage) put(name enc.Name, data []byte) (err error) {
	log.Info(r, "leaving sync group", name)
	return r.repo.Store.Put(name, data)
}

// Remove removes data from the storage
func (r *RepoStorage) remove(name enc.Name) (err error) {
	log.Info(r, "leaving sync group", name)
	return r.repo.Store.Remove(name)
}
