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
	jobs map[string]*tlv.RepoCommand

	// svs groups
	// groups[target]->user_svs
	groups map[string]*UserSvs

	fetchDataHandler func(name *enc.Name)

	updateAwareness func([]*tlv.RepoCommand)
}

// NewRepoStorage creates a new repo storage
func NewRepoStorage(repo *types.RepoShared) *RepoStorage {
	log.Info(nil, "Created Repo Storage")

	return &RepoStorage{
		repo: repo,
		jobs: map[string]*tlv.RepoCommand{},
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

	s.jobs[t.String()] = job
	s.mutex.Unlock()
	log.Trace(s, "after length of getjobs:", len(s.GetJobs()))
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
	gj := s.GetJobs()
	l := len(gj)
	log.Trace(s, "length of getjobs:", l)
	if l >= 1 {
		log.Trace(s, "entry", gj[0])
		if l >= 2 {
			log.Trace(s, "entry", gj[1])
		}
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
	s.jobs[job.Target.String()] = nil
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
// FIXME: this function doesn't work
func (r *RepoStorage) DoingJob(job *tlv.RepoCommand) bool {
	log.Debug(r, "asked whether doing job", job.Target.String())
	r.mutex.Lock()
	defer r.mutex.Unlock()
	_, contains := r.jobs[job.Target.String()]
	log.Debug(r, "returning from DoingJob() with value", contains)
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
