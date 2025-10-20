package awareness

import (
	"sync"
	"time"

	"github.com/named-data/ndnd/repo/tlv"
	"github.com/named-data/ndnd/repo/types"
	enc "github.com/named-data/ndnd/std/encoding"
	"github.com/named-data/ndnd/std/log"
)

type RepoAwarenessStore struct {
	mutex sync.RWMutex

	// Node states store
	nodeStates map[string]*RepoNodeAwareness

	// heartbeat
	heartbeatExpiry time.Duration

	// job target to replica count
	jobReplications map[string]int

	// check and handle if job is under or over-replicated
	checkJob func(*tlv.RepoCommand)
}

func NewRepoAwarenessStore(repo *types.RepoShared) *RepoAwarenessStore {
	return &RepoAwarenessStore{
		nodeStates:      make(map[string]*RepoNodeAwareness),
		jobReplications: make(map[string]int),
		heartbeatExpiry: repo.HeartbeatExpiry,
	}
}

func (s *RepoAwarenessStore) String() string {
	return "repo-awareness-store"
}

func (s *RepoAwarenessStore) SetCheckJob(checkJob func(*tlv.RepoCommand)) {
	s.checkJob = checkJob
}

func (s *RepoAwarenessStore) GetReplications(job *tlv.RepoCommand) int {
	log.Debug(s, "getting replications for job", job.Target)
	s.mutex.Lock()
	log.Debug(s, "after mutex lock")
	defer s.mutex.Unlock()
	val := s.jobReplications[job.Target.String()]
	log.Debug(s, "val for replications:", val)
	return val

}

// get node awareness if it exists, otherwise create it
// not thread safe; must hold mutex
func (s *RepoAwarenessStore) getNode(name *enc.Name) *RepoNodeAwareness {
	node := s.nodeStates[name.String()]
	if node == nil {
		log.Info(s, "New node added", "name", name)
		node = NewRepoNodeAwareness(name, s.onHeartbeatExpire)
		s.nodeStates[name.String()] = node
	}
	return node
}

// ProcessHeartbeat sets status to Up and resets the expirationTimer
// Thread-safe
func (s *RepoAwarenessStore) ProcessHeartbeat(name *enc.Name) {
	log.Trace(s, "Processing heartbeat", "publisher", name)
	s.mutex.Lock()
	defer s.mutex.Unlock()

	node := s.getNode(name)
	node.Heartbeat(s.heartbeatExpiry)
}

// ProcessAwarenessUpdate processes an awareness update from a node.
// Thread-safe.
func (s *RepoAwarenessStore) ProcessAwarenessUpdate(update *tlv.AwarenessUpdate) {
	s.mutex.Lock()

	log.Debug(s, "Processing awareness update", update.Node)
	node := s.getNode(&update.Node)

	// map to reduce duplicates
	mapJobsToCheck := map[*tlv.RepoCommand]int{}

	// subtract 1 from jobs node is doing
	for _, value := range node.jobs {
		s.jobReplications[value.Target.String()]--
		mapJobsToCheck[value] = 1
	}
	node.Update(update.ActiveJobs)

	// add 1 to jobs node is now confirmed to do, should be the same as update.ActiveJobs
	for _, value := range node.jobs {
		s.jobReplications[value.Target.String()]++
		mapJobsToCheck[value] = 1
	}
	s.mutex.Unlock()

	// check relevant jobs for replication factor
	for job := range mapJobsToCheck {
		// check if replicated or not
		if s.checkJob == nil {
			panic("checkJob is nil")
		}
		s.checkJob(job)
	}
}

func (s *RepoAwarenessStore) onHeartbeatExpire(rc []*tlv.RepoCommand) {
	log.Info(s, "heartbeat expired, checking jobs", "length", len(rc))
	for _, job := range rc {
		log.Info(s, "now checking", "job", job.Target)
		s.mutex.Lock()
		s.jobReplications[job.Target.String()]--
		s.mutex.Unlock()
		s.checkJob(job)
	}
}
