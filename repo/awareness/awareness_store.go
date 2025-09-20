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
	nodeStates map[*enc.Name]*RepoNodeAwareness

	// heartbeat
	heartbeatExpiry time.Duration

	// job target to replica count
	jobReplications map[*tlv.RepoCommand]int

	// Callbacks for partition management
	underReplicationHandler func(*tlv.RepoCommand)
	overReplicationHandler  func(*tlv.RepoCommand)
}

func NewRepoAwarenessStore(repo *types.RepoShared) *RepoAwarenessStore {
	return &RepoAwarenessStore{
		nodeStates:      make(map[*enc.Name]*RepoNodeAwareness),
		jobReplications: make(map[*tlv.RepoCommand]int),
		heartbeatExpiry: repo.HeartbeatExpiry,
	}
}

func (s *RepoAwarenessStore) String() string {
	return "repo-awareness-store"
}

func (s *RepoAwarenessStore) GetReplications(job *tlv.RepoCommand) int {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.jobReplications[job]

}

// get node awareness if it exists, otherwise create it
func (s *RepoAwarenessStore) getNode(name *enc.Name) *RepoNodeAwareness {
	node := s.nodeStates[name]
	if node == nil {
		log.Info(s, "New node added", "name", name)
		node = NewRepoNodeAwareness(name, s.onHeartbeatExpire)
	}
	return node
}

// ProcessHeartbeat sets status to Up and resets the expirationTimer
// Thread-safe
func (s *RepoAwarenessStore) ProcessHeartbeat(name *enc.Name) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	log.Info(s, "Processing heartbeat", "publisher", name)

	node := s.getNode(name)
	node.Heartbeat(s.heartbeatExpiry)
}

// ProcessAwarenessUpdate processes an awareness update from a node.
// Thread-safe.
func (s *RepoAwarenessStore) ProcessAwarenessUpdate(update *tlv.AwarenessUpdate) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	log.Info(s, "Processing awareness update", "publisher", update.Node)
	name := update.Node
	node := s.getNode(name)

	// map to reduce duplicates
	mapJobsToCheck := map[*tlv.RepoCommand]int{}

	// subtract 1 from jobs node is doing
	for _, value := range node.jobs {
		s.jobReplications[value]--
		mapJobsToCheck[value] = 1
	}
	node.Update(update.ActiveJobs)

	// add 1 to jobs node is now confirmed to do
	for _, value := range node.jobs {
		s.jobReplications[value]++
		mapJobsToCheck[value] = 1
	}

	// check relevant jobs for replication factor
	for job, _ := range mapJobsToCheck {
		// check if replicated or not
		s.checkJob(job)
	}
}

func (s *RepoAwarenessStore) onHeartbeatExpire(rc []*tlv.RepoCommand) {
	for _, job := range rc {
		s.checkJob(job)
	}
}
