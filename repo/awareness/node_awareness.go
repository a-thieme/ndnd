package awareness

import (
	"fmt"
	"time"

	"github.com/named-data/ndnd/repo/tlv"
	"github.com/named-data/ndnd/std/log"
)

type NodeStatus int

const (
	Up NodeStatus = iota
	Failed
	Forgotten // Node is forgotten and should not be considered for operations
)

// Local awareness of the state of repo nodes within a cluster
type RepoNodeAwareness struct {
	// name (identifier) of the node
	name string
	// last time we checked the node
	lastKnown time.Time
	// jobs a node is doing
	jobs []*tlv.RepoCommand
	// status of the node
	status NodeStatus
}

func (r *RepoNodeAwareness) String() string {
	return fmt.Sprintf("Node Awareness: %s", r.name)
}

// NewRepoNodeAwareness creates a new RepoNodeAwareness instance
// with the given name and initializes the lastKnown time to now.
func NewRepoNodeAwareness(name string) *RepoNodeAwareness {
	return &RepoNodeAwareness{
		name:      name,
		lastKnown: time.Now(),
		jobs:      make(map[uint64]bool),
		status:    Up,
	}
}

// SetState updates the state of the node and resets the lastKnown time.
func (r *RepoNodeAwareness) SetState(state NodeStatus) {
	r.status = state
}

// Update updates the node's jobs and resets its state to Up.
func (r *RepoNodeAwareness) Update(jobs map[uint64]bool) {
	log.Info(r, "Updating node awareness", "node", r.name, "jobs", jobs)

	r.status = Up // Reset state to Up when updating jobs
	r.jobs = jobs
	r.lastKnown = time.Now()
}
