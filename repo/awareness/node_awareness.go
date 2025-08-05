package awareness

import (
	"fmt"
	"time"

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
	// partitions a node is holding
	partitions map[uint64]bool // using map for fast lookup
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
		name:       name,
		lastKnown:  time.Now(),
		partitions: make(map[uint64]bool),
		status:     Up,
	}
}

// SetState updates the state of the node and resets the lastKnown time.
func (r *RepoNodeAwareness) SetState(state NodeStatus) {
	r.status = state
}

// Update updates the node's partitions and resets its state to Up.
func (r *RepoNodeAwareness) Update(partitions map[uint64]bool) {
	log.Info(r, "Updating node awareness", "node", r.name, "partitions", partitions)

	r.status = Up // Reset state to Up when updating partitions
	r.partitions = partitions
	r.lastKnown = time.Now()
}
