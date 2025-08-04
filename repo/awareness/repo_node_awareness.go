package awareness

import (
	"fmt"
	"time"

	"github.com/named-data/ndnd/std/log"
)

type NodeState int

const (
	Up NodeState = iota
	PFailed
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
	partitions map[uint]bool // using map for fast lookup
	// state of the node
	state NodeState
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
		partitions: make(map[uint]bool),
		state:      Up,
	}
}

// SetState updates the state of the node and resets the lastKnown time.
func (r *RepoNodeAwareness) SetState(state NodeState) {
	r.state = state
}

// Update updates the node's partitions and resets its state to Up.
func (r *RepoNodeAwareness) Update(partitions map[uint]bool) {
	log.Info(r, "Updating node awareness", "node", r.name, "partitions", partitions)

	r.state = Up // Reset state to Up when updating partitions
	r.partitions = partitions
	r.lastKnown = time.Now()
}

func (r *RepoNodeAwareness) SetStateUp() {
	r.state = Up
	r.lastKnown = time.Now()
}
