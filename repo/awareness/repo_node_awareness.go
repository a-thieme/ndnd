package awareness

import (
	"fmt"
	"time"
)

// Local awareness of the state of repo nodes within a cluster
type RepoNodeAwareness struct {
	cluster *RepoClusterAwareness

	// name (identifier) of the node
	name string
	// last time we checked the node
	lastKnown time.Time
	// partitions a node is holding
	partitions []string
}

func (r *RepoNodeAwareness) String() string {
	return fmt.Sprintf("Node Awareness: %s", r.name)
}

// NewRepoNodeAwareness creates a new RepoNodeAwareness instance
// with the given name and initializes the lastKnown time to now.
func (r *RepoNodeAwareness) NewRepoNodeAwareness(name string) *RepoNodeAwareness {
	return &RepoNodeAwareness{
		name:       name,
		lastKnown:  time.Now(),
		partitions: make([]string, 0),
	}
}
