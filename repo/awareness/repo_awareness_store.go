package awareness

import (
	"sync"
	"time"

	"github.com/bits-and-blooms/bitset"

	"github.com/named-data/ndnd/repo/tlv"
	"github.com/named-data/ndnd/std/log"
)

type RepoAwarenessStore struct {
	mutex sync.RWMutex

	// Node states store
	nodeStates      map[string]*RepoNodeAwareness
	expirationTimer map[string]*time.Timer

	// Replication and partition management
	replicaCounts []int          // partition ID to replica count
	underRepMask  *bitset.BitSet // bit set for under-replicated partitions

	// Callbacks for node state changes
	onNodeUp        func(*RepoNodeAwareness)
	onNodeFailed    func(*RepoNodeAwareness)
	onNodeForgotten func(*RepoNodeAwareness)
}

func NewRepoAwarenessStore() *RepoAwarenessStore {
	return &RepoAwarenessStore{
		nodeStates:      make(map[string]*RepoNodeAwareness),
		expirationTimer: make(map[string]*time.Timer),
		replicaCounts:   make([]int, NumPartitions),
		underRepMask:    bitset.New(NumPartitions),
	}
}

func (s *RepoAwarenessStore) String() string {
	return "repo-awareness-store"
}

// GetNode retrieves a node's awareness by its name.
// Returns nil if the node does not exist.
func (s *RepoAwarenessStore) GetNode(name string) *RepoNodeAwareness {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	node := s.nodeStates[name]
	return node
}

func (s *RepoAwarenessStore) ProcessUpdate(update *tlv.AwarenessUpdate) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	log.Info(s, "Processing awareness update", "publisher", update.NodeName)
	name := update.NodeName
	node := s.nodeStates[name]

	// Cancel existing expiration timer if it exists
	if timer, exists := s.expirationTimer[name]; exists {
		timer.Stop()
		// don't delete the timer as the node isn't forgotten yet, so it's likely to be reused
	}

	if node == nil { // initialize the node state
		node = NewRepoNodeAwareness(name)
		s.nodeStates[name] = node
		log.Info(s, "New node added", "name", name)
	}

	// Update the node's partitions and reset its state to Up
	s.UpdateNodePartitions(node, update.Partitions)
	if node.state != Up {
		// Mark the node as Up
		s.MarkNodeUp(node)
	}

	// Set new expiration timer
	s.expirationTimer[name] = time.AfterFunc(HeartbeatExpiry, func() {
		s.MarkNodeFailed(node)
	})
}

// Callbacks for node state changes
func (s *RepoAwarenessStore) SetOnNodeUp(callback func(*RepoNodeAwareness)) {
	s.onNodeUp = callback
}

func (s *RepoAwarenessStore) SetOnNodeFailed(callback func(*RepoNodeAwareness)) {
	s.onNodeFailed = callback
}

func (s *RepoAwarenessStore) SetOnNodeForgotten(callback func(*RepoNodeAwareness)) {
	s.onNodeForgotten = callback
}

// MarkNodeUp marks a node as Up and updates its state.
// Thread safety is handled by the caller
func (s *RepoAwarenessStore) MarkNodeUp(node *RepoNodeAwareness) {
	log.Info(s, "Marked node as Up", "node", node.name)

	node.state = Up
	if s.onNodeUp != nil {
		s.onNodeUp(node)
	}

	// Update partition replication counts
	for partition := range node.partitions {
		log.Info(s, "Incrementing replica count for partition", "partition", partition, "node", node.name)

		s.replicaCounts[partition]++
		if s.replicaCounts[partition] >= NumReplicas {
			s.underRepMask.Clear(partition)
		}
	}
}

// MarkNodeFailed marks a node as failed and updates its state.
// Thread safety is handled by the caller
func (s *RepoAwarenessStore) MarkNodeFailed(node *RepoNodeAwareness) {
	log.Info(s, "Marked node as Failed", "node", node.name)

	node.state = Failed
	if s.onNodeFailed != nil {
		s.onNodeFailed(node)
	}

	// Update partition replication counts
	for partition := range node.partitions {
		log.Info(s, "Decrementing replica count for partition", "partition", partition, "node", node.name)

		s.replicaCounts[partition]--
		if s.replicaCounts[partition] < NumReplicas {
			s.underRepMask.Set(partition)
			// TODO: handle auction for under-replicated partitions
		}
	}

	// TODO: in the future, handle forgotten state. For now, we keep the node in the store
}

// MarkNodeForgotten marks a node as forgotten and updates its state.
// Thread safety is handled by the caller
// wip: not currently used
func (s *RepoAwarenessStore) MarkNodeForgotten(node *RepoNodeAwareness) {
	log.Info(s, "Marked node as Forgotten", "node", node.name)

	node.state = Forgotten
	if s.onNodeForgotten != nil {
		s.onNodeForgotten(node)
	}

	// Remove the node from the store
	delete(s.nodeStates, node.name)
	if timer, exists := s.expirationTimer[node.name]; exists {
		timer.Stop()
		delete(s.expirationTimer, node.name)
	}
}

// TODO: this struct should also handle partition underreplication scenario, probably through handlers
// Thread safety is handled by the caller
// TODO: see tlv/definitions.go for the AwarenessUpdate struct definition (and why it's a sequence)
func (s *RepoAwarenessStore) UpdateNodePartitions(node *RepoNodeAwareness, partitions map[uint]bool) {
	log.Info(s, "Updating node partitions", "node", node.name, "partitions", partitions)

	// Update partition replication counts
	for partition := range node.partitions {
		if !partitions[partition] {
			log.Info(s, "Decrementing replica count for partition", "partition", partition, "node", node.name)
			s.replicaCounts[partition]--
			if s.replicaCounts[partition] < NumReplicas {
				s.underRepMask.Set(partition)
				// TODO: handle auction for under-replicated partitions
			}
		}
	}

	for partition := range partitions {
		if !node.partitions[partition] {
			log.Info(s, "Incrementing replica count for partition", "partition", partition, "node", node.name)
			s.replicaCounts[partition]++
			if s.replicaCounts[partition] >= NumReplicas {
				s.underRepMask.Clear(partition)
			}
		}
	}

	// Update the node's partitions
	node.partitions = partitions
}
