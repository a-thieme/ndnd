package awareness

import (
	"fmt"
	"time"

	"github.com/named-data/ndnd/repo/tlv"
	enc "github.com/named-data/ndnd/std/encoding"
	"github.com/named-data/ndnd/std/log"
)

type NodeStatus int

const (
	Up NodeStatus = iota
	Down
)

// Local awareness of the state of repo nodes within a cluster
type RepoNodeAwareness struct {
	name   *enc.Name
	jobs   []*tlv.RepoCommand
	status NodeStatus
	timer  *time.Timer

	expiryFunc func([]*tlv.RepoCommand)
}

func (r *RepoNodeAwareness) String() string {
	return fmt.Sprintf("Node Awareness: %s", r.name)
}

// NewRepoNodeAwareness creates a new RepoNodeAwareness instance
// with the given name and initializes the lastKnown time to now.
func NewRepoNodeAwareness(name *enc.Name, expiryFunc func([]*tlv.RepoCommand)) *RepoNodeAwareness {
	rna := &RepoNodeAwareness{
		name:       name,
		jobs:       []*tlv.RepoCommand{},
		expiryFunc: expiryFunc,
	}
	rna.timer = time.AfterFunc(0, func() {
		rna.status = Down
		rna.expiryFunc(rna.jobs)
	})
	return rna
}

// Update updates the node's jobs and resets its state to Up.
func (r *RepoNodeAwareness) Update(jobs []*tlv.RepoCommand) {
	log.Debug(r, "Updating node awareness", "node", r.name, "jobs", jobs)
	if r.jobs == nil {
		log.Warn(r, "r.jobs is nil for", r.name)
	}

	log.Debug(r, "updating local state for node", r.name, "jobs", jobs)

	r.jobs = jobs
	log.Debug(r, "end of Update()")
}

func (r *RepoNodeAwareness) Heartbeat(expire time.Duration) {
	log.Trace(r, "heartbeat for node", r.name, "expires in", expire)
	r.timer.Reset(expire)
	r.status = Up
}
