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
	log.Info(r, "Updating node awareness", "node", r.name, "jobs", jobs)
	r.jobs = jobs
}

func (r *RepoNodeAwareness) Heartbeat(expire time.Duration) {
	log.Info(r, "heartbeat for", "node", r.name, "expires in", expire)
	r.timer.Reset(expire)
	r.status = Up
}
