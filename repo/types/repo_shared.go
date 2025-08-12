package types

import (
	"time"

	enc "github.com/named-data/ndnd/std/encoding"
	"github.com/named-data/ndnd/std/ndn"
)

// RepoShared is the shared repo resource that contains repo configurations and other shared objects
type RepoShared struct {
	RepoNameN enc.Name
	NodeNameN enc.Name

	NumPartitions     int
	NumReplicas       int
	HeartbeatInterval time.Duration
	HeartbeatExpiry   time.Duration

	Client ndn.Client
	Store  ndn.Store
	Engine ndn.Engine
}

func (r *RepoShared) String() string {
	return "repo-shared-resource"
}

// NewRepoShared creates a new RepoShared object
func NewRepoShared(repoNameN enc.Name, nodeNameN enc.Name, numPartitions int, numReplicas int, heartbeatInterval float64, heartbeatExpiry float64, client ndn.Client, store ndn.Store, engine ndn.Engine) *RepoShared {
	return &RepoShared{
		RepoNameN:         repoNameN,
		NodeNameN:         nodeNameN,
		NumPartitions:     numPartitions,
		NumReplicas:       numReplicas,
		HeartbeatInterval: time.Duration(heartbeatInterval) * time.Second,
		HeartbeatExpiry:   time.Duration(heartbeatExpiry) * time.Second,
		Client:            client,
		Store:             store,
		Engine:            engine,
	}
}
