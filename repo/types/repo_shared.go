package types

import (
	"time"

	enc "github.com/named-data/ndnd/std/encoding"
	"github.com/named-data/ndnd/std/ndn"
	"github.com/named-data/ndnd/std/security/keychain"
)

// RepoShared is the shared repo resource that contains repo configurations and other shared objects
type RepoShared struct {
	RepoNameN enc.Name
	NodeNameN enc.Name

	NumReplicas       int
	HeartbeatInterval time.Duration
	HeartbeatExpiry   time.Duration

	Client ndn.Client
	Store  ndn.Store
	Engine ndn.Engine

	Keychain ndn.KeyChain
}

func (r *RepoShared) String() string {
	return "repo-shared-resource"
}

// NewRepoShared creates a new RepoShared object
func NewRepoShared(repoNameN enc.Name, nodeNameN enc.Name, numReplicas int, heartbeatInterval float64, heartbeatExpiry float64, client ndn.Client, store ndn.Store, engine ndn.Engine) *RepoShared {
	kc, err := keychain.NewKeyChainDir("./keychain", store)
	if err != nil {
		panic(err)
	}
	return &RepoShared{
		RepoNameN:         repoNameN,
		NodeNameN:         nodeNameN,
		NumReplicas:       numReplicas,
		HeartbeatInterval: time.Duration(heartbeatInterval) * time.Second,
		HeartbeatExpiry:   time.Duration(heartbeatExpiry) * time.Second,
		Client:            client,
		Store:             store,
		Engine:            engine,
		Keychain:          kc,
	}
}
