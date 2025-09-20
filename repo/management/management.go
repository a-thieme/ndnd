package management

import (
	"sync"

	"github.com/named-data/ndnd/repo/awareness"
	"github.com/named-data/ndnd/repo/distribution"
	pface "github.com/named-data/ndnd/repo/producer-facing"
	"github.com/named-data/ndnd/repo/storage"
	"github.com/named-data/ndnd/repo/types"
)

// RepoManagement manages all event handlers and coordinates inter-module communication
type RepoManagement struct {
	mutex sync.RWMutex

	repo *types.RepoShared

	// Module references for coordination
	awareness      *awareness.RepoAwareness
	auction        *auction.AuctionEngine
	storage        *storage.RepoStorage
	producerFacing *pface.RepoProducerFacing
	commands       *awareness.Commands
}

func (m *RepoManagement) String() string {
	return "repo-management"
}

// NewRepoManagement creates a new repo management instance
func NewRepoManagement(repo *types.RepoShared, awareness *awareness.RepoAwareness, auction *auction.AuctionEngine, storage *storage.RepoStorage, producerFacing *pface.RepoProducerFacing, commands *awareness.Commands) *RepoManagement {
	return &RepoManagement{
		repo:           repo,
		awareness:      awareness,
		auction:        auction,
		storage:        storage,
		producerFacing: producerFacing,
		commands:       commands,
	}
}
