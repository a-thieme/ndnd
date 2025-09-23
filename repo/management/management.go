package management

import (
	"sync"

	"github.com/named-data/ndnd/repo/awareness"
	"github.com/named-data/ndnd/repo/distribution"
	pface "github.com/named-data/ndnd/repo/producer-facing"
	"github.com/named-data/ndnd/repo/storage"
	"github.com/named-data/ndnd/repo/tlv"
	"github.com/named-data/ndnd/repo/types"
)

// RepoManagement manages all event handlers and coordinates inter-module communication
type RepoManagement struct {
	mutex sync.RWMutex

	repo *types.RepoShared

	// Module references for coordination
	awareness      *awareness.RepoAwareness
	auction        *distribution.AuctionEngine
	storage        *storage.RepoStorage
	producerFacing *pface.RepoProducerFacing
	commands       *awareness.Commands
	timeBased      *distribution.TimeBased

	overReplication  func(command *tlv.RepoCommand)
	underReplication func(command *tlv.RepoCommand)
	goodReplication  func(command *tlv.RepoCommand)
}

func (m *RepoManagement) String() string {
	return "repo-management"
}

// NewRepoManagement creates a new repo management instance
func NewRepoManagement(repo *types.RepoShared, awareness *awareness.RepoAwareness, storage *storage.RepoStorage, producerFacing *pface.RepoProducerFacing) *RepoManagement {
	rm := &RepoManagement{
		repo:           repo,
		awareness:      awareness,
		storage:        storage,
		producerFacing: producerFacing,
		timeBased:      distribution.NewTimeBased(),
	}
	// // Create repo auction
	// rm.auction = distribution.NewAuctionEngine(
	// 	repo,
	// 	awareness.GetOnlineNodes,
	// 	rm.GetAvailability,
	// 	rm.AucDoJob,
	// )
	//rm.auction.Start()

	// NOTE: This file sets callbacks to connect modules to management
	// in addition, it sets the managemnt's decisions for what to do if a job is under, over, or well-replicated
	// these decisions should be changed here for experiments. inter_module_functions should be standard.

	// NOTE: these should in the order they will execute

	// connect producer ingress to management
	rm.producerFacing.SetCommandHandler(rm.OnNewCommand)

	// connect storage to management
	rm.storage.SetFetchDataHandler(rm.fetchData)
	rm.storage.SetJoinSyncHandler(rm.joinSync)

	// connect commands to management
	rm.commands.SetCheckJob(rm.CheckJob)

	// connect management to timers
	rm.setUnder(rm.timeBased.Under)
	rm.setOver(rm.timeBased.Over)
	rm.setGood(rm.timeBased.Good)

	// connect timers to management
	rm.timeBased.SetDoJob(rm.DoJob)
	rm.timeBased.SetAbility(rm.GetAvailability)
	rm.timeBased.SetRelease(rm.ReleaseJob)

	return rm
}

func (rm *RepoManagement) setUnder(f func(command *tlv.RepoCommand)) {
	rm.underReplication = f
}

func (rm *RepoManagement) setOver(f func(command *tlv.RepoCommand)) {
	rm.underReplication = f
}

func (rm *RepoManagement) setGood(f func(command *tlv.RepoCommand)) {
	rm.goodReplication = f
}
