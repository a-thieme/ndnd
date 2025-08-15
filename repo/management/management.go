package management

import (
	"strconv"
	"sync"
	"time"

	"github.com/named-data/ndnd/repo/auction"
	"github.com/named-data/ndnd/repo/awareness"
	pface "github.com/named-data/ndnd/repo/producer-facing"
	"github.com/named-data/ndnd/repo/storage"
	"github.com/named-data/ndnd/repo/tlv"
	"github.com/named-data/ndnd/repo/types"
	enc "github.com/named-data/ndnd/std/encoding"
	"github.com/named-data/ndnd/std/log"
	"github.com/named-data/ndnd/std/ndn"
)

type EventType string

const (
	EventUnderReplication EventType = "under_replication"
	EventOverReplication  EventType = "over_replication"
	EventDropPartition    EventType = "drop_partition"
	EventWonAuction       EventType = "won_auction"

	// TODO: repo-producer communication
	EventNotifyReplicas EventType = "notify_replicas"
	EventProcessCommand EventType = "process_event"
	EventFetchData      EventType = "fetch_data"

	// status request handlers
	EventExternalStatusRequest EventType = "external_status_request"
	EventInternalStatusRequest EventType = "internal_status_request"
)

// Event represents a Repo system event that needs handling
type Event struct {
	Type      EventType
	Timestamp time.Time
}

// Handler represents an event handler
type Handler func()

// RepoManagement manages all event handlers and coordinates inter-module communication
type RepoManagement struct {
	handlers map[EventType]map[string]bool // check if the handler is registered for the event type
	mutex    sync.RWMutex

	repo *types.RepoShared

	// Module references for coordination
	awareness      *awareness.RepoAwareness
	auction        *auction.AuctionEngine
	storage        *storage.RepoStorage
	producerFacing *pface.RepoProducerFacing
}

func (m *RepoManagement) String() string {
	return "repo-management"
}

// NewRepoManagement creates a new repo management instance
func NewRepoManagement(repo *types.RepoShared, awareness *awareness.RepoAwareness, auction *auction.AuctionEngine, storage *storage.RepoStorage, producerFacing *pface.RepoProducerFacing) *RepoManagement {
	return &RepoManagement{
		handlers:       make(map[EventType]map[string]bool),
		repo:           repo,
		awareness:      awareness,
		auction:        auction,
		storage:        storage,
		producerFacing: producerFacing,
	}
}

// RunHandler runs a handler for an event type and handler ID
// It checks if the handler is already registered for the event type and handler ID
// It's a wrapper around the handler to avoid running the same handler repeatedly
func (m *RepoManagement) RunHandler(eventType EventType, handlerID string, handler Handler) {
	// Check if the handler is already running
	m.mutex.Lock()
	// Initialize the map for the event type if it doesn't exist
	if _, ok := m.handlers[eventType]; !ok {
		m.handlers[eventType] = make(map[string]bool)
	}

	// Check if handler is already running
	if _, ok := m.handlers[eventType][handlerID]; ok {
		log.Debug(m, "Handler already registered", "eventType", eventType, "handlerID", handlerID)
		m.mutex.Unlock()
		return
	}

	// Add the handler to the map
	m.handlers[eventType][handlerID] = true
	m.mutex.Unlock()

	// Run handler
	handler()

	// After handler completes, delete history
	m.mutex.Lock()
	delete(m.handlers[eventType], handlerID)
	m.mutex.Unlock()
}

func (m *RepoManagement) Start() error {
	log.Info(m, "Starting Repo Management")

	// TODO
	m.setupHandlers()

	return nil
}

func (m *RepoManagement) Stop() error {
	log.Info(m, "Stopping Repo Management")

	// TODO
	return nil
}

// setupHandlers sets up all event handlers
// RunHandler arguments are: eventType, handlerID, handler
// (eventType, handlerID) are used to identify any handler. Attempts to register handlers with the same (eventType, handlerID) will be ignored.
// Handlers will be ran in a separate goroutine, so blocking is not a concern
func (m *RepoManagement) setupHandlers() {
	// Awareness handlers
	m.awareness.SetOnUnderReplication(func(partition uint64) {
		go m.RunHandler(EventUnderReplication, strconv.FormatUint(partition, 10), func() { m.UnderReplicationHandler(partition) })
	})

	m.awareness.SetOnOverReplication(func(partition uint64) {
		go m.RunHandler(EventOverReplication, strconv.FormatUint(partition, 10), func() { m.OverReplicationHandler(partition) })
	})

	// Auction handlers
	m.auction.SetOnAuctionWin(func(item string) {
		go m.RunHandler(EventWonAuction, item, func() { m.WonAuctionHandler(item) })
	})

	// Producer message handlers
	m.producerFacing.SetOnNotifyReplicas(func(command *tlv.RepoCommand) {
		go m.RunHandler(EventNotifyReplicas, command.SrcName.Name.String(), func() { m.NotifyReplicasHandler(command) })
	})

	m.producerFacing.SetOnProcessCommand(func(command *tlv.RepoCommand) {
		go m.RunHandler(EventProcessCommand, command.SrcName.Name.String(), func() { m.ProcessCommandHandler(command) })
	})

	// Storage handlers
	m.storage.SetFetchDataHandler(func(name enc.Name) {
		go m.RunHandler(EventFetchData, name.String(), func() { m.FetchDataHandler(name) })
	})

	// Internal communication handlers
	m.producerFacing.SetOnExternalStatusRequest(func(interestHandler *ndn.InterestHandlerArgs, status *tlv.RepoStatus) {
		go m.RunHandler(EventExternalStatusRequest, strconv.FormatUint(status.Nonce, 10), func() { m.ExternalStatusRequestHandler(interestHandler, status) })
	})

	m.producerFacing.SetOnInternalStatusRequest(func(interestHandler *ndn.InterestHandlerArgs, status *tlv.RepoStatus) {
		go m.RunHandler(EventInternalStatusRequest, strconv.FormatUint(status.Nonce, 10), func() { m.InternalStatusRequestHandler(interestHandler, status) })
	})
}
