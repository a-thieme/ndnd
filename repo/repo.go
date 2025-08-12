package repo

import (
	"strconv"
	"sync"

	"github.com/named-data/ndnd/repo/auction"
	"github.com/named-data/ndnd/repo/awareness"
	"github.com/named-data/ndnd/repo/management"
	facing "github.com/named-data/ndnd/repo/producer-facing"
	"github.com/named-data/ndnd/repo/storage"
	"github.com/named-data/ndnd/repo/types"

	enc "github.com/named-data/ndnd/std/encoding"
	"github.com/named-data/ndnd/std/engine"
	"github.com/named-data/ndnd/std/engine/basic"
	"github.com/named-data/ndnd/std/log"
	"github.com/named-data/ndnd/std/ndn"
	"github.com/named-data/ndnd/std/object"
	local_storage "github.com/named-data/ndnd/std/object/storage"
	sec "github.com/named-data/ndnd/std/security"
	"github.com/named-data/ndnd/std/security/keychain"
	"github.com/named-data/ndnd/std/security/trust_schema"
)

type Repo struct {
	groupConfig *RepoGroupConfig
	nodeConfig  *RepoNodeConfig

	engine ndn.Engine
	store  ndn.Store
	client ndn.Client

	awareness  *awareness.RepoAwareness
	storage    *storage.RepoStorage
	auction    *auction.AuctionEngine
	facing     *facing.RepoProducerFacing
	management *management.RepoManagement

	groupsSvs map[string]*RepoSvs
	mutex     sync.Mutex
}

func NewRepo(groupConfig *RepoGroupConfig, nodeConfig *RepoNodeConfig) *Repo {
	return &Repo{
		groupConfig: groupConfig,
		nodeConfig:  nodeConfig,
		groupsSvs:   make(map[string]*RepoSvs),
	}
}

func (r *Repo) String() string {
	return "repo"
}

func (r *Repo) Start() (err error) {
	log.Info(r, "Starting NDN Data Repository", "group", r.groupConfig, "node", r.nodeConfig)

	// Make object store database
	r.store, err = local_storage.NewBadgerStore(r.nodeConfig.StorageDir + "/badger")
	if err != nil {
		return err
	}

	// Create NDN engine
	r.engine = engine.NewBasicEngine(engine.NewDefaultFace())
	r.setupEngineHook()
	if err = r.engine.Start(); err != nil {
		return err
	}

	// TODO: Trust config may be specific to application
	// This may need us to make a client for each app
	kc, err := keychain.NewKeyChain(r.nodeConfig.KeyChainUri, r.store)
	if err != nil {
		return err
	}

	// TODO: specify a real trust schema
	// TODO: handle app-specific case
	schema := trust_schema.NewNullSchema()

	// TODO: handle app-specific case
	anchors := r.nodeConfig.TrustAnchorNames()

	// Create trust config
	trust, err := sec.NewTrustConfig(kc, schema, anchors)
	if err != nil {
		return err
	}

	// Attach data name as forwarding hint to cert Interests
	// TODO: what to do if this is app dependent? Separate client for each app?
	trust.UseDataNameFwHint = true

	// Start NDN Object API client
	// r.client = object.NewClient(r.engine, r.store, trust)
	// TODO: temporarily disable trust
	r.client = object.NewClient(r.engine, r.store, nil)
	if err := r.client.Start(); err != nil {
		return err
	}

	// Create repo shared
	shared := types.NewRepoShared(r.groupConfig.RepoNameN,
		r.nodeConfig.NodeNameN,
		r.groupConfig.NumPartitions,
		r.groupConfig.NumReplicas,
		r.groupConfig.HeartbeatInterval,
		r.groupConfig.HeartbeatExpiry,
		r.client,
		r.store,
		r.engine,
	)

	// Attach managmemt interest handler
	commandHandlerPreifx := r.groupConfig.RepoNameN.Append(enc.NewGenericComponent("cmd")) // TODO: unify command handler prefix
	if err := r.client.AttachCommandHandler(commandHandlerPreifx, r.onMgmtCmd); err != nil {
		return err
	}
	r.client.AnnouncePrefix(ndn.Announcement{
		Name:   commandHandlerPreifx,
		Expose: true,
	})

	// Create repo awareness
	r.awareness = awareness.NewRepoAwareness(shared)
	if err := r.awareness.Start(); err != nil {
		return err
	}

	// Create repo storage
	r.storage = storage.NewRepoStorage(shared)

	// Create repo auction
	// TODO: currently we use trivial get bid & on win, etc.
	testGetBid := func(name string) int {
		return 100
	}
	r.auction = auction.NewAuctionEngine(shared, r.awareness.GetOnlineNodes, testGetBid, r.wonAuction)
	if err := r.auction.Start(); err != nil {
		return err
	}
	log.Info(r, "AuctionEngine started", "auction", r.auction)

	// Create repo facing
	r.facing = facing.NewProducerFacing(shared)
	if err := r.facing.Start(); err != nil {
		return err
	}

	// Create repo management
	r.management = management.NewRepoManagement(shared, r.awareness, r.auction, r.storage, r.facing)
	if err := r.management.Start(); err != nil {
		return err
	}

	return nil
}

func (r *Repo) Stop() error {
	log.Info(r, "Stopping NDN Data Repository")

	for _, svs := range r.groupsSvs {
		svs.Stop()
	}
	clear(r.groupsSvs)

	r.client.WithdrawPrefix(r.groupConfig.RepoNameN, nil)
	if err := r.client.DetachCommandHandler(r.groupConfig.RepoNameN); err != nil {
		log.Warn(r, "Failed to detach command handler", "err", err)
	}

	// Stop NDN Object API client
	if r.client != nil {
		if err := r.client.Stop(); err != nil {
			log.Warn(r, "Failed to stop client", "err", err)
		}
	}

	// Stop NDN engine
	if r.engine != nil {
		if err := r.engine.Stop(); err != nil {
			log.Warn(r, "Failed to stop engine", "err", err)
		}
	}

	// Stop awareness
	if r.awareness != nil {
		if err := r.awareness.Stop(); err != nil {
			log.Warn(r, "Failed to stop awareness", "err", err)
		}
	}

	// Stop storage
	if r.storage != nil {
		if err := r.storage.Close(); err != nil {
			log.Warn(r, "Failed to close storage", "err", err)
		}
	}

	// Stop auction
	if r.auction != nil {
		if err := r.auction.Stop(); err != nil {
			log.Warn(r, "Failed to stop auction", "err", err)
		}
	}

	// Stop facing
	if r.facing != nil {
		if err := r.facing.Stop(); err != nil {
			log.Warn(r, "Failed to stop facing", "err", err)
		}
	}

	// Stop management
	if r.management != nil {
		if err := r.management.Stop(); err != nil {
			log.Warn(r, "Failed to stop management", "err", err)
		}
	}

	return nil
}

// setupEngineHook sets up the hook to persist all data.
func (r *Repo) setupEngineHook() {
	r.engine.(*basic.Engine).OnDataHook = func(data ndn.Data, raw enc.Wire, sigCov enc.Wire) error {
		// This is very hacky, improve if possible.
		// Assume that if there is a version it is the second-last component.
		// We might not want to store non-versioned data anyway (?)
		if ver := data.Name().At(-2); ver.IsVersion() {
			log.Trace(r, "Storing data", "name", data.Name())
			return r.store.Put(data.Name(), raw.Join())
		} else {
			log.Trace(r, "Ignoring non-versioned data", "name", data.Name())
		}
		return nil
	}
}

func (r *Repo) wonAuction(item string) {
	log.Info(r, "Won auction for item", "item", item)
	partitionId, _ := strconv.ParseUint(item, 10, 64)

	err := r.storage.RegisterPartition(partitionId)
	if err != nil {
		log.Warn(r, "Failed to register partition", "id", partitionId, "err", err)
		return
	}

	log.Info(r, "Won partition", "id", partitionId)
	r.awareness.AddLocalPartition(partitionId)
}
