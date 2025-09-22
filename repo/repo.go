package repo

import (
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
	facing     *facing.RepoProducerFacing
	management *management.RepoManagement
}

func NewRepo(groupConfig *RepoGroupConfig, nodeConfig *RepoNodeConfig) *Repo {
	return &Repo{
		groupConfig: groupConfig,
		nodeConfig:  nodeConfig,
	}
}

func (r *Repo) String() string {
	return "repo"
}

// first thing called
func (r *Repo) Start() (err error) {
	log.Info(r, "Starting NDN Data Repository", "group", r.groupConfig, "node", r.nodeConfig)

	// Make object store database
	log.Debug(r, "Creating badger store")
	r.store, err = local_storage.NewBadgerStore(r.nodeConfig.StorageDir + "/badger")
	if err != nil {
		return err
	}

	// Create NDN engine
	log.Debug(r, "Creating engine")
	r.engine = engine.NewBasicEngine(engine.NewDefaultFace())
	r.setupEngineHook()
	log.Debug(r, "start engine")
	if err = r.engine.Start(); err != nil {
		return err
	}

	log.Debug(r, "new keychain")
	kc, err := keychain.NewKeyChain(r.nodeConfig.KeyChainUri, r.store)
	if err != nil {
		return err
	}

	log.Debug(r, "here")
	// TODO: specify a real trust schema
	schema := trust_schema.NewNullSchema()

	// testbed anchor, necessary for validating commands
	anchors := r.nodeConfig.TrustAnchorNames()

	// Create trust config
	trust, err := sec.NewTrustConfig(kc, schema, anchors)
	if err != nil {
		return err
	}

	log.Debug(r, "here")
	// Attach data name as forwarding hint to cert Interests
	trust.UseDataNameFwHint = true

	// Start NDN Object API client
	// TODO: enable trust
	r.client = object.NewClient(r.engine, r.store, nil)
	if err := r.client.Start(); err != nil {
		return err
	}

	// Create repo shared
	shared := types.NewRepoShared(
		r.groupConfig.RepoNameN,
		r.nodeConfig.NodeNameN,
		r.groupConfig.NumReplicas,
		r.groupConfig.HeartbeatInterval,
		r.groupConfig.HeartbeatExpiry,
		r.client,
		r.store,
		r.engine,
	)

	// // Create repo awareness
	// r.awareness = awareness.NewRepoAwareness(shared)
	// if err := r.awareness.Start(); err != nil {
	// 	return err
	// }
	//
	// log.Debug(r, "here")
	// // Create repo storage
	// r.storage = storage.NewRepoStorage(shared)

	// Create producer-facing
	r.facing = facing.NewProducerFacing(shared)
	if err := r.facing.Start(); err != nil {
		return err
	}

	// Create repo management
	r.management = management.NewRepoManagement(shared, r.awareness, r.storage, r.facing)

	return nil
}

func (r *Repo) Stop() error {
	log.Info(r, "Stopping NDN Data Repository")
	// TODO: make sure to stop svs groups, but this can be done at a lower level

	r.client.WithdrawPrefix(r.groupConfig.RepoNameN, nil)
	if err := r.client.DetachCommandHandler(r.groupConfig.RepoNameN); err != nil {
		// FIXME: "invalid value for prefix"
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

	// FIXME: somehow stop auction things if they need to be (I don't think they need it)

	// Stop facing
	if r.facing != nil {
		if err := r.facing.Stop(); err != nil {
			log.Warn(r, "Failed to stop facing", "err", err)
		}
	}

	return nil
}

// setupEngineHook sets up the hook to persist all recieved data.
// probably not necessary if lower levels handle it
// TODO: see if this is necesssary
func (r *Repo) setupEngineHook() {
	r.engine.(*basic.Engine).OnDataHook = func(data ndn.Data, raw enc.Wire, sigCov enc.Wire) error {
		// This is very hacky, improve if possible.
		// Assume that if there is a version it is the second-last component.
		// We might not want to store non-versioned data anyway (?)
		if ver := data.Name().At(-2); ver.IsVersion() {
			log.Info(r, "Storing data", "name", data.Name())
			return r.store.Put(data.Name(), raw.Join())
		} else {
			log.Info(r, "Ignoring non-versioned data", "name", data.Name())
		}
		return nil
	}
}
