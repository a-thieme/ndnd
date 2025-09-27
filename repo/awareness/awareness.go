package awareness

import (
	"time"

	"github.com/named-data/ndnd/repo/tlv"
	"github.com/named-data/ndnd/repo/types"
	enc "github.com/named-data/ndnd/std/encoding"
	"github.com/named-data/ndnd/std/log"
	"github.com/named-data/ndnd/std/ndn"
	ndn_sync "github.com/named-data/ndnd/std/sync"
)

// TODO: remove heartbeat from this
type RepoAwareness struct {
	// name of the local node
	nodeNameN enc.Name

	// awareness of the cluster
	Storage *RepoAwarenessStore

	// health ndn client
	client ndn.Client

	// awareness sync group
	awarenessSvs *ndn_sync.SvsALO
	heartbeatSvs *ndn_sync.SvSync

	// TODO: make put into a configuration struct
	awarenessSvsPrefix enc.Name // group prefix for awareness SVS

	// heartbeat interval
	heartbeatInterval time.Duration

	// heartbeat
	ticker             *time.Ticker // ticker for heartbeat
	heartbeatSvsPrefix enc.Name     // group prefix for heartbeat SVS

	// stop channel to signal the heartbeat loop to stop
	stop chan struct{}
}

func (r *RepoAwareness) String() string {
	return "repo-awareness"
}

// NewRepoAwareness creates a new RepoAwareness object
func NewRepoAwareness(repo *types.RepoShared) *RepoAwareness {
	repoNameN := repo.RepoNameN
	nodeNameN := repo.NodeNameN
	client := repo.Client
	heartbeatInterval := repo.HeartbeatInterval

	awarenessPrefix := repoNameN.Append(enc.NewGenericComponent("awareness"))
	heartbeatPrefix := repoNameN.Append(enc.NewGenericComponent("heartbeat"))

	return &RepoAwareness{
		nodeNameN:          nodeNameN,
		client:             client, // use Repo shared client
		Storage:            NewRepoAwarenessStore(repo),
		awarenessSvsPrefix: awarenessPrefix,
		heartbeatSvsPrefix: heartbeatPrefix,
		heartbeatInterval:  heartbeatInterval,
	}
}

func (r *RepoAwareness) Start() (err error) {
	log.Info(r, "Starting Repo Awareness SVS")

	// Start awareness SVS
	r.awarenessSvs, err = ndn_sync.NewSvsALO(ndn_sync.SvsAloOpts{
		Name: r.nodeNameN,
		Svs: ndn_sync.SvSyncOpts{
			Client:      r.client,
			GroupPrefix: r.awarenessSvsPrefix,
		},
		Snapshot:        &ndn_sync.SnapshotNull{},
		FetchLatestOnly: true,
	})
	if err != nil {
		panic(err)
	}

	// Set error handler
	r.awarenessSvs.SetOnError(func(err error) {
		log.Error(r, "SVS ALO error", "err", err)
	})

	// Subscribe to all publishers
	r.awarenessSvs.SubscribePublisher(enc.Name{}, func(pub ndn_sync.SvsPub) {
		if pub.IsSnapshot {
			log.Info(r, "Received snapshot publication", "pub", pub.Content)
			panic("Snapshot publications are not supported in Repo Awareness")
		} else {
			// Process the publication.
			log.Debug(r, "Received non-snapshot publication", "pub", pub.Content)

			update, err := tlv.ParseAwarenessUpdate(enc.NewWireView(pub.Content), true)
			if err != nil {
				panic(err)
			}

			// update storage
			r.Storage.ProcessAwarenessUpdate(update)
		}
	})

	// Announce group prefix route
	for _, route := range []enc.Name{
		r.awarenessSvs.SyncPrefix(),
		r.awarenessSvs.DataPrefix(),
		r.heartbeatSvsPrefix,
	} {
		r.client.AnnouncePrefix(ndn.Announcement{
			Name:   route,
			Expose: true,
		})
	}

	// Start awareness SVS
	log.Info(r, "Starting awareness update")
	if err := r.awarenessSvs.Start(); err != nil {
		log.Error(r, "Failed to start awareness SVS", "err", err)
		return err
	}

	// Start heartbeat SVS
	log.Info(r, "Starting heartbeat")
	log.Debug(r, "configuring heartbeat")
	r.heartbeatSvs = ndn_sync.NewSvSync(ndn_sync.SvSyncOpts{
		Client:      r.client,
		GroupPrefix: r.heartbeatSvsPrefix,
		OnUpdate: func(pub ndn_sync.SvSyncUpdate) {
			log.Debug(r, "Received heartbeat", pub)
			r.Storage.ProcessHeartbeat(&pub.Name)
		},
	})
	log.Debug(r, "starting heartbeat")
	if err = r.StartHeartbeat(); err != nil {
		log.Error(r, "Failed to start heartbeat", "err", err)
		return err
	}

	return err
}

func (r *RepoAwareness) Stop() (err error) {
	log.Info(r, "Stopping Repo Awareness SVS")

	// stop heartbeat svs
	// TODO: try removing these outer-layer if statements
	if r.heartbeatSvs != nil {
		if err := r.heartbeatSvs.Stop(); err != nil {
			log.Error(r, "Error stopping heartbeat SVS", "err", err)
		}
		close(r.stop)
		r.ticker.Stop()
	}

	// Stop awareness SVS
	if r.awarenessSvs != nil {
		// stop awareness svs_alo
		if err := r.awarenessSvs.Stop(); err != nil {
			log.Error(r, "Error stopping health SVS", "err", err)
		}

		// Withdraw group prefix route
		for _, route := range []enc.Name{
			r.awarenessSvs.SyncPrefix(),
			r.awarenessSvs.DataPrefix(),
			r.heartbeatSvsPrefix,
		} {
			r.client.WithdrawPrefix(route, nil)
		}

		r.awarenessSvs = nil
	}

	return nil
}

// StartHeartbeat starts the heartbeat loop in a goroutine
func (r *RepoAwareness) StartHeartbeat() (err error) {
	log.Trace(r, "call start heartbeat")
	// start heartbeat svs
	err = r.heartbeatSvs.Start()
	log.Trace(r, "after svs")
	if err != nil {
		log.Error(r, "Failed to start heartbeat SVS", "err", err)
		return err
	}
	log.Info(r, "Heartbeat started", "node", r.nodeNameN)

	// start ticker
	r.ticker = time.NewTicker(r.heartbeatInterval)

	// create stop channel
	r.stop = make(chan struct{})

	// start heartbeat loop
	go func() {
		for {
			select {
			case <-r.ticker.C:
				log.Info(r, "Heartbeat published")
				r.heartbeatSvs.IncrSeqNo(r.nodeNameN)
			case <-r.stop:
				return
			}
		}
	}()

	return nil
}

// PublishAwarenessUpdate publishes an awareness update with the given node state
// This method is called on-event, i.e. whenever local jobs change
// Thread-safe
// this is called directly from storage because storage knows when it updates itself
func (r *RepoAwareness) PublishAwarenessUpdate(awarenessUpdate *tlv.AwarenessUpdate) {
	// publish to awareness SVS
	log.Info(r, "Publishing awareness update for node", r.nodeNameN, "jobs", awarenessUpdate.ActiveJobs)
	_, _, err := r.awarenessSvs.Publish(awarenessUpdate.Encode())
	log.Debug(r, "after Publish() in awareness for update", awarenessUpdate)
	if err != nil {
		log.Error(r, "Error publishing awareness update", "err", err, "time", time.Now())
	}
}

// GetOnlineNodes returns the nodes that are known to be online
func (r *RepoAwareness) getOnlineNodes() []string {
	nameNs := make([]string, 0)
	for name, awareness := range r.Storage.nodeStates {
		if awareness.status == Up {
			nameNs = append(nameNs, name)
		}
	}
	return nameNs
}
