package awareness

import (
	"math/rand"
	"time"

	"github.com/named-data/ndnd/repo/tlv"
	enc "github.com/named-data/ndnd/std/encoding"
	"github.com/named-data/ndnd/std/log"
	"github.com/named-data/ndnd/std/ndn"
	ndn_sync "github.com/named-data/ndnd/std/sync"
)

// TODO: variables in this section should be configurable or supplied by the higher layer
// for now, we use hardcoded values to finish the implementation first
const (
	// HeartbeatInterval is the interval between heartbeats (at most).
	HeartbeatInterval = 5 * time.Second
	// HeartbeatExpiry is the time after which a node is considered down if no heartbeat is received.
	HeartbeatExpiry = 20 * time.Second

	// NumPartitions is the number of partitions in the system.
	NumPartitions = 128 // TODO: this should be configurable or supplied by the higher layer
	// NumReplicas is the minimum number of replicas per partition.
	NumReplicas = 2 // TODO: this should be configurable or supplied by the higher
)

// Config passed to awareness module
// type RepoAwarenessConfig struct {
// 	HeartbeatInterval time.Duration
// 	HeartbeatExpiry   time.Duration
// 	NumPartitions     uint
// 	NumReplicas       uint
// }

type RepoAwareness struct {
	// name of the local node
	nodeNameN enc.Name
	nodeName  string // for convenience

	// awareness of the cluster
	storage *RepoAwarenessStore

	// health ndn client
	client ndn.Client
	// awareness sync group
	awarenessSvs *ndn_sync.SvsALO
	heartbeatSvs *ndn_sync.SvSync

	// TODO: make put into a configuration struct
	awarenessSvsPrefix enc.Name // group prefix for awareness SVS

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
func NewRepoAwareness(repoNameN enc.Name, nodeNameN enc.Name, client ndn.Client) *RepoAwareness {
	awarenessPrefix := repoNameN.Append(enc.NewGenericComponent("awareness"))
	// awarenessPrefix, _ = enc.NameFromStr("ndnd/repo/awareness") // TODO: testing only
	heartbeatPrefix := repoNameN.Append(enc.NewGenericComponent("heartbeat"))

	return &RepoAwareness{
		nodeNameN:          nodeNameN,
		nodeName:           nodeNameN.String(),
		client:             client, // use Repo shared client
		storage:            NewRepoAwarenessStore(),
		awarenessSvsPrefix: awarenessPrefix,
		heartbeatSvsPrefix: heartbeatPrefix,
		awarenessSvs:       nil,
		heartbeatSvs:       nil,
		ticker:             nil,
		stop:               nil,
	}
}

func (r *RepoAwareness) Start() (err error) {
	log.Info(r, "Starting Repo Awareness SVS")

	// Start awareness SVS
	r.awarenessSvs, err = ndn_sync.NewSvsALO(ndn_sync.SvsAloOpts{
		Name: r.nodeNameN,
		Svs: ndn_sync.SvSyncOpts{
			Client:            r.client,
			GroupPrefix:       r.awarenessSvsPrefix,
			SuppressionPeriod: 500 * time.Millisecond, // TODO: should this be reactive to the heartbeat interval?
			PeriodicTimeout:   30 * time.Second,       // TODO: periodic timeouts are for redundancy only; we want to minimize traffic
		},
		Snapshot:        &ndn_sync.SnapshotNull{}, // there is no need for snapshots in this case
		FetchLatestOnly: false,                    // only fetch the latest sequence number
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
			// log.Info(r, "Received non-snapshot publication", "pub", pub.Content)

			update, err := tlv.ParseAwarenessUpdate(enc.NewWireView(pub.Content), true)
			if err != nil {
				panic(err)
			}

			// update storage
			r.storage.ProcessAwarenessUpdate(update)
		}
	})

	// Start heartbeat SVS
	r.heartbeatSvs = ndn_sync.NewSvSync(ndn_sync.SvSyncOpts{
		Client:      r.client,
		GroupPrefix: r.heartbeatSvsPrefix,
		OnUpdate: func(pub ndn_sync.SvSyncUpdate) {
			// log.Info(r, "Received heartbeat", pub)
			r.storage.ProcessHeartbeat(pub.Name)
		},
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
	if err := r.StartHeartbeat(); err != nil {
		log.Error(r, "Failed to start heartbeat", "err", err)
	}

	// Mark our initial state as alive
	r.storage.ProcessHeartbeat(r.nodeNameN) // the first heartbeat a node hears is its own

	// DEBUG: start with an assigned partition
	if rand.Float64() < 0.5 {
		r.AddLocalPartition(0)
	}
	if rand.Float64() < 0.5 {
		r.AddLocalPartition(1)
	}
	if rand.Float64() < 0.5 {
		r.AddLocalPartition(2)
	}
	if rand.Float64() < 0.5 {
		r.AddLocalPartition(3)
	}
	if rand.Float64() < 0.5 {
		r.AddLocalPartition(4)
	}
	if rand.Float64() < 0.5 {
		r.AddLocalPartition(5)
	}
	// TODO: remove this after testings

	// Check initial replication
	r.storage.CheckReplications()

	return err
}

func (r *RepoAwareness) Stop() (err error) {
	log.Info(r, "Stopping Repo Awareness SVS")

	// stop heartbeat svs
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
	// start heartbeat svs
	err = r.heartbeatSvs.Start()
	if err != nil {
		log.Error(r, "Failed to start heartbeat SVS", "err", err)
		return err
	}
	log.Info(r, "Heartbeat started", "node", r.nodeNameN)

	// start ticker
	r.ticker = time.NewTicker(HeartbeatInterval)

	// create stop channel
	r.stop = make(chan struct{})

	// start heartbeat loop
	go func() {
		for {
			select {
			case <-r.ticker.C:
				r.storage.ProcessHeartbeat(r.nodeNameN) // it's a hack so the node knows itself is alive

				log.Info(r, "Heartbeat published", "time", time.Now())
				r.heartbeatSvs.IncrSeqNo(r.nodeNameN)

				r.publishAwarenessUpdate() // TODO: test: periodically publish awareness update

				r.storage.CheckReplications()
			case <-r.stop:
				return
			}
		}
	}()

	return nil
}

// PublishAwarenessUpdate publishes an awareness update with the current node state
// Thread-safe
func (r *RepoAwareness) publishAwarenessUpdate() {
	// create awareness update
	awarenessUpdate := r.storage.ProduceAwarenessUpdate(r.nodeNameN)

	// publish to awareness SVS
	log.Info(r, "Publishing awareness update", "time", time.Now(), "src", r.nodeNameN, "partitions", awarenessUpdate.Partitions)
	_, _, err := r.awarenessSvs.Publish(awarenessUpdate.Encode())
	if err != nil {
		log.Error(r, "Error publishing awareness update", "err", err, "time", time.Now())
	}
}

// AddLocalPartition adds a partition to the local state and publishes an awareness update
// Thread-safe
func (r *RepoAwareness) AddLocalPartition(partitionId uint64) {
	r.storage.AddNodePartition(partitionId, r.nodeName)
	r.publishAwarenessUpdate()
}

// DropLocalPartition drops a partition from the local state and publishes an awareness update
// Thread-safe
func (r *RepoAwareness) DropLocalPartition(partitionId uint64) {
	r.storage.RemoveNodePartition(partitionId, r.nodeName)
	r.publishAwarenessUpdate()
}

// GetPartitionReplicas returns the replicas for a given partition (local awareness)
// Thread-safe
func (r *RepoAwareness) GetPartitionReplicas(partitionId uint64) []enc.Name {
	return r.storage.GetPartitionReplicas(partitionId)
}

// GetNumReplicas returns the number of replicas for a given partition
// Thread-safe
func (r *RepoAwareness) GetNumReplicas(partitionId uint64) int {
	return r.storage.GetNumReplicas(partitionId)
}

// GetOnlineNodes returns the nodes that are known to be online
func (r *RepoAwareness) GetOnlineNodes() []enc.Name {
	nameNs := make([]enc.Name, 0)
	for name, awareness := range r.storage.nodeStates {
		if awareness.status == Up {
			nameN, _ := enc.NameFromStr(name)
			nameNs = append(nameNs, nameN)
		}
	}
	return nameNs
}

// OwnsPartition returns if the local node owns a partition
// Thread-safe
func (r *RepoAwareness) OwnsPartition(partitionId uint64) bool {
	return r.storage.NodeOwnsPartition(partitionId, r.nodeName)
}

// SetOnOverReplication sets the callback for over-replication
func (r *RepoAwareness) SetOnOverReplication(callback func(uint64)) {
	r.storage.SetOnOverReplication(callback)
}

// SetOnUnderReplication sets the callback for under-replication
func (r *RepoAwareness) SetOnUnderReplication(callback func(uint64)) {
	r.storage.SetOnUnderReplication(callback)
}
