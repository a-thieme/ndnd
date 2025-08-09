package awareness

import (
	"sync"
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
	NumPartitions = 32 // TODO: this should be configurable or supplied by the higher layer
	// NumReplicas is the minimum number of replicas per partition.
	NumReplicas = 3 // TODO: this should be configurable or supplied by the higher
)

// Config passed to awareness module
// type RepoAwarenessConfig struct {
// 	HeartbeatInterval time.Duration
// 	HeartbeatExpiry   time.Duration
// 	NumPartitions     uint
// 	NumReplicas       uint
// }

type RepoAwareness struct {
	// single mutex
	mutex sync.RWMutex

	// name of the local node
	name enc.Name

	// awareness of the cluster
	localState *RepoNodeAwareness // node awareness of the local repo
	storage    *RepoAwarenessStore

	// health ndn client
	client ndn.Client
	// awareness sync group
	awarenessSvs *ndn_sync.SvsALO
	heartbeatSvs *ndn_sync.SvSync // TODO: we don't need alo guarantees for heartbeat

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

// TODO: create a new repo awareness object
func NewRepoAwareness(repoName enc.Name, nodeName enc.Name, client ndn.Client) *RepoAwareness {
	awarenessPrefix := repoName.Append(enc.NewGenericComponent("awareness"))
	// awarenessPrefix, _ = enc.NameFromStr("ndnd/repo/awareness") // TODO: testing only
	heartbeatPrefix := repoName.Append(enc.NewGenericComponent("heartbeat"))

	return &RepoAwareness{
		name:               nodeName,
		client:             client, // use Repo shared client
		localState:         NewRepoNodeAwareness(nodeName.String()),
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

	// DEBUG: start with an assigned partition
	r.localState.partitions[0] = true
	r.localState.partitions[3] = true
	r.localState.partitions[5] = true
	r.localState.partitions[7] = true
	// TODO: remove this after testings

	// Start awareness SVS
	r.awarenessSvs, err = ndn_sync.NewSvsALO(ndn_sync.SvsAloOpts{
		Name: r.name,
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
			log.Info(r, "Received heartbeat", pub)
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

	// Partition handlers
	// TODO: currently, just trivial handlers
	r.storage.SetOnUnderReplication(func(id uint64) {
		log.Info(r, "Partition under-replicated", "id", id)
	})

	r.storage.SetOnOverReplication(func(id uint64) {
		log.Info(r, "Partition over-replicated", "id", id)
	})

	// Start awareness update
	log.Info(r, "Starting awareness update")
	if err := r.awarenessSvs.Start(); err != nil {
		log.Error(r, "Failed to start awareness SVS", "err", err)
		return err
	}

	// Start heartbeat
	log.Info(r, "Starting heartbeat")
	if err := r.StartHeartbeat(); err != nil {
		log.Error(r, "Failed to start heartbeat", "err", err)
	}

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
	log.Info(r, "Heartbeat started", "node", r.name)

	// start ticker
	r.ticker = time.NewTicker(HeartbeatInterval)

	// create stop channel
	r.stop = make(chan struct{})

	// start heartbeat loop
	go func() {
		for {
			select {
			case <-r.ticker.C:
				log.Info(r, "Heartbeat published", "time", time.Now())
				r.heartbeatSvs.IncrSeqNo(r.name)
				r.publishAwarenessUpdate() // TODO: test: periodically publish awareness update
			case <-r.stop:
				return
			}
		}
	}()

	return nil
}

// PublishAwarenessUpdate publishes an awareness update with the current node state
func (r *RepoAwareness) publishAwarenessUpdate() {
	// create awareness update
	r.mutex.RLock()
	awarenessUpdate := &tlv.AwarenessUpdate{
		NodeName:   r.name.String(),
		Partitions: r.localState.partitions, // Get partitions this node is responsible for
	}
	r.mutex.RUnlock()

	// publish to awareness SVS
	log.Info(r, "Publishing awareness update", "time", time.Now(), "src", r.name, "partitions", awarenessUpdate.Partitions)
	_, _, err := r.awarenessSvs.Publish(awarenessUpdate.Encode())
	if err != nil {
		log.Error(r, "Error publishing awareness update", "err", err, "time", time.Now())
	}
}

// UpdateLocalPartitions updates the local partitions and publishes an awareness update
func (r *RepoAwareness) UpdateLocalPartitions(partitions *map[uint64]bool) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	r.localState.partitions = *partitions // create a shallow copy of the partitions
	r.publishAwarenessUpdate()
}

// GetReplicas returns the replicas for a given partition (local awareness)
func (r *RepoAwareness) GetReplicas(partitionId uint64) []enc.Name {
	replicas := make([]enc.Name, 0)

	// TODO: implement lookups

	return replicas
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

// SetOnOverReplication sets the callback for over-replication
func (r *RepoAwareness) SetOnOverReplication(callback func(uint64)) {
	r.storage.SetOnOverReplication(callback)
}

// SetOnUnderReplication sets the callback for under-replication
func (r *RepoAwareness) SetOnUnderReplication(callback func(uint64)) {
	r.storage.SetOnUnderReplication(callback)
}
