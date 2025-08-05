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
	// HealthSVSName is the group fix for health svs sync
	HealthSVSName        = "repo-health"
	HealthSVSGroupPrefix = "/ndn/repo/health"
	// HeartbeatInterval is the interval between heartbeats.
	HeartbeatInterval = 5 * time.Second
	// HeartbeatExpiry is the time after which a node is considered down if no heartbeat is received.
	HeartbeatExpiry = 20 * time.Second

	// NumPartitions is the number of partitions in the system.
	NumPartitions = 32 // TODO: this should be configurable or supplied by the higher layer
	// NumReplicas is the minimum number of replicas per partition.
	NumReplicas = 3 // TODO: this should be configurable or supplied by the higher
)

type RepoAwareness struct {
	// single mutex
	mutex sync.RWMutex

	// name of the local repo
	name enc.Name

	// awareness of the cluster
	localState *RepoNodeAwareness // node awareness of the local repo
	storage    *RepoAwarenessStore

	// health ndn client
	client ndn.Client
	// health sync group
	healthSvs *ndn_sync.SvsALO

	// TODO: make put into a configuration struct
	healthGroupPrefix enc.Name // group prefix for health SVS
}

func (r *RepoAwareness) String() string {
	return "repo-awareness"
}

// TODO: create a new repo awareness object
func NewRepoAwareness(name enc.Name, client ndn.Client) *RepoAwareness {
	// TODO: get the health svs group prefix from somewhere else, e.g., configuration
	groupPrefix, _ := enc.NameFromStr(HealthSVSGroupPrefix)

	return &RepoAwareness{
		name:              name,
		client:            client, // use Repo shared client
		healthSvs:         nil,
		healthGroupPrefix: groupPrefix,
		localState:        NewRepoNodeAwareness(name.String()),
		storage:           NewRepoAwarenessStore(),
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

	r.healthSvs, err = ndn_sync.NewSvsALO(ndn_sync.SvsAloOpts{
		Name: r.name,
		Svs: ndn_sync.SvSyncOpts{
			Client:            r.client,
			GroupPrefix:       r.healthGroupPrefix,
			SuppressionPeriod: 500 * time.Millisecond, // TODO: should this be reactive to the heartbeat interval?
			PeriodicTimeout:   30 * time.Second,       // TODO: this is the default value. To my understanding, periodic sync interests don't increase sequence numbers; it can be used as a redundancy to inform other nodes about the newest local state, but it can't replace the heartbeat mechanism
			OnUpdate: func(ssu ndn_sync.SvSyncUpdate) {
				log.Info(r, "Received SVS update", "update", ssu)
				// TODO： handle the update
				// 1. if the update is a heartbeat, update the node last known time
				// 2. if the update is a partition dump, update both the last known time and partition awareness
				// go r.processUpdate(&ssu)
			},
		},
		Snapshot:        &ndn_sync.SnapshotNull{}, // there is no need for snapshots in this case
		FetchLatestOnly: false,                    // only fetch the latest sequence number
	})
	if err != nil {
		panic(err)
	}

	// Set error handler
	r.healthSvs.SetOnError(func(err error) {
		log.Error(r, "SVS ALO error", "err", err)
	})

	// Subscribe to all publishers
	r.healthSvs.SubscribePublisher(enc.Name{}, func(pub ndn_sync.SvsPub) {
		if pub.IsSnapshot {
			log.Info(r, "Received snapshot publication", "pub", pub.Content)
			panic("Snapshot publications are not supported in Repo Awareness")
		} else {
			// Process the publication.
			log.Info(r, "Received non-snapshot publication", "pub", pub.Content)

			update, err := tlv.ParseAwarenessUpdate(enc.NewWireView(pub.Content), true)
			if err != nil {
				panic(err)
			}

			// update storage
			r.storage.ProcessUpdate(update)
		}
	})

	// Announce group prefix route
	for _, route := range []enc.Name{
		r.healthSvs.SyncPrefix(),
		r.healthSvs.DataPrefix(),
	} {
		r.client.AnnouncePrefix(ndn.Announcement{Name: route})
	}

	// Start health SVS
	if err = r.healthSvs.Start(); err != nil {
		log.Error(r, "Unable to start health SVS ALO", "err", err)
		return err
	}

	// TODO: under/over replication handler
	r.storage.SetOnUnderReplication(func(id uint64) {
		log.Info(r, "Partition under-replicated", "id", id)
	})

	r.storage.SetOnOverReplication(func(id uint64) {
		log.Info(r, "Partition over-replicated", "id", id)
	})

	// Start heartbeat
	go func() {
		if err := r.StartHeartbeat(); err != nil {
			log.Error(r, "Failed to start heartbeat", "err", err)
		}
	}()

	return err
}

func (r *RepoAwareness) Stop() (err error) {
	log.Info(r, "Stopping Repo Awareness SVS")

	// Stop health SVS
	if r.healthSvs != nil {
		if err := r.healthSvs.Stop(); err != nil {
			log.Error(r, "Error stopping health SVS", "err", err)
		}

		// Withdraw group prefix route
		for _, route := range []enc.Name{
			r.healthSvs.SyncPrefix(),
			r.healthSvs.DataPrefix(),
		} {
			r.client.WithdrawPrefix(route, nil)
		}

		r.healthSvs = nil
	}

	return nil
}

// StartHeartbeat runs the heartbeat loop in a goroutine
func (r *RepoAwareness) StartHeartbeat() (err error) {
	log.Info(r, "Heartbeat started", "repo", r.name)

	ticker := time.NewTicker(HeartbeatInterval)
	defer ticker.Stop()

	for {
		t := <-ticker.C
		// Create awareness update with current node info
		r.mutex.RLock()
		awarenessUpdate := &tlv.AwarenessUpdate{
			NodeName:   r.name.String(),
			Partitions: r.localState.partitions, // Get partitions this node is responsible for
		}
		r.mutex.RUnlock()

		log.Info(r, "Published partitions", "time", t, "partitions", awarenessUpdate.Partitions)
		_, _, err := r.healthSvs.Publish(awarenessUpdate.Encode())

		// log.Info(r, "Published heartbeat", "time", t)

		if err != nil {
			log.Error(r, "Error publishing heartbeat", "err", err, "time", t)
			return err
		}
	} // TODO: make this a separate goroutine that doesn't block the main thread
}
