package awareness

import (
	"sync"
	"time"

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
)

type RepoAwareness struct {
	// single mutex
	mutex sync.Mutex

	// name of the local repo
	name enc.Name

	// awareness of the cluster
	storage *LRU[string, RepoNodeAwareness]
	// callback function when a node awareness expires
	onExpiry func(RepoNodeAwareness)

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
	// TODO:
	groupPrefix, _ := enc.NameFromStr(HealthSVSGroupPrefix)

	return &RepoAwareness{
		name:              name,
		storage:           NewLRU[string, RepoNodeAwareness](),
		client:            client, // use Repo shared client
		healthSvs:         nil,
		healthGroupPrefix: groupPrefix,
	}
}

func (r *RepoAwareness) Start() (err error) {
	log.Info(r, "Starting Repo Awareness SVS")

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
		FetchLatestOnly: true,                     // only fetch the latest sequence number
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
			log.Info(r, "Received snapshot publication", "pub", pub)
		} else {
			// Process the publication.
			log.Info(r, "Received non-snapshot publication", "pub", pub)
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

	// Start heartbeat
	if err := r.StartHeartbeat(); err != nil {
		log.Error(r, "Failed to start heartbeat", "err", err)
	}

	return err
}

func (r *RepoAwareness) Stop() (err error) {
	log.Info(r, "Stopping Repo Awareness SVS")

	// Stop health SVS
	if r.healthSvs != nil {
		if err := r.healthSvs.Stop(); err != nil {
			log.Error(r, "Error stopping health SVS", "err", err)
		}
		r.healthSvs = nil
	} else {
		return nil
	}

	// Withdraw group prefix route
	for _, route := range []enc.Name{
		r.healthSvs.SyncPrefix(),
		r.healthSvs.DataPrefix(),
	} {
		r.client.WithdrawPrefix(route, nil)
	}

	// Stop SVS ALO
	if r.healthSvs != nil {
		r.healthSvs.Stop()
	}

	return nil
}

func (r *RepoAwareness) processUpdate(update *ndn_sync.SvSyncUpdate) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	// TODO: Process the update
	// This is a placeholder for the actual implementation
	// In a real implementation, we would update the awareness of the node in the cluster
	// We should only fetch high in any case, even if this may miss potential partition updates - for performance reasons
	log.Info(r, "Processing update", "update", update)
}

// TODO: transmit a heartbeat, and schedule next heartbeat
// This function is blocking and should be in a separate goroutine
func (r *RepoAwareness) StartHeartbeat() (err error) {
	log.Info(r, "Heartbeat started", "repo", r.name)

	// publish heartbeat data using svs
	ticker := time.NewTicker(HeartbeatInterval)
	for {
		t := <-ticker.C
		_, _, err := r.healthSvs.Publish(enc.Wire{}) // TODO: empty message
		log.Info(r, "Published heartbeat", "time", t)

		if err != nil {
			log.Error(r, "Error publishing heartbeat", "err", err, "time", t)
			return err
		}

		// TODO: create a heartbeat update
		// seq := r.healthSvs.IncrSeqNo(r.client)
	}
}

// TODO: return the list of known nodes and their status (up, p-fail, fail)
func (r *RepoAwareness) GetKnownNodes() {

}

// Handlers
// SetOnExpiry sets the callback function to be called when a node awareness expires.
func (l *RepoAwareness) SetOnExpiry(handler func(RepoNodeAwareness)) {
	l.onExpiry = handler
}

// Update updates the awareness of a node in the cluster.
// If the node does not exist, it adds a new entry.
func (l *RepoAwareness) Update(awareness RepoNodeAwareness) {
	key := awareness.name
	l.storage.Put(key, awareness, true)

	// TODO: pass expired node awareness to an expiry handler
	// for l.storage.storage.Head() != nil {
	// 	if l.storage.storage.Head().value.
	// }
}

// Get retrieves the awareness of a node by its name.
// If the node does not exist, it returns nil.
func (l *RepoAwareness) Get(key string) *RepoNodeAwareness {
	awareness := l.storage.Get(key, false)
	return awareness
}
