package distribution

import (
	"github.com/named-data/ndnd/repo/tlv"
	"github.com/named-data/ndnd/repo/types"
	enc "github.com/named-data/ndnd/std/encoding"
	"github.com/named-data/ndnd/std/log"
	"github.com/named-data/ndnd/std/ndn"
	ndn_sync "github.com/named-data/ndnd/std/sync"
	"sort"
	"time"
)

type Sharing struct {
	availabilitySvs    *ndn_sync.SvsALO
	availabilityPrefix enc.Name
	// time interval
	interval          time.Duration
	client            ndn.Client
	nodeName          enc.Name
	groupAvailability map[*enc.Name]uint64

	ticker *time.Ticker
	// tell ticker to stop
	stop chan struct{}

	getAbility func(*tlv.RepoCommand) int
}

func NewSharing(repo *types.RepoShared) *Sharing {
	return &Sharing{
		interval:           1,
		client:             repo.Client,
		availabilityPrefix: repo.NodeNameN.Append(enc.NewGenericComponent("availability")),
		nodeName:           repo.NodeNameN,
		groupAvailability:  map[*enc.Name]uint64{},
	}
}

func (s *Sharing) Start() (err error) {
	log.Info(s, "Starting Availability SVS")

	// Start awareness SVS
	s.availabilitySvs, err = ndn_sync.NewSvsALO(ndn_sync.SvsAloOpts{
		Name: s.nodeName,
		Svs: ndn_sync.SvSyncOpts{
			Client:      s.client,
			GroupPrefix: s.availabilityPrefix,
		},
		Snapshot:        &ndn_sync.SnapshotNull{},
		FetchLatestOnly: true,
	})
	if err != nil {
		panic(err)
	}

	// Set error handler
	s.availabilitySvs.SetOnError(func(err error) {
		log.Error(s, "SVS ALO error", "err", err)
	})

	// Subscribe to all publishers
	s.availabilitySvs.SubscribePublisher(enc.Name{}, func(pub ndn_sync.SvsPub) {
		if pub.IsSnapshot {
			log.Info(s, "Received snapshot publication", "pub", pub.Content)
			panic("Snapshot publications are not supported in Repo Awareness")
		} else {
			// Process the publication.
			log.Debug(s, "Received non-snapshot publication from", pub.Publisher)

			update, err := tlv.ParseRepoAvailabilityUpdate(enc.NewWireView(pub.Content), true)
			if err != nil {
				log.Warn(s, "could not parse availabilty update")
				panic(err)
			}
			log.Debug(s, "new availability:", pub.Publisher, update.Availability)

			s.groupAvailability[&pub.Publisher] = update.Availability
		}
	})

	// Announce group prefix route
	for _, route := range []enc.Name{
		s.availabilitySvs.SyncPrefix(),
		s.availabilitySvs.DataPrefix(),
	} {
		s.client.AnnouncePrefix(ndn.Announcement{
			Name:   route,
			Expose: true,
		})
	}
	s.availabilitySvs.Start()
	if err := s.availabilitySvs.Start(); err != nil {
		log.Error(s, "Failed to start availability SVS", "err", err)
		return err
	} // start ticker
	s.ticker = time.NewTicker(s.interval)

	// create stop channel
	s.stop = make(chan struct{})

	// start heartbeat loop
	go func() {
		for {
			select {
			case <-s.ticker.C:
				log.Info(s, "publishing availability")
				au := tlv.RepoAvailabilityUpdate{
					Availability: uint64(s.getAbility(nil)),
				}
				_, _, err := s.availabilitySvs.Publish(au.Encode())
				if err != nil {
					log.Warn(s, "issue publishing availability", err.Error)
				}
			case <-s.stop:
				return
			}
		}
	}()
	return nil
}

type Pair struct {
	Key   *enc.Name
	Value uint64
}

// get nodes in order of availability
func (s *Sharing) GetNodes() []*enc.Name {
	var pairs []Pair
	for k, v := range s.groupAvailability {
		pairs = append(pairs, Pair{Key: k, Value: v})
	}

	sort.Slice(pairs, func(i, j int) bool {
		return pairs[i].Value > pairs[j].Value
	})

	var sortedKeys []*enc.Name
	for _, p := range pairs {
		sortedKeys = append(sortedKeys, p.Key)
	}

	return sortedKeys
}

func (s *Sharing) String() string {
	return "sharing"
}
