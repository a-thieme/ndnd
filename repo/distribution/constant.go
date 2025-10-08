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
	}
	return err
}

type Pair struct {
	Key   *enc.Name
	Value uint64
}

// sortMapByValue takes a map[string]int and returns a slice of strings
// (the keys), sorted in ascending order based on their corresponding integer values.
func (s *Sharing) getNodes() []*enc.Name {
	// 1. Convert the map into a slice of Pair structs.
	var pairs []Pair
	for k, v := range s.groupAvailability {
		pairs = append(pairs, Pair{Key: k, Value: v})
	}

	// 2. Sort the slice of Pair structs using sort.Slice.
	// The function provided to sort.Slice defines the comparison logic.
	// It sorts by 'Value' in descending order (a > b).
	sort.Slice(pairs, func(i, j int) bool {
		return pairs[i].Value > pairs[j].Value
	})

	// 3. Extract the sorted keys (strings) back into a []string slice.
	var sortedKeys []*enc.Name
	for _, p := range pairs {
		sortedKeys = append(sortedKeys, p.Key)
	}

	return sortedKeys
}

// FIXME:  needs timer/ticker similar to heartbeat and a callback to get the availability

func (s *Sharing) String() string {
	return "sharing"
}
