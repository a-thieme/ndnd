package storage

import (
	"fmt"
	"time"

	enc "github.com/named-data/ndnd/std/encoding"
	"github.com/named-data/ndnd/std/log"
	"github.com/named-data/ndnd/std/ndn"
	ndn_sync "github.com/named-data/ndnd/std/sync"
)

type PartitionStatus int

const (
	Registered    PartitionStatus = iota
	Unsustainable                 // we can't sustain the partition anymore, or it's not worth it
)

// Represents a partition in the storage
type Partition struct {
	// id
	id uint64

	// SVS
	svsPrefix enc.Name         // the prefix of the SVS group
	svsGroup  *ndn_sync.SvsALO // the SVS group
	client    ndn.Client

	// storage
	store *RepoStorage
	size  uint64 // the estimated size of the partition, in bytes

	// status
	status PartitionStatus // current status of the partition
}

func (p *Partition) String() string {
	return fmt.Sprintf("Partition #%d", p.id)
}

// NewPartition creates a new partition
// TODO: need more parameters to create a partition
func NewPartition(id uint64, client ndn.Client, store *RepoStorage) *Partition {
	return &Partition{
		id:        id,
		size:      0,
		svsPrefix: nil,
		status:    Registered,
		client:    client,
		svsGroup:  nil,
		store:     store,
	}
}

// Starts the partition sync group
func (p *Partition) Start() (err error) {
	log.Info(p, "Starting partition SVS ALO")

	// TODO: get the svs prefix from the configuraiton
	groupPrefix := "/ndnd/repo/svs/partition/"
	p.svsPrefix, err = enc.NameFromStr(fmt.Sprintf("%s%d", groupPrefix, p.id))
	if err != nil {
		return err
	}

	// initialize the SVS group
	p.svsGroup, err = ndn_sync.NewSvsALO(ndn_sync.SvsAloOpts{
		Name: p.svsPrefix,
		Svs: ndn_sync.SvSyncOpts{
			Client:            p.client,
			GroupPrefix:       p.svsPrefix,
			SuppressionPeriod: 500 * time.Millisecond, // TODO: should this be reactive to the heartbeat interval?
			PeriodicTimeout:   30 * time.Second,       // TODO: this is the default value. To my understanding, periodic sync interests don't increase sequence numbers; it can be used as a redundancy to inform other nodes about the newest local state, but it can't replace the heartbeat mechanism
		},
		Snapshot: &ndn_sync.SnapshotNodeLatest{
			Client: p.client,
			SnapMe: func(enc.Name) (enc.Wire, error) {
				// TODO: bundle all state of the partition and make a snapshot

				return enc.Wire{}, nil
			},
			Threshold: 10, // TODO: configurable
		},
	})
	if err != nil {
		return err
	}

	// Set error handler
	p.svsGroup.SetOnError(func(error) {
		log.Error(p, "SVS ALO error", "err", err)
	})

	// Subscribe to all publishers
	// TODO: currently listen to all publishers - should be subscribe to only publishers known by awareness module?
	p.svsGroup.SubscribePublisher(enc.Name{}, func(pub ndn_sync.SvsPub) {
		if pub.IsSnapshot {
			log.Info(p, "Received snapshot publication", "pub", pub.Content)
			// TODO: handle snapshot
		} else {
			// Process the publication.
			log.Info(p, "Received non-snapshot publication", "pub", pub.Content)
			// TODO: process individual update
		}
	})

	// Announce group prefix route
	for _, route := range []enc.Name{
		p.svsGroup.SyncPrefix(),
		p.svsGroup.DataPrefix(),
	} {
		p.client.AnnouncePrefix(ndn.Announcement{Name: route})
	}

	// Start partition SVS
	if err = p.svsGroup.Start(); err != nil {
		log.Error(p, "Unable to start partition SVS ALO", "err", err)
		return err
	}

	return nil
}

// Stops the partition sync group
func (p *Partition) Stop() (err error) {
	log.Info(p, "Stopping partition SVS ALO")

	// TODO: leave the SVS group
	if p.svsGroup != nil {
		if err = p.svsGroup.Stop(); err != nil {
			log.Error(p, "Unable to stop partition SVS ALO", "err", err)
		}

		// Withdraw group prefix route
		for _, route := range []enc.Name{
			p.svsGroup.SyncPrefix(),
			p.svsGroup.DataPrefix(),
		} {
			p.client.WithdrawPrefix(route, nil)
		}

		p.svsGroup = nil
	}

	return nil
}
