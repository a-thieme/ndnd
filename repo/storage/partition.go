package storage

import (
	"fmt"
	"sync"
	"time"

	"github.com/named-data/ndnd/repo/tlv"
	enc "github.com/named-data/ndnd/std/encoding"
	"github.com/named-data/ndnd/std/log"
	"github.com/named-data/ndnd/std/ndn"
	"github.com/named-data/ndnd/std/ndn/svs_ps"
	ndn_sync "github.com/named-data/ndnd/std/sync"
)

type PartitionStatus int

const (
	Registered    PartitionStatus = iota
	Unsustainable                 // we can't sustain the partition anymore, or it's not worth it
)

// Represents a partition in the storage
type Partition struct {
	mutex sync.RWMutex

	// id
	id uint64

	// SVS
	svsPrefix enc.Name         // the prefix of the SVS group
	svsGroup  *ndn_sync.SvsALO // the SVS group
	client    ndn.Client

	// commands
	commands map[string]*tlv.RepoCommand

	// storage
	size           uint64 // the estimated size of the partition, in bytes
	store          *RepoStorage
	userSyncGroups map[string]*PartitionSvs

	// status
	status PartitionStatus // current status of the partition
}

func (p *Partition) String() string {
	return fmt.Sprintf("partition #%d", p.id)
}

// NewPartition creates a new partition
// TODO: need more parameters to create a partition
func NewPartition(id uint64, client ndn.Client, store *RepoStorage) *Partition {
	return &Partition{
		id:             id,
		size:           0,
		svsPrefix:      nil,
		client:         client,
		svsGroup:       nil,
		store:          store,
		status:         Registered,
		commands:       make(map[string]*tlv.RepoCommand),
		userSyncGroups: make(map[string]*PartitionSvs),
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
			SnapMe: func(n enc.Name) (enc.Wire, error) {
				return p.Snap(), nil
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
			// partition snapshot is the application state, i.e. the data in the partition

			snapshot, err := svs_ps.ParseHistorySnap(enc.NewWireView(pub.Content), true)
			if err != nil {
				panic(err) // impossible, encoded by us
			}

			// handle snapshot
			for _, entry := range snapshot.Entries {
				// parse command
				command, err := tlv.ParseRepoCommand(enc.NewWireView(entry.Content), true)
				if err != nil {
					panic(err)
				}

				// handle command
				p.HandleCommand(command)
			}
		} else {
			// Process the publication.
			log.Info(p, "Received non-snapshot publication", "pub", pub.Content)

			// parse command
			command, err := tlv.ParseRepoCommand(enc.NewWireView(pub.Content), true)
			if err != nil {
				panic(err)
			}

			// handle command
			p.HandleCommand(command)
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

// Snap takes a snapshot of this partition
func (p *Partition) Snap() enc.Wire {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

	snap := tlv.PartitionSnapshot{
		Commands: make([]*tlv.RepoCommand, 0),
	}

	// TODO: optimization: if there is a delete command after insert, we don't need to include either in the snapshot
	for _, entry := range p.commands {
		snap.Commands = append(snap.Commands, entry)
	}

	return snap.Encode()
}
