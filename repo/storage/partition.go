package storage

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/named-data/ndnd/repo/tlv"
	"github.com/named-data/ndnd/repo/types"
	enc "github.com/named-data/ndnd/std/encoding"
	"github.com/named-data/ndnd/std/log"
	"github.com/named-data/ndnd/std/ndn"
	"github.com/named-data/ndnd/std/ndn/svs_ps"
	ndn_sync "github.com/named-data/ndnd/std/sync"
)

// TODO: need a method to check if the data a command is related to has actually been fetched. Need to check this because, e.g., in case of failure recovery, we will lose our handlers and need to restart them.

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

	// commands
	commands *types.RandomAccessDoubleLinkedList[string, *tlv.RepoCommand]

	// storage
	size           uint64 // the estimated size of the partition, in bytes
	userSyncGroups map[string]*PartitionSvs

	// status
	status PartitionStatus // current status of the partition

	// repo shared resource
	repo    *types.RepoShared
	storage *RepoStorage
}

func (p *Partition) String() string {
	return fmt.Sprintf("partition #%d", p.id)
}

// NewPartition creates a new partition
// TODO: need more parameters to create a partition
func NewPartition(id uint64, repo *types.RepoShared, storage *RepoStorage) *Partition {
	return &Partition{
		id:             id,
		size:           0,
		svsPrefix:      nil,
		svsGroup:       nil,
		status:         Registered,
		commands:       types.NewRandomAccessDoubleLinkedList[string, *tlv.RepoCommand](),
		userSyncGroups: make(map[string]*PartitionSvs),
		repo:           repo,
		storage:        storage,
	}
}

// Starts the partition sync group
func (p *Partition) Start() (err error) {
	log.Info(p, "Starting partition SVS ALO")

	// get the svs prefix from the configuration
	p.svsPrefix = p.repo.RepoNameN.Append(enc.NewGenericComponent(strconv.FormatUint(p.id, 10)))
	if err != nil {
		return err
	}

	// initialize the SVS group
	p.svsGroup, err = ndn_sync.NewSvsALO(ndn_sync.SvsAloOpts{
		Name: p.repo.NodeNameN,
		Svs: ndn_sync.SvSyncOpts{
			Client:            p.repo.Client,
			GroupPrefix:       p.svsPrefix,
			SuppressionPeriod: 500 * time.Millisecond, // TODO: should this be reactive to the heartbeat interval?
			PeriodicTimeout:   30 * time.Second,       // TODO: this is the default value. To my understanding, periodic sync interests don't increase sequence numbers; it can be used as a redundancy to inform other nodes about the newest local state, but it can't replace the heartbeat mechanism
		},
		// Snapshot: &ndn_sync.SnapshotNodeLatest{
		// 	Client: p.client,
		// 	SnapMe: func(n enc.Name) (enc.Wire, error) {
		// 		return p.Snap(), nil
		// 	},
		// 	Threshold: 10, // TODO: configurable
		// },
		Snapshot: &ndn_sync.SnapshotNull{}, // TODO: starts with no snapshot
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
				p.CommitCommand(command)
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
			p.CommitCommand(command)
		}
	})

	// Announce group prefix route
	for _, route := range []enc.Name{
		p.svsGroup.SyncPrefix(),
		p.svsGroup.DataPrefix(),
	} {
		p.repo.Client.AnnouncePrefix(ndn.Announcement{
			Name:   route,
			Expose: true,
		})
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
			p.repo.Client.WithdrawPrefix(route, nil)
		}

		p.svsGroup = nil
	}

	return nil
}

// Snap takes a snapshot of this partition
// TODO: this is probably buggy and needs to be fixed
func (p *Partition) Snap() enc.Wire {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

	snap := tlv.PartitionSnapshot{
		Commands: make([]*tlv.RepoCommand, 0),
	}

	// TODO: optimization: if there is a delete command after insert, we don't need to include either in the snapshot
	for it := p.commands.Begin(); it != p.commands.End(); it = it.Next() {
		snap.Commands = append(snap.Commands, it.Value())
	}

	return snap.Encode()
}

// Commit operations ensure commands are persisted to the commands list
// However, it doesn't ensure the data is available yet. (i.e. the client could still be fetching data)
func (p *Partition) CommitCommand(command *tlv.RepoCommand) (err error) {
	switch command.CommandType {
	case "INSERT":
		return p.CommitInsert(command)
	case "DELETE":
		return p.CommitDelete(command)
	case "JOIN":
		return p.CommitJoin(command)
	case "LEAVE":
		return p.CommitLeave(command)
	}

	return fmt.Errorf("unknown command type: %s", command.CommandType)
}

// name: name of the data
// command: the command that inserted the data
// data: the data that was inserted
func (p *Partition) CommitInsert(command *tlv.RepoCommand) (err error) {
	if command.SrcName == nil || len(command.SrcName.Name) == 0 {
		return fmt.Errorf("missing data name")
	}

	p.mutex.Lock()
	defer p.mutex.Unlock()

	// Push to commands
	p.commands.PushBack(command.SrcName.Name.String(), command)
	p.storage.fetchDataHandler(command.SrcName.Name)

	return nil
}

func (p *Partition) CommitDelete(command *tlv.RepoCommand) (err error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	p.commands.Remove(command.SrcName.Name.String())

	return nil
}

func (p *Partition) CommitJoin(command *tlv.RepoCommand) (err error) {
	// TODO: join user sync group
	if command.SrcName == nil || len(command.SrcName.Name) == 0 {
		return fmt.Errorf("missing group name")
	}

	p.mutex.RLock()
	defer p.mutex.RUnlock()

	// Check if already started
	hash := command.SrcName.Name.String()
	if _, exists := p.userSyncGroups[hash]; exists {
		return nil
	}

	// Push to commands
	p.commands.PushBack(command.SrcName.Name.String(), command)

	// Start partition svs group
	svs := NewPartitionSvs(p.id, p.repo, command)
	if err := svs.Start(); err != nil {
		return err
	}
	p.userSyncGroups[hash] = svs

	return nil
}

func (p *Partition) CommitLeave(command *tlv.RepoCommand) (err error) {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

	// TODO: leave user sync group

	p.commands.Remove(command.SrcName.Name.String())

	return nil
}

// OwnsSvsGroup checks if the partition owns the given SVS group
func (p *Partition) OwnsSvsGroup(groupPrefix string) bool {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

	svs, exists := p.userSyncGroups[groupPrefix]
	return exists && svs.svs_alo != nil
}

// Checks if there are any commands that have not been fetched
// If there are, it will call the handler to fetch the relevant data
// TODO: actually call this periodically / on failure
func (p *Partition) CheckUnfetchedData() {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

	// Check if there are any commands that have not been fetched
	for it := p.commands.Begin(); it != p.commands.End(); it = it.Next() {
		command := it.Value()
		if command.CommandType == "INSERT" {
			if wire, _ := p.repo.Store.Get(command.SrcName.Name, true); wire == nil {
				p.storage.fetchDataHandler(command.SrcName.Name)
			}
		}
	}
}

// TODO: we need to have a similar method as CheckUnfetchedData for user sync groups
