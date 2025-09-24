package storage

import (
	"fmt"
	"time"

	"github.com/named-data/ndnd/repo/tlv"
	"github.com/named-data/ndnd/repo/types"
	enc "github.com/named-data/ndnd/std/encoding"
	"github.com/named-data/ndnd/std/log"
	"github.com/named-data/ndnd/std/ndn"
	// "github.com/named-data/ndnd/std/ndn/svs_ps"
	ndn_sync "github.com/named-data/ndnd/std/sync"
)

// NOTE: this is a wrapper around the SvsALO. If it turns out we don't need much
//  more functionality, we can just replace this with SvsALO and let partition directly manages it.

const (
	// hack
	multicastPrefix = "/ndn/multicast"
	// this should be higher so repo is only used as a fallback option
	repoRouteCost = 1000
)

// UserSvs handles a SVS group specified by a join sync command
type UserSvs struct {
	partitionId uint64
	repo        *types.RepoShared
	command     *tlv.RepoCommand
	svs_alo     *ndn_sync.SvsALO
}

func NewUserSvs(repo *types.RepoShared, command *tlv.RepoCommand) *UserSvs {
	return &UserSvs{
		repo:    repo,
		command: command,
	}
}

func (p *UserSvs) String() string {
	return fmt.Sprintf("user-svs (%s)", p.command.Target)
}

func (p *UserSvs) Start() (err error) {
	// start the svsalo
	log.Info(p, "Starting user SVS", "partitionId", p.partitionId, "group", p.command.Target)

	// Parse snapshot configuration
	var snapshot ndn_sync.Snapshot = nil

	// History snapshot
	threshold := p.command.SnapshotThreshold
	if threshold < 10 {
		return fmt.Errorf("invalid history snapshot threshold: %d", threshold)
	}

	snapshot = &ndn_sync.SnapshotNodeHistory{
		Client:    p.repo.Client,
		Threshold: threshold,
		IsRepo:    true,
	}

	// Start SVS ALO
	multicastPrefixN, _ := enc.NameFromStr(multicastPrefix)
	p.svs_alo, _ = ndn_sync.NewSvsALO(ndn_sync.SvsAloOpts{
		Name:         enc.Name{enc.NewKeywordComponent("repo")}, // unused
		InitialState: p.readState(),
		Svs: ndn_sync.SvSyncOpts{
			Client:            p.repo.Client,
			GroupPrefix:       p.command.Target.Clone(),
			SuppressionPeriod: 500 * time.Millisecond,
			PeriodicTimeout:   365 * 24 * time.Hour, // basically never
			Passive:           true,
		},
		Snapshot:        snapshot,
		MulticastPrefix: multicastPrefixN,
	})

	// Subscribe to all publishers
	// FIXME: this has a blank name as a prefix
	p.svs_alo.SubscribePublisher(enc.Name{}, func(pub ndn_sync.SvsPub) {
		// TODO: this content is the actual published content, right?
		log.Debug(p, "got a publication from", pub.Publisher, "at state", pub.State.Join())
		p.commitState(pub.State)
	})

	// Set up error handler
	p.svs_alo.SetOnError(func(err error) {
		log.Error(p, "User SVS ALO error", "err", err)
	})

	// Announce prefixes
	for _, prefix := range []enc.Name{
		p.svs_alo.GroupPrefix(),
		p.svs_alo.SyncPrefix(),
	} {
		p.repo.Client.AnnouncePrefix(ndn.Announcement{
			Name:    prefix,
			Cost:    repoRouteCost,
			Expose:  true,
			OnError: nil, // TODO
		})
	}

	return nil
}

func (p *UserSvs) Stop() (err error) {
	// TODO: stop the svsalo
	log.Info(p, "Stopping user SVS", "group", p.svs_alo.SyncPrefix())

	// Withdraw handlers
	for _, prefix := range []enc.Name{
		p.svs_alo.GroupPrefix(),
		p.svs_alo.SyncPrefix(),
	} {
		p.repo.Client.WithdrawPrefix(prefix, nil)
	}

	if p.svs_alo != nil {
		// Stop sync groups
		if err := p.svs_alo.Stop(); err != nil {
			log.Error(p, "Failed to stop user SVS", "err", err)
			return err
		}
	}

	p.svs_alo = nil
	return nil
}

func (r *UserSvs) commitState(state enc.Wire) {
	name := r.command.Target.Append(enc.NewKeywordComponent("alo-state"))
	r.repo.Client.Store().Put(name, state.Join())
}

// NOTE: only used for getting the initial state of a group. only helpful if rejoining a group AND not removing data when you leave a group
func (r *UserSvs) readState() enc.Wire {
	name := r.command.Target.Append(enc.NewKeywordComponent("alo-state"))
	if stateWire, _ := r.repo.Client.Store().Get(name, false); stateWire != nil {
		return enc.Wire{stateWire}
	}
	return nil
}
