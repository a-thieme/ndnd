package storage

import (
	"fmt"

	"github.com/named-data/ndnd/repo/tlv"
	"github.com/named-data/ndnd/repo/types"
	// enc "github.com/named-data/ndnd/std/encoding"
	"github.com/named-data/ndnd/std/log"
	ndn_sync "github.com/named-data/ndnd/std/sync"
)

// NOTE: this is a wrapper around the SvsALO. If it turns out we don't need much
//  more functionality, we can just replace this with SvsALO and let partition directly manages it.

// PartitionSvs handles a SVS group specified by a join sync command
type PartitionSvs struct {
	partitionId uint64
	repo        *types.RepoShared
	command     *tlv.RepoCommand
	svs_alo     *ndn_sync.SvsALO
}

func NewPartitionSvs(partitionId uint64, repo *types.RepoShared, command *tlv.RepoCommand) *PartitionSvs {
	return &PartitionSvs{
		partitionId: partitionId,
		repo:        repo,
		command:     command,
	}
}

func (p *PartitionSvs) String() string {
	return fmt.Sprintf("partition-svs (%s)", p.command.SrcName.Name)
}

func (p *PartitionSvs) Start() (err error) {
	// TODO: start the svsalo
	log.Info(p, "Starting SVS", "partitionId", p.partitionId, "group", p.command.SrcName.Name)

	// Parse snapshot configuration
	// var snapshot ndn_sync.Snapshot = nil

	// History snapshot
	// if p.command.HistorySnapshot != nil {
	// 	if t := p.command.HistorySnapshot.Threshold; t < 10 {
	// 		return fmt.Errorf("invalid history snapshot threshold: %d", t)
	// 	}

	// 	snapshot = &ndn_sync.SnapshotNodeHistory{
	// 		Client:    p.repo.Client,
	// 		Threshold: p.command.HistorySnapshot.Threshold,
	// 		IsRepo:    true,
	// 	}
	// }

	// snapshot = &ndn_sync.SnapshotNodeHistory{
	// 	Client:    p.repo.Client,
	// 	Threshold: 10, // TODO: this may cause issues, we should get this from the command
	// 	IsRepo:    true,
	// }

	// TODO: need to get this from the command
	// var multicastPrefix enc.Name = nil

	// Start SVS ALO
	// p.svs_alo, err = ndn_sync.NewSvsALO(ndn_sync.SvsAloOpts{
	// 	Name:         enc.Name{enc.NewKeywordComponent("repo")}, // unused
	// 	InitialState: p.readState(),
	// 	Svs: ndn_sync.SvSyncOpts{
	// 		Client:            p.repo.Client,
	// 		GroupPrefix:       p.command.SrcName.Name,
	// 		SuppressionPeriod: 500 * time.Millisecond,
	// 		PeriodicTimeout:   365 * 24 * time.Hour, // basically never
	// 		Passive:           true,
	// 	},
	// 	Snapshot:        snapshot,
	// 	MulticastPrefix: multicastPrefix,
	// })

	return nil
}

func (p *PartitionSvs) Stop() (err error) {
	// // TODO: stop the svsalo
	// log.Info(p, "Stopping SVS: %s", p.svsalo.SyncPrefix())

	// if err := p.svsalo.Stop(); err != nil {
	// 	log.Error(p, "Failed to stop SVS", "err", err)
	// 	return err
	// }

	// p.svsalo = nil
	return nil
}
