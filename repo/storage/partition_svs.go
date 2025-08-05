package storage

import (
	"fmt"

	"github.com/named-data/ndnd/repo/tlv"
	"github.com/named-data/ndnd/std/log"
	ndn_sync "github.com/named-data/ndnd/std/sync"
)

// NOTE: this is a wrapper around the SvsALO. If it turns out we don't need much
//  more functionality, we can just replace this with SvsALO and let partition directly manages it.

// PartitionSvs handles a SVS group specified by a join sync command
type PartitionSvs struct {
	svsalo  *ndn_sync.SvsALO
	command *tlv.RepoCommand
}

func NewPartitionSvs(svsalo *ndn_sync.SvsALO, command *tlv.RepoCommand) *PartitionSvs {
	return &PartitionSvs{
		svsalo:  svsalo,
		command: command,
	}
}

func (p *PartitionSvs) String() string {
	return fmt.Sprintf("partition-svs (%s)", p.svsalo.SyncPrefix())
}

func (p *PartitionSvs) Start() (err error) {
	// TODO: start the svsalo
	log.Info(p, "Starting SVS: %s", p.svsalo.SyncPrefix())

	// Parse snapshot configuration
	// var snapshot ndn_sync.Snapshot = nil

	// History snapshot

	return nil
}

func (p *PartitionSvs) Stop() (err error) {
	// TODO: stop the svsalo
	log.Info(p, "Stopping SVS: %s", p.svsalo.SyncPrefix())

	if err := p.svsalo.Stop(); err != nil {
		log.Error(p, "Failed to stop SVS", "err", err)
		return err
	}

	p.svsalo = nil
	return nil
}
