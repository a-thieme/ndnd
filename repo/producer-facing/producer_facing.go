package producerfacing

import (
	"github.com/named-data/ndnd/repo/tlv"
	"github.com/named-data/ndnd/repo/types"
	enc "github.com/named-data/ndnd/std/encoding"
	"github.com/named-data/ndnd/std/log"
	"github.com/named-data/ndnd/std/ndn"
)

// RepoProducerFacing is the producer facing component of the repo
// It is responsible for announcing the repo prefix and handling the notify interests
// It also handles the direct command interests from the node
type RepoProducerFacing struct {
	repo   *types.RepoShared
	notify enc.Name
	status enc.Name

	newCommandCallback func(*tlv.RepoCommand)
	statusCallback     func(name enc.Name, content enc.Wire, reply func(wire enc.Wire) error)
}

func (p *RepoProducerFacing) String() string {
	return "producer-facing"
}

func NewProducerFacing(repo *types.RepoShared) *RepoProducerFacing {
	notify := repo.RepoNameN.Append(enc.NewGenericComponent("notify"))
	status := repo.RepoNameN.Append(enc.NewGenericComponent("status"))

	return &RepoProducerFacing{
		repo:   repo,
		notify: notify,
		status: status,
	}
}

func (p *RepoProducerFacing) SetCommandHandler(cb func(*tlv.RepoCommand)) {
	p.newCommandCallback = cb
}

func (p *RepoProducerFacing) SetStatusHandler(cb func(name enc.Name, content enc.Wire, reply func(wire enc.Wire) error)) {
	p.statusCallback = cb
}

func (p *RepoProducerFacing) Start() error {
	log.Info(p, "Starting Repo Producer Facing")

	// Announce command & status request handler prefixes
	log.Debug(p, "Announcing Prefixes")
	for _, prefix := range []enc.Name{p.notify, p.status} {
		p.repo.Client.AnnouncePrefix(ndn.Announcement{
			Name:   prefix,
			Expose: true,
		})
	}

	// Register command handler prefixes
	log.Debug(p, "Attaching command handlers")
	p.repo.Client.AttachCommandHandler(p.notify, p.onCommand)
	p.repo.Client.AttachCommandHandler(p.status, p.statusCheck)
	log.Trace(p, "end of Start()")
	return nil
}

func (p *RepoProducerFacing) Stop() error {
	log.Info(p, "Stopping Repo Producer Facing")

	// Unregister command & status request handler prefixes
	log.Debug(p, "Detaching handlers and withdrawing prefixes")
	for _, prefix := range []enc.Name{p.notify, p.status} {
		p.repo.Client.DetachCommandHandler(prefix)
		p.repo.Client.WithdrawPrefix(prefix, nil)
	}

	log.Trace(p, "end of Stop()")
	return nil
}

// onExternalNotify is called when a repo notify interest is received
func (p *RepoProducerFacing) onCommand(name enc.Name, content enc.Wire, reply func(wire enc.Wire) error) {
	log.Info(p, "got new command, interest name", name)

	command, err := tlv.ParseRepoCommand(enc.NewWireView(content), false)
	if err != nil {
		log.Trace(p, "Failed to parse notify app param", "err", err)
		return
	}

	log.Debug(p, "making response")
	sr := &tlv.RepoStatusResponse{
		Target: command.Target,
		Status: "recieved",
	}

	log.Debug(p, "responding")
	reply(sr.Encode())
	log.Debug(p, "running new command callback")
	p.newCommandCallback(command)
	log.Trace(p, "end of onCommand")
}

func (p *RepoProducerFacing) statusCheck(name enc.Name, content enc.Wire, reply func(wire enc.Wire) error) {
	log.Info(p, "doing status check for %s", name)
	p.statusCallback(name, content, reply)
	log.Trace(p, "after status callback for %s", name)
}
