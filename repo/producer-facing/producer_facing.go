package producerfacing

import (
	"github.com/named-data/ndnd/repo/tlv"
	"github.com/named-data/ndnd/repo/types"
	enc "github.com/named-data/ndnd/std/encoding"
	"github.com/named-data/ndnd/std/log"
	"github.com/named-data/ndnd/std/ndn"
	"github.com/named-data/ndnd/std/types/optional"
)

// RepoProducerFacing is the producer facing component of the repo
// It is responsible for announcing the repo prefix and handling the notify interests
// It also handles the direct command interests from the node
type RepoProducerFacing struct {
	repo                  *types.RepoShared
	externalNotifyPrefixN enc.Name
	// TODO: add status checking
	//externalStatusPrefixN enc.Name

	newCommandCallback func(*tlv.RepoCommand)
}

func (p *RepoProducerFacing) String() string {
	return "producer-facing"
}

func NewProducerFacing(repo *types.RepoShared) *RepoProducerFacing {
	externalNotifyPrefixN := repo.RepoNameN.Append(enc.NewGenericComponent("notify"))

	return &RepoProducerFacing{
		repo:                  repo,
		externalNotifyPrefixN: externalNotifyPrefixN,
	}
}

func (p *RepoProducerFacing) SetCommandHandler(cb func(*tlv.RepoCommand)) {
	p.newCommandCallback = cb
}

func (p *RepoProducerFacing) Start() error {
	log.Info(p, "Starting Repo Producer Facing")

	// Announce command & status request handler prefixes
	for _, prefix := range []enc.Name{p.externalNotifyPrefixN} {
		p.repo.Client.AnnouncePrefix(ndn.Announcement{
			Name:   prefix,
			Expose: true,
		})
	}

	// Register command handler prefixes
	p.repo.Engine.AttachHandler(p.externalNotifyPrefixN, p.onExternalNotify)

	return nil
}

func (p *RepoProducerFacing) Stop() error {
	log.Info(p, "Stopping Repo Producer Facing")

	// Unregister command & status request handler prefixes
	for _, prefix := range []enc.Name{p.externalNotifyPrefixN} {
		p.repo.Engine.DetachHandler(p.externalNotifyPrefixN)
		p.repo.Client.WithdrawPrefix(prefix, nil)
	}

	return nil
}

// onExternalNotify is called when a repo notify interest is received
func (p *RepoProducerFacing) onExternalNotify(args ndn.InterestHandlerArgs) {
	interest := args.Interest

	if interest.AppParam() == nil {
		log.Trace(p, "Notify interest has no app param, ignoring")
		return
	}

	// TODO: do trust schema validation here
	command, err := tlv.ParseRepoCommand(enc.NewWireView(interest.AppParam()), true)
	if err != nil {
		log.Trace(p, "Failed to parse notify app param", "err", err)
		return
	}

	log.Info(p, "Received external notify command", command)

	// Reply to the command - "Repo has received the command"
	sr := &tlv.RepoStatusResponse{
		Target: command.Target,
		Status: "recieved",
	}

	data, err := p.repo.Engine.Spec().MakeData(
		interest.Name(),
		&ndn.DataConfig{
			ContentType: optional.Some(ndn.ContentTypeBlob), // TODO: see if this needs to be set
		},
		sr.Encode(),
		nil, // TODO: sign this data
	)
	if err != nil {
		log.Error(p, "Failed to make reply data", "err", err)
		return
	}
	args.Reply(data.Wire)
	log.Debug(p, "replied with data", sr)
	p.newCommandCallback(command)
	log.Debug(p, "after callback for command", command)
}
