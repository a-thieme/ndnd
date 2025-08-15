package producerfacing

import (
	"time"

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
	internalNotifyPrefixN enc.Name
	externalStatusPrefixN enc.Name
	internalStatusPrefixN enc.Name

	notifyReplicasHandler        func(*tlv.RepoCommand)
	processCommandHandler        func(*tlv.RepoCommand)
	externalStatusRequestHandler func(*ndn.InterestHandlerArgs, *tlv.RepoStatus)
	internalStatusRequestHandler func(*ndn.InterestHandlerArgs, *tlv.RepoStatus)
}

func (p *RepoProducerFacing) String() string {
	return "producer-facing"
}

func NewProducerFacing(repo *types.RepoShared) *RepoProducerFacing {
	externalNotifyPrefixN := repo.RepoNameN.Append(enc.NewGenericComponent("notify"))
	internalNotifyPrefixN := repo.NodeNameN.Append(externalNotifyPrefixN...)

	externalStatusPrefixN := repo.RepoNameN.Append(enc.NewGenericComponent("status"))
	internalStatusPrefixN := repo.NodeNameN.Append(externalStatusPrefixN...)

	return &RepoProducerFacing{
		repo:                  repo,
		externalNotifyPrefixN: externalNotifyPrefixN,
		internalNotifyPrefixN: internalNotifyPrefixN,
		externalStatusPrefixN: externalStatusPrefixN,
		internalStatusPrefixN: internalStatusPrefixN,
	}
}

func (p *RepoProducerFacing) Start() error {
	log.Info(p, "Starting Repo Producer Facing")

	// Announce command & status request handler prefixes
	for _, prefix := range []enc.Name{
		p.externalNotifyPrefixN,
		p.internalNotifyPrefixN,
		p.externalStatusPrefixN,
		p.internalStatusPrefixN,
	} {
		p.repo.Client.AnnouncePrefix(ndn.Announcement{
			Name:   prefix,
			Expose: true,
		})
	}

	// Register command handler prefixes
	p.repo.Engine.AttachHandler(p.externalNotifyPrefixN, p.onExternalNotify)
	p.repo.Engine.AttachHandler(p.internalNotifyPrefixN, p.onInternalNotify)

	// Register status request handler prefixes
	p.repo.Engine.AttachHandler(p.externalStatusPrefixN, p.onExternalStatusRequest)
	p.repo.Engine.AttachHandler(p.internalStatusPrefixN, p.onInternalStatusRequest)

	return nil
}

func (p *RepoProducerFacing) Stop() error {
	log.Info(p, "Stopping Repo Producer Facing")

	// Unregister command & status request handler prefixes
	for _, prefix := range []enc.Name{
		p.externalNotifyPrefixN,
		p.internalNotifyPrefixN,
		p.externalStatusPrefixN,
		p.internalStatusPrefixN,
	} {
		p.repo.Engine.DetachHandler(prefix)
		p.repo.Client.WithdrawPrefix(prefix, nil)
	}

	return nil
}

// onExternalNotify is called when a repo notify interest is received
// This will distributes the command to responsible nodes
func (p *RepoProducerFacing) onExternalNotify(args ndn.InterestHandlerArgs) {
	log.Info(p, "Received repo notify interest", "interest", args.Interest.Name().String())
	interest := args.Interest

	if interest.AppParam() == nil {
		log.Warn(p, "Notify interest has no app param, ignoring")
		return
	}

	command, err := tlv.ParseRepoCommand(enc.NewWireView(interest.AppParam()), true)
	if err != nil {
		log.Warn(p, "Failed to parse notify app param", "err", err)
		return
	}

	commandType := command.CommandType
	srcName := command.SrcName.Name
	log.Info(p, "Received external command", "commandName", commandType, "srcName", srcName)

	// TODO: check digest?

	p.notifyReplicasHandler(command) // notify responsible replicas

	// Reply to the command - "Repo has received the command"
	data, err := p.repo.Engine.Spec().MakeData(
		interest.Name(),
		&ndn.DataConfig{
			ContentType: optional.Some(ndn.ContentTypeBlob),
			Freshness:   optional.Some(10 * time.Second),
		},
		enc.Wire{[]byte{}},
		nil,
	)
	if err != nil {
		log.Error(p, "Failed to make reply data", "err", err)
		return
	}
	args.Reply(data.Wire)
}

// onInternalNotify is called when an internal notify interest is received
func (p *RepoProducerFacing) onInternalNotify(args ndn.InterestHandlerArgs) {
	interest := args.Interest

	if interest.AppParam() == nil {
		log.Warn(p, "Notify interest has no app param, ignoring")
		return
	}

	command, err := tlv.ParseRepoCommand(enc.NewWireView(interest.AppParam()), true)

	if err != nil {
		log.Warn(p, "Failed to parse command", "err", err)
		return
	}

	commandType := command.CommandType
	srcName := command.SrcName.Name
	log.Info(p, "Received responsible internal command", "commandName", commandType, "srcName", srcName) // TODO: need a better name

	p.processCommandHandler(command)
}

// Handlers
func (p *RepoProducerFacing) SetOnNotifyReplicas(onNotifyReplicas func(*tlv.RepoCommand)) {
	p.notifyReplicasHandler = onNotifyReplicas
}

func (p *RepoProducerFacing) SetOnProcessCommand(onProcessCommand func(*tlv.RepoCommand)) {
	p.processCommandHandler = onProcessCommand
}
