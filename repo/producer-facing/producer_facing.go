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
	notifyPrefix          enc.Name
	notifyReplicasHandler func(*tlv.RepoCommand)
	processCommandHandler func(*tlv.RepoCommand)
}

func (p *RepoProducerFacing) String() string {
	return "producer-facing"
}

func NewProducerFacing(repo *types.RepoShared) *RepoProducerFacing {
	return &RepoProducerFacing{
		repo:         repo,
		notifyPrefix: repo.RepoNameN.Append(enc.NewGenericComponent("notify")),
	}
}

func (p *RepoProducerFacing) Start() error {
	log.Info(p, "Starting Repo Producer Facing")

	for _, prefix := range []enc.Name{
		p.notifyPrefix,
		p.repo.NodeNameN.Append(enc.NewGenericComponent(p.repo.RepoNameN.String())),
	} {
		p.repo.Client.AnnouncePrefix(ndn.Announcement{
			Name:   prefix,
			Expose: true,
		})
	}

	p.repo.Engine.AttachHandler(p.notifyPrefix, p.onRepoNotify)
	p.repo.Engine.AttachHandler(p.repo.NodeNameN.Append(enc.NewGenericComponent(p.repo.RepoNameN.String())), p.onNodeNotify)

	return nil
}

func (p *RepoProducerFacing) Stop() error {
	log.Info(p, "Stopping Repo Producer Facing")

	p.repo.Engine.DetachHandler(p.notifyPrefix)
	p.repo.Client.WithdrawPrefix(p.notifyPrefix, nil)

	return nil
}

// onRepoNotify is called when a repo notify interest is received
// This will distributes the command to responsible nodes
func (p *RepoProducerFacing) onRepoNotify(args ndn.InterestHandlerArgs) {
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
	log.Info(p, "Received command", "commandName", commandType, "srcName", srcName)

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

// onNodeNotify is called when a node notify interest is received
func (p *RepoProducerFacing) onNodeNotify(args ndn.InterestHandlerArgs) {
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
	log.Info(p, "Received responsible node command", "commandName", commandType, "srcName", srcName) // TODO: need a better name

	p.processCommandHandler(command)
}

// Handlers
func (p *RepoProducerFacing) SetOnNotifyReplicas(onNotifyReplicas func(*tlv.RepoCommand)) {
	p.notifyReplicasHandler = onNotifyReplicas
}

func (p *RepoProducerFacing) SetOnProcessCommand(onProcessCommand func(*tlv.RepoCommand)) {
	p.processCommandHandler = onProcessCommand
}
