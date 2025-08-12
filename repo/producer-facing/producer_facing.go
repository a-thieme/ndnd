package producerfacing

import (
	"github.com/named-data/ndnd/repo/tlv"
	"github.com/named-data/ndnd/repo/types"
	enc "github.com/named-data/ndnd/std/encoding"
	"github.com/named-data/ndnd/std/log"
	"github.com/named-data/ndnd/std/ndn"
)

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
	interest := args.Interest

	if interest.AppParam() == nil {
		log.Warn(p, "Notify interest has no app param, ignoring")
		return
	}

	notifyParam, err := tlv.ParseRepoNotify(enc.NewWireView(interest.AppParam()), true)

	if err != nil {
		log.Warn(p, "Failed to parse notify app param", "err", err)
		return
	}

	command := notifyParam.Command
	commandName := command.CommandName.Name
	srcName := command.SrcName.Name
	log.Info(p, "Received command", "commandName", commandName, "srcName", srcName)

	// TODO: check digest?

	p.notifyReplicasHandler(command) // notify responsible replicas
}

// onNodeNotify is called when a node notify interest is received
func (p *RepoProducerFacing) onNodeNotify(args ndn.InterestHandlerArgs) {
	interest := args.Interest

	if interest.AppParam() == nil {
		log.Warn(p, "Notify interest has no app param, ignoring")
		return
	}

	notifyParam, err := tlv.ParseRepoNotify(enc.NewWireView(interest.AppParam()), true)

	if err != nil {
		log.Warn(p, "Failed to parse command", "err", err)
		return
	}

	command := notifyParam.Command
	commandName := command.CommandName.Name
	srcName := command.SrcName.Name
	log.Info(p, "Received direct command", "commandName", commandName, "srcName", srcName) // TODO: need to come up with a better name

	p.processCommandHandler(command)
}

// Handlers
func (p *RepoProducerFacing) SetOnNotifyReplicas(onNotifyReplicas func(*tlv.RepoCommand)) {
	p.notifyReplicasHandler = onNotifyReplicas
}

func (p *RepoProducerFacing) SetOnProcessCommand(onProcessCommand func(*tlv.RepoCommand)) {
	p.processCommandHandler = onProcessCommand
}
