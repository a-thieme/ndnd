package producerfacing

import (
	"github.com/named-data/ndnd/repo/tlv"
	enc "github.com/named-data/ndnd/std/encoding"
	"github.com/named-data/ndnd/std/log"
	"github.com/named-data/ndnd/std/ndn"
)

type RepoProducerFacing struct {
	repoNameN enc.Name
	nodeNameN enc.Name

	notifyPrefix          enc.Name
	notifyReplicasHandler func(*tlv.RepoCommand)
	processCommandHandler func(*tlv.RepoCommand)

	client ndn.Client
}

func (p *RepoProducerFacing) String() string {
	return "producer-facing"
}

func NewProducerFacing(repoNameN enc.Name, nodeNameN enc.Name, client ndn.Client) *RepoProducerFacing {
	return &RepoProducerFacing{
		repoNameN:    repoNameN,
		nodeNameN:    nodeNameN,
		client:       client,
		notifyPrefix: repoNameN.Append(enc.NewGenericComponent("notify")),
	}
}

func (p *RepoProducerFacing) Start() error {
	log.Info(p, "Starting Repo Producer Facing")

	for _, prefix := range []enc.Name{
		p.notifyPrefix,
		p.nodeNameN.Append(enc.NewGenericComponent(p.repoNameN.String())),
	} {
		p.client.AnnouncePrefix(ndn.Announcement{
			Name:   prefix,
			Expose: true,
		})
	}

	p.client.Engine().AttachHandler(p.notifyPrefix, p.onRepoNotify)
	p.client.Engine().AttachHandler(p.nodeNameN.Append(enc.NewGenericComponent(p.repoNameN.String())), p.onNodeNotify)

	return nil
}

func (p *RepoProducerFacing) Stop() error {
	log.Info(p, "Stopping Repo Producer Facing")

	p.client.Engine().DetachHandler(p.notifyPrefix)
	p.client.WithdrawPrefix(p.notifyPrefix, nil)

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
