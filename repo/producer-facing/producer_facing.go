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

	p.client.Engine().AttachHandler(p.notifyPrefix, p.onNotify)
	p.client.AnnouncePrefix(ndn.Announcement{
		Name:   p.notifyPrefix,
		Expose: true,
	})

	return nil
}

func (p *RepoProducerFacing) Stop() error {
	log.Info(p, "Stopping Repo Producer Facing")

	p.client.Engine().DetachHandler(p.notifyPrefix)
	p.client.WithdrawPrefix(p.notifyPrefix, nil)

	return nil
}

func (p *RepoProducerFacing) onNotify(args ndn.InterestHandlerArgs) {
	// I1: parse
	interest := args.Interest

	if interest.AppParam() == nil {
		log.Warn(p, "Notify interest has no app param, ignoring")
		return
	}

	notifyParam, err := tlv.ParseRepoNotify(enc.NewWireView(interest.AppParam()), true)
	if err != nil {
		log.Warn(p, "Failed to parse notify app param")
		return
	}

	// TODO: check digest?

	// I2: fetch command data
	// TODO: retry fetching if failed, even across restarts
	// TODO: do not fetch commands that are too large
	go p.client.Consume(notifyParam.CommandName.Name, func(status ndn.ConsumeState) {
		if status.Error() != nil {
			log.Warn(p, "Command fetch error", "err", status.Error(), "name", notifyParam.CommandName.Name)
			return
		}
		log.Info(p, "Command fetch success", "name", notifyParam.CommandName.Name)

		command, err := tlv.ParseRepoCommand(enc.NewWireView(status.Content()), true)
		if err != nil {
			log.Info(p, "Failed to parse command data")
			return
		}

		reply, err := p.onProducerCommand(command)
		if err != nil {
			log.Info(p, "Failed to parse repo command", err)
			return
		}

		args.Reply(reply)
	})
}

func (p *RepoProducerFacing) onProducerCommand(command *tlv.RepoCommand) (enc.Wire, error) {
	// forward commands to responsible replicas
	p.notifyReplicasHandler(command)
	return nil, nil
}

func (p *RepoProducerFacing) SetOnNotifyReplicas(onNotifyReplicas func(*tlv.RepoCommand)) {
	p.notifyReplicasHandler = onNotifyReplicas
}
