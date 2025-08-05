package repo

import (
	"fmt"

	"github.com/named-data/ndnd/repo/tlv"
	"github.com/named-data/ndnd/repo/utils"
	enc "github.com/named-data/ndnd/std/encoding"
	"github.com/named-data/ndnd/std/log"
	"github.com/named-data/ndnd/std/ndn"
)

// onMgmtCmd handles repo commands sent to the node
// TODO: it should have different procedures for single/distributed mode
func (r *Repo) onMgmtCmd(_ enc.Name, wire enc.Wire, reply func(enc.Wire) error) {
	cmd, err := tlv.ParseRepoCmd(enc.NewWireView(wire), false)
	if err != nil {
		log.Warn(r, "Failed to parse management command", "err", err)
		return
	}

	if cmd.SyncJoin != nil {
		go r.handleSyncJoin(cmd.SyncJoin, reply)
		return
	}

	log.Warn(r, "Unknown management command received")
}

// TODO: handle repo notify interest
func (r *Repo) onRepoNotify(args ndn.InterestHandlerArgs) {
	// I1: parse
	interest := args.Interest

	if interest.AppParam() == nil {
		log.Warn(r, "Notify interest has no app param, ignoring")
		return
	}

	notifyParam, err := tlv.ParseRepoNotify(enc.NewWireView(interest.AppParam()), true)
	if err != nil {
		log.Warn(r, "Failed to parse notify app param")
		return
	}

	// TODO: check digest?

	// I2: fetch command data
	// TODO: retry fetching if failed, even across restarts
	// TODO: do not fetch commands that are too large
	go r.client.Consume(notifyParam.CommandName.Name, func(status ndn.ConsumeState) {
		if status.Error() != nil {
			log.Warn(r, "Command fetch error", "err", status.Error(), "name", notifyParam.CommandName.Name)
			return
		}
		log.Info(r, "Command fetch success", "name", notifyParam.CommandName.Name)

		command, err := tlv.ParseRepoCommand(enc.NewWireView(status.Content()), true)
		if err != nil {
			log.Info(r, "Failed to parse command data")
			return
		}

		reply, err := r.onRepoCommand(command)
		if err != nil {
			log.Info(r, "Failed to parse repo command", err)
			return
		}

		args.Reply(reply)
	})
}

// TODO: handle repo command interest
func (r *Repo) onRepoCommand(command *tlv.RepoCommand) (enc.Wire, error) {
	name := command.TargetName.Name
	partition := utils.PartitionIdFromEncName(name, 32) // TODO: num of partitions should be from configuration
	replicas := r.awareness.GetReplicas(partition)

	// forward commands to responsible replicas
	for _, replica := range replicas {
		forward_interest, _ := r.engine.Spec().MakeInterest(replica, &ndn.InterestConfig{
			// TODO: interest configs?
			// TODO: nonce?
		}, nil, nil)

		r.engine.Express(forward_interest, func(ndn.ExpressCallbackArgs) {
			// TODO: handle callbacks
		})
	}

	// TODO: quorum-based status -> reply I1 notify only after meeting quorum
	return nil, nil
}

func (r *Repo) handleSyncJoin(cmd *tlv.SyncJoin, reply func(enc.Wire) error) {
	res := tlv.RepoCmdRes{Status: 200}

	if cmd.Protocol != nil && cmd.Protocol.Name.Equal(tlv.SyncProtocolSvsV3) {
		if err := r.startSvs(cmd); err != nil {
			res.Status = 500
			log.Error(r, "Failed to start SVS", "err", err)
		}
		reply(res.Encode())
		return
	}

	log.Warn(r, "Unknown sync protocol specified in command", "protocol", cmd.Protocol)
	res.Status = 400
	reply(res.Encode())
}

func (r *Repo) startSvs(cmd *tlv.SyncJoin) error {
	if cmd.Group == nil || len(cmd.Group.Name) == 0 {
		return fmt.Errorf("missing group name")
	}

	r.mutex.Lock()
	defer r.mutex.Unlock()

	// Check if already started
	hash := cmd.Group.Name.TlvStr()
	if _, ok := r.groupsSvs[hash]; ok {
		return nil
	}

	// Start group
	svs := NewRepoSvs(r.config, r.client, cmd)
	if err := svs.Start(); err != nil {
		return err
	}
	r.groupsSvs[hash] = svs

	return nil
}
