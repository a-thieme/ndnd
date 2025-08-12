package storage

import (
	"github.com/named-data/ndnd/repo/tlv"
	"github.com/named-data/ndnd/std/log"
)

// HandleCommand handles a repo command
// For now, it may be insert/delete (data) or join/leave (sync)
// Thread-safe
func (p *Partition) HandleCommand(command *tlv.RepoCommand) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	// TODO: the naming of commands is not finalized
	switch command.CommandName.Name.String() {
	case "INSERT":
		p.HandleInsert(command)
	case "DELETE":
		p.HandleDelete(command)
	case "JOIN":
		p.HandleJoin(command)
	case "LEAVE":
		p.HandleLeave(command)
	}

	// TODO: this assumes sync prefix can't be the same as data prefix
	p.commands[command.SrcName.Name.String()] = command
}

// HandleInsert handles an insert (data) command
// Thread-safety is handled by the caller
func (p *Partition) HandleInsert(command *tlv.RepoCommand) {
	// TODO: handle insert
	log.Info(p, "Inserting data", "command", command)

	// TODO: fetch data from the client
}

// HandleDelete handles a delete (data) command
// Thread-safety is handled by the caller
func (p *Partition) HandleDelete(command *tlv.RepoCommand) {
	log.Info(p, "Deleting data", "command", command)

	if err := p.store.Remove(command.SrcName.Name); err != nil {
		log.Error(p, "Unable to remove data", "err", err)
	}

	// Remove the previous insertion command if it exists
	delete(p.commands, command.SrcName.Name.String())
}

// HandleJoin handles a join (sync group) command
// Thread-safety is handled by the caller
func (p *Partition) HandleJoin(command *tlv.RepoCommand) {
	log.Info(p, "Joining sync group", "command", command)

	partitionsvs := NewPartitionSvs(p.svsGroup, command)
	if err := partitionsvs.Start(); err != nil {
		log.Error(p, "Unable to start partition SVS", "err", err)
	}

	p.userSyncGroups[command.SrcName.Name.String()] = partitionsvs
}

// HandleLeave handles a leave (sync group) command
// Thread-safety is handled by the caller
func (p *Partition) HandleLeave(command *tlv.RepoCommand) {
	log.Info(p, "Leaving sync group", "command", command)

	// Stop the user sync group
	partitionsvs, ok := p.userSyncGroups[command.SrcName.Name.String()]
	if !ok {
		log.Error(p, "Sync group not found", "command", command)
		return
	}

	if err := partitionsvs.Stop(); err != nil {
		log.Error(p, "Unable to stop partition SVS", "err", err)
	}

	// Remove the user sync group
	delete(p.userSyncGroups, command.SrcName.Name.String())

	// Remove the previous insertion command if it exists
	delete(p.commands, command.SrcName.Name.String())
}
