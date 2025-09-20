package management

import (
	"golang.org/x/sys/unix" // POSIX system
	"os"

	"github.com/named-data/ndnd/repo/tlv"
	enc "github.com/named-data/ndnd/std/encoding"
	"github.com/named-data/ndnd/std/log"
	"github.com/named-data/ndnd/std/ndn"
)

// NOTE: every handler registered by the management module will be ran in a separate goroutine, so blocking is not a concern

func (m *RepoManagement) CheckJob(job *tlv.RepoCommand) string {
	num := m.awareness.Storage.GetReplications(job)
	if m.storage.DoingJob(job) {
		num++
	}
	if num < m.repo.NumReplicas {
		return "under"
	} else if num > m.repo.NumReplicas {
		return "over"
	}
	return "good"
}

// Launches a job to fetch application data from the network
// TODO: make sure this does what it's supposed to do
// TODO: this should not be called if the data is already in the storage...
// FIXME: what do we do here...what calls this?
func (m *RepoManagement) FetchDataHandler(dataNameN enc.Name) {
	log.Info(m, "Fetching data", "name", dataNameN)
	// haotian: the assumption here is that command can be committed to the group state before the data is available, hence we keep retrying to fetch the relevant data. This, however, cause unnecessary traffics. Also, if the producer, for any reason, send the command again, we need to fetch the data again immediately, so the handler's id should not be tied to the data name.
	// at: tie it to the data name, reset the wait period if you need to fetch the data immediately
}

// Launches a coordination job to check the status from responsible node
func (m *RepoManagement) StatusRequestHandler(interestHandler *ndn.InterestHandlerArgs, target *enc.Name) {
	log.Info(m, "Got status request for", target)
	// FIXME: actually implement this to check replication for this target
	//checkJob(target)

	// Prepare reply
	reply := tlv.RepoStatusResponse{
		Target: target,
		Status: 200,
	}

	data, _ := m.repo.Engine.Spec().MakeData(
		interestHandler.Interest.Name(),
		&ndn.DataConfig{},
		reply.Encode(),
		nil, // TODO: security
	)

	interestHandler.Reply(data.Wire)
}

// FIXME: change to take in RepoCommand instead of string
func (m *RepoManagement) GetAvailability(job string) int {
	// Get storage state
	var stat unix.Statfs_t
	wd, _ := os.Getwd()
	unix.Statfs(wd, &stat)

	// Get free spaces
	// FIXME: if you are already doing the job, add more to the availability
	freeSpace := stat.Bavail * uint64(stat.Bsize)
	log.Info(m, "Bid: free space", freeSpace, "for job", job)

	// Calculate bid
	bid := int(freeSpace)

	return bid
}

// FIXME: see if these calls need go routines
func (m *RepoManagement) OnNewJob(job *tlv.RepoCommand) {
	// do job if you have the resources
	m.DoJob(job)
	// publish command to the commands SVS group, since it's new
	m.commands.PublishCommand(job)
}

// WonAuction prompts the awareness and storage module to register a new partition, as the result of winning auctions
// FIXME: see if these calls need go routines
func (m *RepoManagement) DoJob(job *tlv.RepoCommand) {
	log.Info(m, "now doing job", job.Target)
	// FIXME: this should return an error if the node cannot do the command
	// this needs to be fixed before stress testing (filling up storage)
	m.storage.DoCommand(job)
	// TODO: embed this into the publish awareness update call
	tmp := tlv.AwarenessUpdate{
		Node:       job.Target,
		ActiveJobs: m.storage.GetJobs(),
	}
	m.awareness.PublishAwarenessUpdate(&tmp)
}
func (m *RepoManagement) AucDoJob(s string) {
	m.DoJob(m.DecodeCommand(s))
}

// TODO: eventually remove these helpers
func EncodeCommand(command *tlv.RepoCommand) string {
	return enc.Component{Typ: 8, Val: command.Target.Bytes()}.String()
}

func (m *RepoManagement) DecodeCommand(s string) *tlv.RepoCommand {
	n, err := enc.NameFromStr(s)
	if err != nil {
		n, _ = enc.NameFromStr("somethingwentwronginDecodeCommand")
	}
	return m.commands.Get(&n)
}
