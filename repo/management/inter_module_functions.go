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

func (m *RepoManagement) CheckJob(job *tlv.RepoCommand) {
	status := m.getJobStatus(job)
	// TODO: make it a switch statement
	if status == "under" {
		m.underReplication(job)
	} else if status == "over" {
		m.overReplication(job)
	} else if status == "good" {
		m.goodReplication(job)
	} else {
		log.Warn(m, "got bad status", status)
	}
}

// calculate number of times a job is done
func (m *RepoManagement) getJobStatus(job *tlv.RepoCommand) string {
	// how many times the job should be done
	r := 0
	if m.commands.ShouldBeActive(job) {
		r = m.repo.NumReplicas
	}

	// how many times the job is done (local understanding)
	num := m.awareness.Storage.GetReplications(job)
	if m.storage.DoingJob(job) {
		num++
	}

	// status return
	// TODO: maybe standardize this
	if num < r {
		return "under"
	} else if num == r {
		return "over"
	}
	return "good"
}

// Launches a coordination job to check the status from responsible node
func (m *RepoManagement) StatusRequestHandler(interestHandler *ndn.InterestHandlerArgs, target *enc.Name) {
	log.Info(m, "Got status request for", target)
	// Prepare reply
	reply := tlv.RepoStatusResponse{
		Target: target.Clone(),
		Status: m.getJobStatus(m.commands.Get(target)),
	}

	data, _ := m.repo.Engine.Spec().MakeData(
		interestHandler.Interest.Name(),
		&ndn.DataConfig{},
		reply.Encode(),
		nil, // TODO: security
	)

	interestHandler.Reply(data.Wire)
}

func (m *RepoManagement) GetAvailability(job *tlv.RepoCommand) int {
	// Get storage state
	var stat unix.Statfs_t
	wd, _ := os.Getwd()
	unix.Statfs(wd, &stat)

	// Get free spaces
	// FIXME: if you are already doing the job, add more to the availability
	freeSpace := stat.Bavail * uint64(stat.Bsize)
	log.Info(m, "Availability: free space", freeSpace, "for job", job)

	// Calculate bid
	bid := int(freeSpace)

	return bid
}

// got command from producer
func (m *RepoManagement) OnNewCommand(command *tlv.RepoCommand) {
	// NOTE: ideally, this would publish the command to the group, do the job, then check for replication
	// however, PublishCommand does the first and last.
	// Doing the job after publishing guarantees an incorrect replication count (off by 1)
	// the underlying handlers for replication do not check to see if they are still needed; it is assumed that they are needed unless otherwise noted,
	// The cost of avoiding this issue is losing a job if m.DoJob() causes the repo to crash, since it is not published to the group.
	// for now, we're going to assume that the producer will do a status check on the repo to see if it is doing the command.
	// if it isn't (m.DoJob crashed the node), then it should try again
	//
	// TODO: the way to fix this is by separating the check for replication out of the PublishCommand call.

	// do job if you have the resources
	m.DoJob(command)
	// publish command to the commands SVS group, since it's new
	m.commands.PublishCommand(command)
}

// storage will call awareness if an update happens
// doing this in storage might be easier for coding+debugging since the checks to see if state changed need to happen regardless
func (m *RepoManagement) DoJob(job *tlv.RepoCommand) {
	log.Info(m, "now doing job", job.Target)
	if m.storage.DoingJob(job) {
		log.Warn(m, "already doing job", job)
		return
	}
	m.handleFromStorage(m.storage.AddJob(job))
}

func (m *RepoManagement) ReleaseJob(job *tlv.RepoCommand) {
	log.Info(m, "releasing job", job.Target)
	m.handleFromStorage(m.storage.ReleaseJob(job))
}

func (m *RepoManagement) handleFromStorage(err error) {
	if err != nil {
		log.Warn(m, err.Error())
		// NOTE: if using auction, maybe run an auction here (this requires a refactor)
		// this would return an error if type is invalid, state didn't change, or doesn't have enough storage
	} else {
		tmp := tlv.AwarenessUpdate{
			Node:       m.repo.NodeNameN,
			ActiveJobs: m.storage.GetJobs(),
		}
		m.awareness.PublishAwarenessUpdate(&tmp)
	}

}

// TODO: eventually remove these helpers
func (m *RepoManagement) AucDoJob(s string) {
	m.DoJob(m.DecodeCommand(s))
}

func (m *RepoManagement) AucAucJob(job *tlv.RepoCommand) {
	m.auction.AuctionItem(EncodeCommand(job))
}

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

// NOTE: these next two can be more or less sophisticated about handling edge cases and timeouts (see comments)
// at the very least (and for now), they simply join a sync group or fetch data,
// they are also (so far) only called by storage, since storage knows whether it has joined a sync group, fetched data, or not

// fetch application data from the network
func (m *RepoManagement) fetchData(name *enc.Name) {
	log.Info(m, "Fetching data", "name", name)
	// haotian: the assumption here is that command can be committed to the group state before the data is available, hence we keep retrying to fetch the relevant data. This, however, cause unnecessary traffics. Also, if the producer, for any reason, send the command again, we need to fetch the data again immediately, so the handler's id should not be tied to the data name.
	// at: tie it to the data name, reset the wait period if you need to fetch the data immediately

	// use m.repo.Client
}

// join sync group
func (m *RepoManagement) joinSync(syncGroupName *enc.Name, threshold *uint64) {
	log.Info(m, "joining sync group", syncGroupName, "with threshold", threshold)
}
