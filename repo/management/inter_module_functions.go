package management

import (
	"golang.org/x/sys/unix" // POSIX system
	"os"

	"github.com/named-data/ndnd/repo/tlv"
	enc "github.com/named-data/ndnd/std/encoding"
	"github.com/named-data/ndnd/std/log"
)

// TODO: need to figure out whether any of these need goroutines
func (m *RepoManagement) CheckJob(job *tlv.RepoCommand) {
	log.Info(m, "checking job", job.Target)
	status := m.getJobStatus(job)
	// TODO: make it a switch statement
	if status == "under" {
		log.Info(m, job.Target.String(), "is under replicated")
		if !m.storage.DoingJob(job) {
			log.Debug(m, job.Target.String(), "under replication handler")
			m.underReplication(job)
		} else {
			log.Debug(m, job.Target.String(), "under but already doing job")
		}
	} else if status == "over" {
		log.Info(m, job.Target.String(), "is over replicated")
		m.overReplication(job)
	} else if status == "good" {
		log.Info(m, job.Target.String(), "is replicated")
		m.goodReplication(job)
	} else {
		log.Warn(m, "got bad status", status)
	}
	log.Trace(m, "end of CheckJob")
}

// calculate number of times a job is done
func (m *RepoManagement) OnStatus(name enc.Name, content enc.Wire, reply func(wire enc.Wire) error) {
	log.Info(m, "Getting status for name", name)
	job, exists := m.commands.Get(&name)
	sr := tlv.RepoStatusResponse{
		Target: name,
	}
	if !exists {
		sr.Status = "unknown"
	} else {
		sr.Status = m.getJobStatus(job)
	}

	err := reply(sr.Encode())
	if err != nil {
		log.Warn(m, "error replying to status request", name)
	}
}
func (m *RepoManagement) getJobStatus(job *tlv.RepoCommand) string {
	log.Info(m, "getJobStatus")
	// how many times the job should be done
	r := 0
	log.Debug(m, "get whether it should be active")
	if m.commands.ShouldBeActive(job) {
		r = m.repo.NumReplicas
	}

	log.Debug(m, "from awareness, get whether it is done by others")
	// how many times the job is done (local understanding)
	num := m.awareness.Storage.GetReplications(job)
	log.Debug(m, "get whether i'm doing it")
	if m.storage.DoingJob(job) {
		num++
	}
	log.Trace(m, "job is done", num, "times and should be", r)

	// status return
	// TODO: maybe standardize this
	if num < r {
		return "under"
	} else if num == r {
		return "over"
	}
	return "good"
}

func (m *RepoManagement) GetAvailability(job *tlv.RepoCommand) int {
	// Get storage state
	var stat unix.Statfs_t
	wd, _ := os.Getwd()
	unix.Statfs(wd, &stat)

	// Get free spaces
	freeSpace := stat.Bavail * uint64(stat.Bsize)
	log.Debug(m, "Availability: free space", freeSpace, "for job", job)

	// Calculate bid
	// NOTE: if you are already doing the job, add more to the availability
	if m.storage.DoingJob(job) {
		return int(freeSpace * 3 / 2)
	}

	return int(freeSpace)
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

	log.Info(m, "new command from producer")
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
	log.Trace(m, "after storage.DoingJob() check for job", job.Target)
	m.handleFromStorage(m.storage.AddJob(job))
	log.Trace(m, "end of DoJob() for", job.Target)
}

func (m *RepoManagement) ReleaseJob(job *tlv.RepoCommand) {
	log.Info(m, "releasing job", job.Target)
	log.Debug(m, "not really releasing job", job.Target)

	m.handleFromStorage(m.storage.ReleaseJob(job))
}

func (m *RepoManagement) handleFromStorage(err error) {
	log.Debug(m, "handling callback from storage")
	if err != nil {
		log.Debug(m, "got an error:", err.Error())
		// NOTE: if using auction, maybe run an auction here (this requires a refactor)
		// this would return an error if type is invalid, state didn't change, or doesn't have enough storage
	} else {
		log.Debug(m, "making awareness update")
		tmp := tlv.AwarenessUpdate{
			Node:       m.repo.NodeNameN,
			ActiveJobs: m.storage.GetJobs(),
		}
		log.Debug(m, "publishing awareness update")
		m.awareness.PublishAwarenessUpdate(&tmp)
	}

}

// TODO: these are helpers to interface with auction
func (m *RepoManagement) AucDoJob(s string) {
	job := m.DecodeCommand(s)
	if job != nil {
		m.DoJob(job)
	}
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
	rc, exists := m.commands.Get(&n)
	if exists {
		return rc
	}
	return nil
}
