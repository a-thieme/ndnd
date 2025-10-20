package management

import (
	"fmt"
	"golang.org/x/sys/unix" // POSIX system
	"os"
	"sort"

	"github.com/named-data/ndnd/repo/tlv"
	enc "github.com/named-data/ndnd/std/encoding"
	"github.com/named-data/ndnd/std/log"
)

func (m *RepoManagement) CheckJob(job *tlv.RepoCommand) {
	log.Debug(m, "checking job", job.Target)
	status := m.getJobStatus(job)
	// TODO: make it a switch statement
	if status > 0 {
		log.Debug(m, job.Target.String(), "is under replicated")
		// auction should be able to figure out if it is the auctioneer
		// m.underReplication(job.Target.String())
		m.AucAucJob(job)
		// if !m.storage.DoingJob(job) {
		// 	log.Debug(m, job.Target.String(), "under replication handler")
		// 	m.underReplicationCont(job, status)
		// } else {
		// 	log.Debug(m, job.Target.String(), "under but already doing job")
		// }
	} else if status < 0 {
		log.Info(m, job.Target.String(), "job", "is over replicated")
		if m.storage.DoingJob(job) {
			log.Debug(m, job.Target.String(), "calling", "over handler")
			m.overReplication(job)
		}
	} else if status == 0 {
		log.Debug(m, job.Target.String(), "is replicated")
		m.goodReplication(job)
	} else {
		log.Warn(m, "got bad status", status)
	}
	log.Trace(m, "end of CheckJob")
}

// figure out if you need to drop job
// this should only be called if you're doing the job
func (m *RepoManagement) overReplicationCont(job *tlv.RepoCommand, num int) {
	// starts the negative value
	num = num * -1
	// positive value for how many times it still needs to be done
	// get nodes doing the job

	// all nodes
	a := m.awareness.GetNodes()
	for node := range a {
		aware := m.awareness.Storage.GAwareness(node)
		jbs := aware.GetJobs()
		found := false
		for _, j := range jbs {
			if found {
				continue
			}
			if j.Target.String() == job.Target.String() {
				found = true
			}
		}
		if found == false {
			delete(a, node)
		}
	}
	// add myself to top r nodes
	a[m.repo.NodeNameN.String()] = len(m.storage.GetJobs())

	// sort nodes by availability
	sorted := smbv(a)
	myVal := -1
	for index, name := range sorted {
		log.Info(m, name, "val", a[name])
		if name == m.repo.NodeNameN.String() {
			myVal = index
		}
	}
	if myVal == -1 {
		panic("tried to match my name in the array but it didn't find it")
	}

	// if i am not in top num nodes, drop replication
	if myVal > m.repo.NumReplicas-1 {
		m.ReleaseJob(job)
	}

}

// figure out if you need to do the job
// this should only be called if you're not doing the job
func (m *RepoManagement) underReplicationCont(job *tlv.RepoCommand, num int) {
	// positive value for how many times it still needs to be done
	// get nodes doing the job

	// all nodes
	a := m.awareness.GetNodes()
	jTarget := job.Target.String()
	for node := range a {
		aware := m.awareness.Storage.GAwareness(node)
		jbs := aware.GetJobs()
		found := false
		// see if the node is doing the job
		for _, j := range jbs {
			if found {
				continue
			}
			if j.Target.String() == jTarget {
				found = true
			}
		}
		if found == true {
			delete(a, node)
		}
	}

	// add myself to top r nodes
	a[m.repo.NodeNameN.String()] = len(m.storage.GetJobs())

	// sort nodes by availability
	sorted := smbv(a)
	log.Debug(m, fmt.Sprintf("%v", sorted), "needs", num, "target", jTarget)
	myVal := -1
	for index, name := range sorted {
		if name == m.repo.NodeNameN.String() {
			myVal = index
		}
	}
	if myVal == -1 {
		panic("tried to match my name in the array but it didn't find it")
	}

	// if i am in top num nodes, do job
	if myVal < num {
		m.DoJob(job)
	}
}

// A helper struct to hold a key-value pair.
type kv struct {
	Key   string
	Value int
}

// SortMapByValue takes a map and returns the keys sorted by value in ascending order.
func smbv(m map[string]int) []string {
	// 1. Create a slice of key-value structs from the map.
	var ss []kv
	for k, v := range m {
		ss = append(ss, kv{k, v})
	}

	// 2. Sort the slice of structs by the Value field.
	// The anonymous function tells sort.Slice how to compare two elements.
	sort.Slice(ss, func(i, j int) bool {
		// First, check if the values are different.
		if ss[i].Value != ss[j].Value {
			// If values are different, sort by value (ascending).
			return ss[i].Value < ss[j].Value
		}
		// --- TIE-BREAKER ---
		// If values are equal, sort by key (alphabetical).
		return ss[i].Key < ss[j].Key
	})

	// 3. Extract the keys from the now-sorted slice.
	// We pre-allocate the slice for better performance.
	sortedKeys := make([]string, len(m))
	for i, pair := range ss {
		sortedKeys[i] = pair.Key
	}

	return sortedKeys
}

// calculate number of times a job is done
func (m *RepoManagement) OnStatus(name enc.Name, content enc.Wire, reply func(wire enc.Wire) error) {
	log.Debug(m, "Getting status for name", name)
	request, err := tlv.ParseRepoStatusRequest(enc.NewWireView(content), false)
	if err != nil {
		log.Warn(m, "got error when trying to parse status from producer", name)
		sr := tlv.RepoStatusResponse{Target: name, Status: "parsing error"}
		reply(sr.Encode())
		return
	}
	log.Debug(m, "request is parsed, now getting the target")
	target := request.Target
	log.Debug(m, "getting command")
	job, exists := m.commands.Get(&target)
	log.Debug(m, "generating response")
	sr := tlv.RepoStatusResponse{
		Target: target,
	}
	log.Debug(m, "check exists")
	if !exists {
		log.Warn(m, "could not find job")
		sr.Status = "unknown"
	} else {
		log.Debug(m, "job exists, getting status", job.Target)
		sr.Status = string(m.getJobStatus(job))
	}

	log.Debug(m, "Reply with status response")
	err = reply(sr.Encode())
	if err != nil {
		log.Warn(m, "error replying to status request", name)
	}
	log.Debug(m, "end of OnStatus()")
}
func (m *RepoManagement) getJobStatus(job *tlv.RepoCommand) int {
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
	// FIXME: remove this, just doing debug since i'm not at trace level
	log.Info(m, job.Target.String(), "done", num, "target", r)

	// status return
	// TODO: maybe standardize this
	return r - num
}

// FIXME: get this to actuall do storage analysis. for now, just number of commands, and total is 50
func (m *RepoManagement) GetAvailability(job *tlv.RepoCommand) float64 {
	// Get storage state
	var stat unix.Statfs_t
	wd, _ := os.Getwd()
	unix.Statfs(wd, &stat)

	// Get free spaces
	// freeSpace := stat.Bavail * uint64(stat.Bsize)
	total := 50.
	numJobs := len(m.storage.GetJobs())
	free := total - float64(numJobs)
	// log.Debug(m, "Availability: free space", freeSpace, "for job", job)

	// add checking for nil for continuous publishing, since it doesn't calculate on each command
	// Calculate bid
	setting := free * free / total
	// NOTE: if you are already doing the job, add more to the availability
	if job != nil && m.storage.DoingJob(job) {
		// return int(freeSpace * 3 / 2)
		return (setting) * 3 / 2
	}
	// return int(freeSpace)
	log.Debug(m, "Availability", setting)
	return setting
}

func (m *RepoManagement) GetUsage(job *tlv.RepoCommand) float64 {
	usage := float64(len(m.storage.GetJobs()))
	log.Info(m, "usage", "lengetjobs", usage)
	total := 50.
	setting := usage * usage / total
	if job != nil && m.storage.DoingJob(job) {
		// return int(freeSpace * 3 / 2)
		return (setting) / 2
	}
	return setting

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
	// publish command to the commands SVS group, since it's new
	m.commands.PublishCommand(command)
	// do job if you have the resources
	m.DoJob(command)
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
	} else {
		panic("decoding command didn't work")
	}
}

func (m *RepoManagement) AucAucJob(job *tlv.RepoCommand) {
	m.auction.AuctionItem(job.Target.String())
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

func (m *RepoManagement) AucCalcBid(s string) int {
	cmd := m.DecodeCommand(s)
	if cmd == nil {
		return 0
	}
	working := int(m.GetAvailability(cmd)) + 1
	if m.storage.DoingJob(cmd) {
		return 3 * working / 2
	}
	return working
}
