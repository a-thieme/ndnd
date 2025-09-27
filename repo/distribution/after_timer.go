package distribution

import (
	"github.com/named-data/ndnd/repo/tlv"
	"github.com/named-data/ndnd/std/log"
	"time"
)

type TimeBased struct {
	timers     map[string]*time.Timer
	getAbility func(*tlv.RepoCommand) int
	doJob      func(*tlv.RepoCommand)
	releaseJob func(*tlv.RepoCommand)
}

func (t *TimeBased) SetAbility(f func(*tlv.RepoCommand) int) {
	t.getAbility = f
}

func (t *TimeBased) SetDoJob(f func(*tlv.RepoCommand)) {
	t.doJob = f
}

func (t *TimeBased) SetRelease(f func(*tlv.RepoCommand)) {
	t.releaseJob = f
}

func NewTimeBased() *TimeBased {
	return &TimeBased{
		timers: make(map[string]*time.Timer),
	}
}

func (t *TimeBased) String() string {
	return "time-based"
}

func (t *TimeBased) Over(job *tlv.RepoCommand) {
	log.Debug(t, "called over replication")
	r := t.getAbility(job)
	wait := calculateOver(r)
	t.timers[job.Target.String()] = time.AfterFunc(wait, func() {
		t.releaseJob(job)
	})
}

func (t *TimeBased) Under(job *tlv.RepoCommand) {
	log.Debug(t, "called under replication")
	a := t.getAbility(job)
	wait := calculateUnder(a)
	t.timers[job.Target.String()] = time.AfterFunc(wait, func() {
		t.doJob(job)
	})
}

func (t *TimeBased) Good(job *tlv.RepoCommand) {
	log.Debug(t, "called good replication")
	timer := t.timers[job.Target.String()]
	if timer != nil {
		timer.Stop()
	}
}
func calculateOver(a int) time.Duration {
	// a
	return time.Duration(a) * time.Second
}

func calculateUnder(a int) time.Duration {
	// 1/a
	return time.Duration(int(time.Second) / a)
}
