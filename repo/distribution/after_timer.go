package distribution

import (
	"github.com/named-data/ndnd/repo/tlv"
	enc "github.com/named-data/ndnd/std/encoding"
	"time"
)

type TimeBased struct {
	timers     map[*enc.Name]*time.Timer
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
		timers: make(map[*enc.Name]*time.Timer),
	}
}

func (t *TimeBased) Over(job *tlv.RepoCommand) {
	// default
	r := t.getAbility(job)
	wait := calculateOver(r)
	t.timers[&job.Target] = time.AfterFunc(wait, func() {
		t.releaseJob(job)
	})
}

func (t *TimeBased) Under(job *tlv.RepoCommand) {
	a := t.getAbility(job)
	wait := calculateUnder(a)
	t.timers[&job.Target] = time.AfterFunc(wait, func() {
		t.doJob(job)
	})
}

func (t *TimeBased) Good(job *tlv.RepoCommand) {
	timer := t.timers[&job.Target]
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
