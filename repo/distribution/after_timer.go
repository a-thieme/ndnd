package distribution

import (
	"github.com/named-data/ndnd/repo/tlv"
	"github.com/named-data/ndnd/std/log"
	"math/rand/v2"
	"sync"
	"time"
)

type TimeBased struct {
	mutex      sync.RWMutex
	timers     map[string]*time.Timer
	types      map[string]string
	getAbility func(*tlv.RepoCommand) int
	getUsage   func(*tlv.RepoCommand) int

	doJob      func(*tlv.RepoCommand)
	releaseJob func(*tlv.RepoCommand)
}

func (t *TimeBased) SetAbility(f func(*tlv.RepoCommand) int) {
	t.getAbility = f
}

func (t *TimeBased) SetUsage(f func(*tlv.RepoCommand) int) {
	t.getUsage = f
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
		types:  make(map[string]string),
	}
}

func (t *TimeBased) String() string {
	return "time-based"
}

func (t *TimeBased) Over(job *tlv.RepoCommand) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	target := job.Target.String()
	log.Debug(t, "called over replication for", "target", target)
	if t.types[target] == "over" {
		log.Debug(t, "already over for", "target", target)
		return
	}
	t.types[target] = "over"

	r := t.getAbility(job)
	wait := calculateOver(r)
	log.Debug(t, "waiting", "ability", r, "time", wait, "target", target)

	timer := t.timers[target]
	if timer != nil {
		timer.Stop()
	}
	t.timers[target] = time.AfterFunc(wait, func() {
		t.releaseJob(job)
		t.reset(target)
	})
}

func (t *TimeBased) reset(target string) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	delete(t.timers, target)
	delete(t.types, target)
}

func (t *TimeBased) Under(job *tlv.RepoCommand) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	target := job.Target.String()
	log.Debug(t, "called under replication for", "target", target)
	if t.types[target] == "under" {
		log.Debug(t, "already under for", "target", target)
		return
	}
	a := t.getUsage(job)
	wait := calculateUnder(a)
	log.Debug(t, "waiting", a, wait, "target", target)

	timer := t.timers[target]
	if timer != nil {
		timer.Stop()
	}
	t.timers[target] = time.AfterFunc(wait, func() {
		t.doJob(job)
		t.reset(target)
	})
}

func (t *TimeBased) Good(job *tlv.RepoCommand) {
	t.mutex.Lock()
	target := job.Target.String()
	log.Debug(t, "called good replication for", target)
	t.types[target] = "good"
	timer := t.timers[target]
	if timer != nil {
		timer.Stop()
	}
	t.mutex.Unlock()
	t.reset(target)
}

func calculateOver(a int) time.Duration {
	// a
	// return time.Duration(float32(a)*(0.5+rand.Float32()*0.5)) * time.Second
	return time.Duration(a) * time.Second
	// return time.Duration(a) * time.Second
}

func calculateUnder(a int) time.Duration {
	// max a is 20, so shooting for max 400ms wait
	return time.Duration(a) * time.Millisecond * 20
}
