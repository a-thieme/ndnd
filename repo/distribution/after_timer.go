package distribution

import (
	"github.com/named-data/ndnd/repo/tlv"
	"github.com/named-data/ndnd/std/log"
	"math/rand/v2"
	// "strconv"
	// "math"
	"sync"
	"time"
)

type TimeBased struct {
	mutex      sync.RWMutex
	timers     map[string]*time.Timer
	types      map[string]string
	getAbility func(*tlv.RepoCommand) float64
	getUsage   func(*tlv.RepoCommand) float64

	doJob      func(*tlv.RepoCommand)
	releaseJob func(*tlv.RepoCommand)
}

func (t *TimeBased) SetAbility(f func(*tlv.RepoCommand) float64) {
	t.getAbility = f
}

func (t *TimeBased) SetUsage(f func(*tlv.RepoCommand) float64) {
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
	timer := t.timers[target]
	if timer != nil {
		timer.Stop()
	}

	t.types[target] = "over"

	r := t.getAbility(job)
	wait := t.calculateOver(r)
	log.Info(t, "waiting", "ability", r, "time", wait, "target", target)

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
	timer := t.timers[target]
	if timer != nil {
		timer.Stop()
	}

	t.types[target] = "under"
	u := t.getUsage(job)
	wait := calculateUnder(u)
	log.Info(t, "waiting", "usage", u, "time", wait, "target", target)

	t.timers[target] = time.AfterFunc(wait, func() {
		if t.getAbility(job) > 0 {
			t.doJob(job)
		}
		t.reset(target)
	})
}

func (t *TimeBased) Good(job *tlv.RepoCommand) {
	t.mutex.Lock()
	target := job.Target.String()
	log.Debug(t, "called good replication for", target)
	timer := t.timers[target]
	if timer != nil {
		timer.Stop()
	}
	t.mutex.Unlock()
	t.reset(target)
}

func (t *TimeBased) calculateOver(a float64) time.Duration {
	// a
	// log.Info(t, "check", "over", strconv.FormatFloat(a, 'f', 3, 64))
	// return time.Duration(float64(a)*(0.5+rand.Float64())) * time.Second
	// 1000 * 1000 -> ms
	// 1000 * 1000 * 1000 -> s
	// this works ~ 15s until == 3, little bounce, tail to ~25s, sequences max 50
	return time.Duration(a * 1000 * 1000 * 1000 * rand.Float64())

	// this one bounced at ~1.5s, converged at 10
	// return time.Duration(a * 1000 * 1000 * 100 * rand.Float64())

	// this one bounced a lot, converged at 20-30
	// return time.Duration(a * 1000 * 1000 * 10 * rand.Float64())
}

func calculateUnder(a float64) time.Duration {
	// max a is 50, so shooting for max 400ms wait
	// this works ~ 800ms until >= 3
	return time.Duration(a * 1000 * 1000 * 1000 * rand.Float64())
}
