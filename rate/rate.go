package rate

import (
	"context"
	"fmt"
	"log"
	"math"
	"os"
	"time"
)

type ctxKey byte

const DefaultTimeUnit = time.Minute

type PermitsLimit float64

const DefaultPermits PermitsLimit = 1000
const Unlimited PermitsLimit = math.MaxFloat64

type Limiter struct {
	limit                          float64
	limitDuration                  time.Duration
	timeToCreatePermit             time.Duration
	timeUnitFractionToCreatePermit float64
	permits                        float64
	lastAt                         time.Time
	lock                           chan struct{}
	log                            *log.Logger
}

// NewLimiter returns new limiter with a `limit` of permits in `timeUnit`;
// Default `timeUnit` is one minute;
func NewLimiter(limit PermitsLimit, timeUnit ...time.Duration) *Limiter {

	limiterTimeUnit := DefaultTimeUnit
	if len(timeUnit) > 0 {
		limiterTimeUnit = timeUnit[0]
	}

	lock := make(chan struct{}, 1)

	l := &Limiter{
		limit:                          float64(limit),
		limitDuration:                  limiterTimeUnit,
		timeToCreatePermit:             calcTimeForSinglePermit(float64(limit), limiterTimeUnit),
		timeUnitFractionToCreatePermit: calcTimeUnitFractionForSinglePermit(float64(limit), limiterTimeUnit),
		permits:                        float64(0),
		lastAt:                         time.Now(),
		lock:                           lock,
		log:                            log.New(os.Stdout, "L > ", log.Lmicroseconds),
	}

	l.log.Printf("time for one permit: %s | %.5f", l.timeToCreatePermit, l.timeUnitFractionToCreatePermit)

	return l
}

func (l *Limiter) Wait(ctx context.Context) error {
	return l.WaitMany(ctx, 1)
}

func getBIdxAndIdx(ctx context.Context) (int, int) {
	bIdx, ok := ctx.Value('b').(int)
	if !ok {
		bIdx = -1
	}

	i, ok := ctx.Value('i').(int)
	if !ok {
		i = -1
	}
	return bIdx, i
}

func (l *Limiter) WaitMany(ctx context.Context, n int64) error {

	bIdx, i := getBIdxAndIdx(ctx)

	if l.limit == float64(Unlimited) {
		return nil
	}

	if n > int64(l.limit) {
		return fmt.Errorf("[%2d][%2d] requested amount (%d) exceeds current limit (%d)", bIdx, i, n, int64(l.limit))
	}

	select {
	case l.lock <- struct{}{}:
		defer func() { <-l.lock }()
	case <-ctx.Done():
		return fmt.Errorf("[%2d][%2d] ctx Done while waiting for the lock, %w", bIdx, i, ctx.Err())
	}

	now := time.Now()
	availableNow := l.available(now)

	if int64(availableNow) >= n {
		l.log.Printf("[%2d][%2d] permits are available right away", bIdx, i)
		l.lastAt = now
		l.permits = availableNow - float64(n)
		return nil
	}

	missingPermits := float64(n) - availableNow

	timeToCatchUp := l.permitsToDuration(missingPermits)

	wait := time.After(timeToCatchUp)

	l.log.Printf("[%2d][%2d] not enough permits available (we are missing ~%.3f permits), need to wait %s", bIdx, i, missingPermits, timeToCatchUp)

	select {
	case <-ctx.Done():
		return fmt.Errorf("[%2d][%2d] ctx Done while waiting for the permit, %w", bIdx, i, ctx.Err())
	case <-wait:
		l.lastAt = time.Now()
		// in case on a single permit:
		// we waited a time that corresponds to the fraction of the permit that was missing,
		// e.g. we had 0.3 of te permit due to the different from `l.lastAt` to `now()`, so we need to wait
		// the time for the rest of the permit, i.e. `~(0.7*timeForSinglePermit)`
		// after that we have full permit, but we instantly use that so the available permits
		// is zero after this operation;
		// in case of multiple permits, the logic stays the same;
		l.permits = 0
		l.log.Printf("[%2d][%2d] %.3f permits awaited (%s)", bIdx, i, missingPermits, timeToCatchUp)
		return nil
	}
}

func (l *Limiter) permitsToDuration(needPermits float64) time.Duration {
	return time.Duration(needPermits * l.timeUnitFractionToCreatePermit)
}

func (l *Limiter) durationToPermits(d time.Duration) float64 {
	return float64(d) / l.timeUnitFractionToCreatePermit
}

func (l *Limiter) AllowNow(n int64) bool {
	if n > int64(l.limit) {
		return false
	}

	return l.AvailableNow() >= n
}

func (l *Limiter) Allow(n int64, at time.Time) bool {
	// if n > int64(l.permits) {
	// 	return false
	// }

	available := l.Available(at)

	return available >= n
}

func (l *Limiter) AvailableNow() int64 {
	return l.Available(time.Now())
}

// Available returns number of available permits at given time
func (l *Limiter) Available(at time.Time) int64 {

	return int64(l.available(at))
}

func (l *Limiter) available(at time.Time) float64 {

	// TODO: do we correctly handle case when `at - now > timeUnit`

	possiblePermits := l.catchUp(at)
	return possiblePermits + l.permits
}

// catchUp calculates number of permits that would be produced since the last time the permit was acquired until now;
//
// catchUp does not update the Limiter;
func (l *Limiter) catchUp(now time.Time) float64 {

	lastAcquiredAt := l.lastAt
	d := now.Sub(lastAcquiredAt)

	if d < 0 {
		d = -d
	}

	wouldProducePermits := l.durationToPermits(d)

	return wouldProducePermits
}

func (l *Limiter) Reserve(ctx context.Context, n int64) Reservation {

	now := time.Now()

	if l.limit == float64(Unlimited) {
		return Reservation{
			Ok:          true,
			lim:         l,
			Permits:     n,
			Due:         now,
			requestedAt: now,
			canceled:    new(bool),
		}
	}

	if n > int64(l.limit) {
		return Reservation{
			Ok:          false,
			lim:         l,
			Permits:     n,
			Due:         now,
			requestedAt: now,
			canceled:    new(bool),
		}
	}

	l.lock <- struct{}{}
	defer func() { <-l.lock }()

	bIdx, i := getBIdxAndIdx(ctx)

	now = time.Now()

	permits := l.catchUp(now)
	l.permits = min(l.permits+permits, l.limit)
	l.lastAt = now

	missingPermits := float64(n) - l.permits

	var d time.Duration
	var due = now
	if missingPermits > 0 {
		d = l.permitsToDuration(missingPermits)
		due = l.lastAt.Add(d)
	}

	l.permits -= float64(n)

	if deadline, ok := ctx.Deadline(); ok && due.After(deadline) {
		r := Reservation{
			lim:         l,
			Ok:          false,
			Due:         due,
			Permits:     n,
			requestedAt: now,
			canceled:    new(bool),
		}

		l.log.Printf("[%2d][%2d] %s", bIdx, i, r)
		return r
	}

	r := Reservation{
		lim:         l,
		Ok:          true,
		Due:         due,
		Permits:     n,
		requestedAt: now,
		canceled:    new(bool),
	}

	l.log.Printf("[%2d][%2d] %s", bIdx, i, r)

	return r
}

type Reservation struct {
	lim         *Limiter
	Ok          bool
	Permits     int64
	Due         time.Time
	requestedAt time.Time
	canceled    *bool // instead of canceled set Ok to false
}

func (r Reservation) String() string {
	if r.Ok {
		return fmt.Sprintf("Successful Reservation of %d permits, Due: %s, Requested: %s", r.Permits, r.Due, r.requestedAt)
	}

	return fmt.Sprintf("Failed Reservation of %d permits, Requested: %s", r.Permits, r.requestedAt)
}

func (r Reservation) Restore() {

	if !r.Ok || *r.canceled {
		return
	}

	*r.canceled = true

	r.lim.lock <- struct{}{}
	defer func() { <-r.lim.lock }()

	now := time.Now()

	if now.After(r.lim.lastAt) {
		catchUpPermits := r.lim.catchUp(now)
		r.lim.permits += catchUpPermits
		r.lim.lastAt = now
	}

	minTime := now.Add(-r.lim.limitDuration)

	if r.Due.Before(minTime) {
		r.lim.log.Printf("R Expired: due=%s, minTime=%s", r.Due, minTime)
		// already expired
		return
	}

	startTime := maxT(minTime, r.requestedAt)
	// endTime := minT(r.Due, now) // restore only until now
	endTime := r.Due

	restorableDuration := min(endTime.Sub(startTime), r.lim.limitDuration)

	restorablePermits := r.lim.durationToPermits(restorableDuration)

	r.lim.log.Printf("R restorable duration=%s => %.4f of permits", restorableDuration, restorablePermits)

	r.lim.permits += restorablePermits

	if r.lim.permits > r.lim.limit {
		r.lim.permits = r.lim.limit
		if now.After(r.lim.lastAt) {
			r.lim.lastAt = now
		}

		return
	}

	if now.After(r.lim.lastAt) {
		r.lim.lastAt = now
	}
}

func calcTimeForSinglePermit(permitsInTimeUnit float64, timeUnit time.Duration) time.Duration {
	return time.Duration(calcTimeUnitFractionForSinglePermit(permitsInTimeUnit, timeUnit))
}

func calcTimeUnitFractionForSinglePermit(permitsInTimeUnit float64, timeUnit time.Duration) float64 {
	return float64(timeUnit) / permitsInTimeUnit
}

func minT(t1, t2 time.Time, tx ...time.Time) time.Time {
	min := t1
	if t2.Before(min) {
		min = t2
	}

	for _, t := range tx {
		if t.Before(min) {
			min = t
		}
	}

	return min
}

func maxT(t1, t2 time.Time, tx ...time.Time) time.Time {
	max := t1
	if t2.After(max) {
		max = t2
	}

	for _, t := range tx {
		if t.After(max) {
			max = t
		}
	}

	return max
}
