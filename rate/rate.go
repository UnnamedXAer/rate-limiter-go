package rate

import (
	"context"
	"fmt"
	"log"
	"math"
	"os"
	"time"
)

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

	l.lock <- struct{}{}
	defer func() { <-l.lock }()

	availableNow := l.available(time.Now())

	if int64(availableNow) >= n {
		l.log.Printf("[%2d][%2d] permits are available right away", bIdx, i)
		l.lastAt = time.Now()
		l.permits = availableNow - float64(n)
		return nil
	}

	missingPermits := float64(n) - availableNow

	timeToCatchUp := l.calcTimeToCatchUp(missingPermits)

	wait := time.After(timeToCatchUp)

	l.log.Printf("[%2d][%2d] not enough permits available (we are missing ~%.3f permits), need to wait %s", bIdx, i, missingPermits, timeToCatchUp)

	select {
	case <-ctx.Done():
		return fmt.Errorf("[%2d][%2d] ctx Done while waiting for the permit, %w", bIdx, i, ctx.Err())
	case <-wait:
		l.lastAt = time.Now()
		// in case on a signle permit:
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

func (l *Limiter) calcTimeToCatchUp(needPermits float64) time.Duration {
	return time.Duration(l.timeUnitFractionToCreatePermit * needPermits)
}

func (l *Limiter) AllowNow(n int64) bool {
	if n > int64(l.limit) {
		return false
	}

	return l.AvailableNow() >= n
}

func (l *Limiter) correct() {
	if l.permits > l.limit {
		ago := time.Since(l.lastAt)
		l.log.Printf("wrong state: permits=%.4f, lastAt=%s, %s ago", l.permits, l.lastAt, ago)
		l.permits = l.limit
		l.lastAt = time.Now().Add(-l.limitDuration)
	}
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
func (l *Limiter) catchUp(due time.Time) float64 {

	lastAcquiredAt := l.lastAt
	d := due.Sub(lastAcquiredAt)

	if d < 0 {
		d = -d
	}

	// if passedTime >= l.timeUnit {
	// 	return l.limit
	// }

	wouldProducePermits := float64(d) / l.timeUnitFractionToCreatePermit

	return wouldProducePermits
}

func (l *Limiter) Reserve(ctx context.Context, n int64) Reservation {

	now := time.Now()

	bIdx, i := getBIdxAndIdx(ctx)

	d := l.calcTimeToCatchUp(float64(n))
	due := now.Add(d)
	if deadline, ok := ctx.Deadline(); ok {
		if due.After(deadline) {
			r := Reservation{
				lim:         l,
				Ok:          false,
				Due:         due,
				Permits:     n,
				requestedAt: now,
			}

			l.log.Printf("[%2d][%2d] %s", bIdx, i, r)
			return r
		}
	}

	l.lastAt = due
	l.permits = 0

	r := Reservation{
		lim:         l,
		Ok:          true,
		Due:         due,
		Permits:     n,
		requestedAt: now,
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
}

func (r Reservation) String() string {
	if r.Ok {
		return fmt.Sprintf("Successful Reservation of %d permits, Due: %s, Requested: %s", r.Permits, r.Due, r.requestedAt)
	}

	return fmt.Sprintf("Failed Reservation of %d permits, Requested: %s", r.Permits, r.requestedAt)
}

func (r Reservation) Restore() {

	if !r.Ok {
		return
	}

	now := time.Now()

	minTime := now.Add(-r.lim.limitDuration)
	if r.Due.Before(minTime) {
		r.lim.log.Printf("R Expired: due=%s, minTime=%s", r.Due, minTime)
		// already expired
		return
	}

	startTime := maxT(minTime, r.requestedAt)
	endTime := r.Due

	restorableDuration := endTime.Sub(startTime)

	if restorableDuration > r.lim.limitDuration {
		restorableDuration = r.lim.limitDuration
	}

	restorablePermits := float64(restorableDuration) / (r.lim.timeUnitFractionToCreatePermit)

	// if restorablePermits > r.lim.limit {
	// 	restorablePermits = r.lim.limit
	// }

	r.lim.log.Printf("R restorable duration=%s => %.4f of permits", restorableDuration, restorablePermits)
	fmt.Print()

	r.lim.permits = min(r.lim.permits+restorablePermits, r.lim.limit)

	last := endTime
	if last.After(r.lim.lastAt) {
		r.lim.lastAt = last
	}

	r.lim.correct()

	// TODO: remove assert
	if r.lim.permits > r.lim.limit {
		r.lim.log.Fatalf("permits exceeds limit. limit=%.0f, permits=%.4f", r.lim.limit, r.lim.permits)
	}
	// assert:end
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
