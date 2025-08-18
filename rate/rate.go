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
	permits                        float64
	timeUnit                       time.Duration
	timeToCreatePermit             time.Duration
	timeUnitFractionToCreatePermit float64
	oldPermits                     float64
	lastAt                         time.Time
	lock                           chan struct{}
	log                            *log.Logger
}

func NewLimiter(permitsPerMinute PermitsLimit, timeUnit ...time.Duration) *Limiter {

	limiterTimeUnit := DefaultTimeUnit
	if len(timeUnit) > 0 {
		limiterTimeUnit = timeUnit[0]
	}

	lock := make(chan struct{}, 1)

	l := &Limiter{
		permits:                        float64(permitsPerMinute),
		timeUnit:                       limiterTimeUnit,
		timeToCreatePermit:             calcTimeForSinglePermit(float64(permitsPerMinute), limiterTimeUnit),
		timeUnitFractionToCreatePermit: calcTimeUnitFractionForSinglePermit(float64(permitsPerMinute), limiterTimeUnit),
		oldPermits:                     float64(0),
		lastAt:                         time.Now(),
		lock:                           lock,
		log:                            log.New(os.Stdout, "L > ", log.Lmicroseconds),
	}

	l.log.Printf("time for one permit: %s | %.5f", l.timeToCreatePermit, l.timeUnitFractionToCreatePermit)

	return l
}

func (l *Limiter) Wait(ctx context.Context) error {

	if time.Now().Nanosecond() > 0 {
		return l.WaitMany(ctx, 1)
	}

	bIdx, i := getBIdxAndIdx(ctx)

	if l.permits == float64(Unlimited) {
		l.log.Printf("[%2d][%2d] permit granted - unlimited permits", bIdx, i)
		return nil
	}

	select {
	case <-ctx.Done():
		return fmt.Errorf("[%2d][%2d] ctx Done while waiting for permits' lock, %w", bIdx, i, ctx.Err())
	default:
		//
	}

	l.lock <- struct{}{}
	defer func() {
		<-l.lock
	}()

	prevAvailablePermits := l.oldPermits
	now := time.Now()
	wouldProducePermits := l.catchUp(now)
	l.oldPermits = min(prevAvailablePermits+wouldProducePermits, float64(l.permits))

	// d := l.timeUnitFractionToCreatePermit

	// l.log.Printf("[%2d][%2d] last permit acquired %s ago | prev permits %.3f | would produce %.3f permits | now %.3f permits available",
	// 	bIdx,
	// 	i,
	// 	timeDiff,
	// 	prevAvailablePermits,
	// 	wouldProducePermits,
	// 	l.availablePermits,
	// )

	if l.oldPermits >= 1 {
		l.oldPermits--
		l.lastAt = now
		l.log.Printf("[%2d][%2d] permit granted, %.3f permits were available", bIdx, i, l.oldPermits+1)
		return nil
	}

	// issue: if we acquire all of the permits in "burst" (in a single moment),
	// then we must wait the "limit time" to be able to produce a new one.
	// In that situation we cannot produce a new one by just waiting "time for single permit".
	// If we do that we would be able to produce nearly (2 * l.permits) in a our unit of time (limiterTime).
	//
	// So we meet to wait until that unit ends to produce one permit, and then a new one for every
	// "time for single permit" passed.
	//
	// if available permits is zero, we should calculate if some permits could already be produces
	// based on passed time;
	//

	missingPermitFraction := 1 - l.oldPermits
	waitTime := l.timeUnitFractionToCreatePermit * missingPermitFraction
	waitDuration := time.Duration(waitTime)
	next := time.After(waitDuration)

	l.log.Printf("[%2d][%2d] not enough permits available (we are missing ~%.3f of permit), need to wait %s", bIdx, i, missingPermitFraction, waitDuration)

	select {
	case <-ctx.Done():
		return fmt.Errorf("[%2d][%2d] ctx Done while waiting for the permit, %w", bIdx, i, ctx.Err())
	case <-next:
		l.lastAt = time.Now()
		// we waited a time that corresponds to the fraction of the permit that was missing,
		// e.g. we had 0.3 of te permit due to the different from `l.lastAt` to `now()`, so we need to wait
		// the time for the rest of the permit, i.e. `~(0.7*timeForSinglePermit)`
		// after that we have full permit, but we instantly use that so the available permits
		// is zero after this operation;
		l.oldPermits = 0
		l.log.Printf("[%2d][%2d] permit awaited (%s)", bIdx, i, waitDuration)
		return nil
	}
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

	if l.permits == float64(Unlimited) {
		return nil
	}

	if n > int64(l.permits) {
		return fmt.Errorf("[%2d][%2d] requested amount (%d) exceeds current limit (%d)", bIdx, i, n, int64(l.permits))
	}

	l.lock <- struct{}{}
	defer func() { <-l.lock }()

	availableNow := l.available(time.Now())

	if int64(availableNow) >= n {
		l.log.Printf("[%2d][%2d] permits are available right away", bIdx, i)
		l.lastAt = time.Now()
		l.oldPermits = availableNow - float64(n)
		return nil
	}

	missingPermits := float64(n) - availableNow

	timeToCatchUp := l.calcTimetoCatchUp(missingPermits)

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
		l.oldPermits = 0
		l.log.Printf("[%2d][%2d] %.3f permits awaited (%s)", bIdx, i, missingPermits, timeToCatchUp)
		return nil
	}
}

func (l *Limiter) calcTimetoCatchUp(needPermits float64) time.Duration {
	return time.Duration(l.timeUnitFractionToCreatePermit * needPermits)
}

func (l *Limiter) AllowNow(n int64) bool {
	if n > int64(l.permits) {
		return false
	}

	return l.AvailableNow() >= n
}

func (l *Limiter) Allow(n int64, at time.Time) bool {
	if n > int64(l.permits) {
		return false
	}

	return l.Available(at) >= n
}

func (l *Limiter) AvailableNow() int64 {
	return l.Available(time.Now())
}

// Available returns number of available permits at given time
func (l *Limiter) Available(at time.Time) int64 {

	return int64(l.available(at))
}

func (l *Limiter) available(at time.Time) float64 {
	possiblePermits := l.catchUp(at)
	return possiblePermits + l.oldPermits
}

// catchUp calculates number of permits that would be produced since the last time the permit was acquired until now;
//
// catchUp does not update the Limiter;
func (l *Limiter) catchUp(due time.Time) float64 {

	lastAcquiredAt := l.lastAt
	passedTime := due.Sub(lastAcquiredAt)

	if passedTime >= l.timeUnit {
		return l.permits
	}

	wouldProducePermits := float64(passedTime) / l.timeUnitFractionToCreatePermit

	return wouldProducePermits
}

func calcTimeForSinglePermit(permitsInTimeUnit float64, timeUnit time.Duration) time.Duration {
	return time.Duration(calcTimeUnitFractionForSinglePermit(permitsInTimeUnit, timeUnit))
}

func calcTimeUnitFractionForSinglePermit(permitsInTimeUnit float64, timeUnit time.Duration) float64 {
	return float64(timeUnit) / permitsInTimeUnit
}
