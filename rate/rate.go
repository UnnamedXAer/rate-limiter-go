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
const InfiniteLimit PermitsLimit = math.MaxFloat64

type Limiter struct {
	permits                        float64
	timeToCreatePermit             time.Duration
	timeUnitFractionToCreatePermit float64
	availablePermits               float64
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
		timeToCreatePermit:             calcTimeForSinglePermit(float64(permitsPerMinute), limiterTimeUnit),
		timeUnitFractionToCreatePermit: calcTimeUnitFractionForSinglePermit(float64(permitsPerMinute), limiterTimeUnit),
		availablePermits:               float64(0),
		lastAt:                         time.Now(),
		lock:                           lock,
		log:                            log.New(os.Stdout, "L > ", log.Lmicroseconds),
	}

	l.log.Printf("time for one permit: %s | %.5f", l.timeToCreatePermit, l.timeUnitFractionToCreatePermit)

	return l
}

func (l *Limiter) Wait(ctx context.Context) error {

	i, ok := ctx.Value('i').(int)
	if !ok {
		i = -1
	}
	bIdx, ok := ctx.Value('b').(int)
	if !ok {
		bIdx = -1
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

	prevAvailablePermits := l.availablePermits
	now := time.Now()
	wouldProducePermits := l.catchUp(now)
	l.availablePermits = min(prevAvailablePermits+wouldProducePermits, float64(l.permits))

	// d := l.timeUnitFractionToCreatePermit

	// l.log.Printf("[%2d][%2d] last permit acquired %s ago | prev permits %.3f | would produce %.3f permits | now %.3f permits available",
	// 	bIdx,
	// 	i,
	// 	timeDiff,
	// 	prevAvailablePermits,
	// 	wouldProducePermits,
	// 	l.availablePermits,
	// )

	if l.availablePermits >= 1 {
		l.availablePermits--
		l.lastAt = now
		l.log.Printf("[%2d][%2d] permit granted, %.3f permits were available", bIdx, i, l.availablePermits+1)
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

	missingPermitFraction := 1 - l.availablePermits
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
		l.availablePermits = 0
		l.log.Printf("[%2d][%2d] permit awaited (%s)", bIdx, i, waitDuration)
		return nil
	}
}

func (l *Limiter) AllowNow(n int64) bool {

	return l.AvailableNow() >= n
}

func (l *Limiter) Allow(n int64, at time.Time) bool {
	return l.Available(at) >= n
}

func (l *Limiter) AvailableNow() int64 {
	return l.Available(time.Now())
}

// Available returns number of available permits at given time
func (l *Limiter) Available(at time.Time) int64 {

	possiblePermits := l.catchUp(at)

	return int64(possiblePermits + l.availablePermits)
}

// catchUp calculates number of permits that would be produced since the last time the permit was acquired until now;
//
// catchUp does not update the Limiter;
func (l *Limiter) catchUp(due time.Time) float64 {

	lastAcquiredAt := l.lastAt
	passedTime := due.Sub(lastAcquiredAt)
	wouldProducePermits := float64(passedTime) / l.timeUnitFractionToCreatePermit

	return wouldProducePermits
}

func calcTimeForSinglePermit(permitsInTimeUnit float64, timeUnit time.Duration) time.Duration {
	return time.Duration(calcTimeUnitFractionForSinglePermit(permitsInTimeUnit, timeUnit))
}

func calcTimeUnitFractionForSinglePermit(permitsInTimeUnit float64, timeUnit time.Duration) float64 {
	return float64(timeUnit) / permitsInTimeUnit
}
