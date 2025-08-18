package rate

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestAllowNow(t *testing.T) {
	t.Parallel()

	var permitsLimit float64 = 50.0
	var timeLimitFraction float64 = 0.333

	limiter := NewLimiter(PermitsLimit(permitsLimit), time.Second)

	d := time.Duration(timeLimitFraction * float64(time.Second))
	time.Sleep(d)

	expectedAtLeast := int64(permitsLimit * timeLimitFraction)

	got := limiter.AllowNow(expectedAtLeast)

	if !got {
		t.Errorf("with limiter of 50 permits per 1 second expect to have at least %d permits after 333 ms", expectedAtLeast)
	}

	expectedNotAvailable := int64(permitsLimit / 2)
	got2 := limiter.AllowNow(expectedNotAvailable)

	if got2 {
		t.Errorf("with limiter of 50 permits per 1 second expect to have %d permits after 333 ms, but at least %d were available", expectedAtLeast, expectedNotAvailable)
	}
}

func TestAllowNowWithSomePermitsUsed(t *testing.T) {
	t.Parallel()

	var permitsLimit float64 = 50.0
	var timeLimitFraction float64 = 0.333

	limiter := NewLimiter(PermitsLimit(permitsLimit), time.Second)

	d := time.Duration(timeLimitFraction * float64(time.Second))
	time.Sleep(d)

	producedPermits := int64(permitsLimit * timeLimitFraction)

	waistPermits := producedPermits / 3

	for range waistPermits {
		limiter.Wait(t.Context())
	}

	expectedAvailable := producedPermits - waistPermits

	got := limiter.AllowNow(expectedAvailable)

	expectedNotAvailable := expectedAvailable + waistPermits/3
	got2 := limiter.AllowNow(producedPermits)

	if !got {
		t.Errorf("with limiter of 50 permits per 1 second expect to have produce %d permits after 333 ms, with %d waisted, %d should be available but were not", producedPermits, waistPermits, expectedAvailable)
	}

	if got2 {
		t.Errorf("with limiter of 50 permits per 1 second expect to have produce %d permits after 333 ms, but %d were waisted, so %d should not be available but were available", producedPermits, waistPermits, expectedNotAvailable)
	}
}

func TestAllow(t *testing.T) {
	t.Parallel()

	var permitsLimit float64 = 50.0
	var timeLimitFraction float64 = 0.333

	limiter := NewLimiter(PermitsLimit(permitsLimit), time.Second)

	d := time.Duration(timeLimitFraction * float64(time.Second))
	time.Sleep(d)

	timeAt := time.Now().Add(time.Second)
	expectedAvailable := int64(permitsLimit + permitsLimit*timeLimitFraction)

	got := limiter.Allow(expectedAvailable, timeAt)

	if !got {
		t.Errorf("with limiter of 50 permits per 1 second expect to have ~%d permits after at %s, but didn't", expectedAvailable, timeAt)
	}

	expectedNotAvailable := int64(expectedAvailable) + 5
	got2 := limiter.Allow(expectedNotAvailable, timeAt)

	if got2 {
		t.Errorf("with limiter of 50 permits per 1 second expect to have no more than ~%d permits after at %s, but got at least %d", expectedAvailable, timeAt, expectedNotAvailable)
	}
}

func TestAllowTimeBeforeLastAcquire(t *testing.T) {
	t.Parallel()

	var permitsLimit float64 = 50.0

	limiter := NewLimiter(PermitsLimit(permitsLimit), time.Second)

	limiter.Wait(t.Context())
	limiter.Wait(t.Context())
	limiter.Wait(t.Context())
	limiter.Wait(t.Context())

	timeAt := time.Now().Add(time.Second)

	expectedAvailable := int64(permitsLimit)

	got := limiter.Allow(expectedAvailable, timeAt)

	if !got {
		t.Errorf("with limiter of 50 permits per 1 second expect to have ~%d permits after at %s, but didn't", expectedAvailable, timeAt)
	}

	expectedNotAvailable := int64(expectedAvailable) + 2
	got2 := limiter.Allow(expectedNotAvailable, timeAt)

	if got2 {
		t.Errorf("with limiter of 50 permits per 1 second expect to have no more than ~%d permits after at %s, but got at least %d", expectedAvailable, timeAt, expectedNotAvailable)
	}
}

func TestAllowWithSomePermitsUsed(t *testing.T) {
	t.Parallel()

	var permitsLimit float64 = 50.0
	var timeLimitFraction float64 = 0.333

	limiter := NewLimiter(PermitsLimit(permitsLimit), time.Second)

	d := time.Duration(timeLimitFraction * float64(time.Second))
	time.Sleep(d)

	timeAt := time.Now().Add(time.Second)

	producedPermits := int64(permitsLimit + permitsLimit*timeLimitFraction)
	waistPermits := producedPermits / 3

	for range waistPermits {
		limiter.Wait(t.Context())
	}

	expectedAvailable := producedPermits - waistPermits

	got := limiter.Allow(expectedAvailable, timeAt)

	expectedNotAvailable := expectedAvailable + waistPermits/3
	got2 := limiter.Allow(producedPermits, timeAt)

	if !got {
		t.Errorf("with limiter of 50 permits per 1 second expect to have produce %d permits after 1s 333 ms, with %d waisted, %d should be available but were not", producedPermits, waistPermits, expectedAvailable)
	}

	if got2 {
		t.Errorf("with limiter of 50 permits per 1 second expect to have produce %d permits after 1s 333 ms, but %d were waisted, so %d should not be available but were available", producedPermits, waistPermits, expectedNotAvailable)
	}
}

func TestLimiterStartRightAway(t *testing.T) {
	t.Parallel()

	// 50 per 1s
	limiter := NewLimiter(50 * 60)

	mu := &sync.Mutex{}
	processedCnt := 0
	cancelledCnt := 0
	totalStartedCnt := 0

	done := make(chan struct{})

	time.AfterFunc(time.Second, func() {
		close(done)
	})

	ctx, cancel := context.WithCancel(t.Context())
	scheduleJobsBundle(ctx, done, limiter, mu, 0, 100, &totalStartedCnt, &processedCnt, &cancelledCnt)

	<-done
	cancel()

	if processedCnt > 50 || processedCnt < 48 {
		t.Errorf("with limiter set to process ~50 jobs per 1s: got=%d", processedCnt)
	}
}

func TestLimiterWithJobsScheduledInPackagesOverTime(t *testing.T) {
	t.Parallel()

	limiter := NewLimiter(50 * 60)

	mu := &sync.Mutex{}
	processedCnt := 0
	cancelledCnt := 0
	totalStartedCnt := 0

	done := make(chan struct{})

	time.AfterFunc(time.Second, func() {
		close(done)
	})

	ctx, cancel := context.WithCancel(t.Context())

	scheduleJobsBundle(ctx, done, limiter, mu, 0, 10, &totalStartedCnt, &processedCnt, &cancelledCnt)

	select {
	case <-done:
	case <-time.After(400 * time.Millisecond):
	}

	scheduleJobsBundle(ctx, done, limiter, mu, 1, 20, &totalStartedCnt, &processedCnt, &cancelledCnt)

	select {
	case <-done:
	case <-time.After(400 * time.Millisecond):
	}

	scheduleJobsBundle(ctx, done, limiter, mu, 2, 110, &totalStartedCnt, &processedCnt, &cancelledCnt)

	<-done
	cancel()

	if processedCnt > 50 || processedCnt < 48 {
		t.Errorf("with limiter set to process ~50 jobs per 1s: got=%d", processedCnt)
	}
}

func TestLimiterCtxCanceled(t *testing.T) {
	t.Parallel()

	limiter := NewLimiter(50 * 60)

	mu := &sync.Mutex{}
	processedCnt := 0
	cancelledCnt := 0
	totalStartedCnt := 0

	done := make(chan struct{})

	time.AfterFunc(time.Second, func() {
		close(done)
	})

	ctx, cancel := context.WithCancel(t.Context())

	cancel()
	<-time.After(time.Millisecond * 10)
	scheduleJobsBundle(ctx, done, limiter, mu, 2, 10, &totalStartedCnt, &processedCnt, &cancelledCnt)

	<-done
	cancel()

	if processedCnt != 0 {
		t.Errorf("all jobs should be cancel due to their context was cancelled before, processed jobs: got=%d", processedCnt)
	}

	if cancelledCnt != 10 {
		t.Errorf("all jobs should be cancel due to their context was cancelled before, cancelled jobs: got=%d", cancelledCnt)
	}
}

func TestLimiterAccumulatingPermits(t *testing.T) {
	t.Parallel()

	limiter := NewLimiter(50 * 60)

	mu := &sync.Mutex{}
	processedCnt := 0
	cancelledCnt := 0
	totalStartedCnt := 0

	done := make(chan struct{})

	// a second -> 50 permits + some time for processing the jobs
	time.AfterFunc(time.Second+30*time.Millisecond, func() {
		close(done)
	})

	ctx, cancel := context.WithCancel(t.Context())

	<-time.After(time.Second)
	scheduleJobsBundle(ctx, done, limiter, mu, 2, 101, &totalStartedCnt, &processedCnt, &cancelledCnt)

	<-done
	cancel()

	if processedCnt < 50 {
		t.Errorf("with limiter set to process ~50 jobs per 1s and 101 jobs scheduled after one second I would expect to have processed at least 50 of them immediately: got=%d", processedCnt)
	}

	want := 50 + 1
	if processedCnt < want {
		t.Errorf("should not process more than %d jobs in given time, got=%d", want, processedCnt)
	}
}

func TestLimiterPermitsDoesNotAccumulateOverLimit(t *testing.T) {
	t.Parallel()

	limiter := NewLimiter(50, 500*time.Millisecond)

	mu := &sync.Mutex{}
	processedCnt := 0
	cancelledCnt := 0
	totalStartedCnt := 0

	done := make(chan struct{})

	time.AfterFunc(1030*time.Millisecond, func() {
		close(done)
	})

	ctx, cancel := context.WithCancel(t.Context())

	<-time.After(time.Second)
	scheduleJobsBundle(ctx, done, limiter, mu, 2, 201, &totalStartedCnt, &processedCnt, &cancelledCnt)

	<-done
	cancel()

	if processedCnt < 50 {
		t.Errorf("with limiter set to process ~50 jobs per 0.5s and 201 jobs scheduled after one second I would expect to have processed at ~50 of them: got=%d", processedCnt)
	}

	want := 50 + 1
	if processedCnt < want {
		t.Errorf("should not over-accumulate permits and process no more than %d jobs in given time, got=%d", want, processedCnt)
	}
}

func scheduleJobsBundle(ctx context.Context, done chan struct{}, limiter *Limiter, mu *sync.Mutex, bundleIdx, jobsToSchedule int, totalStartedCnt, processedCnt, canceledJobsCnt *int) {
	// outer:
	for i := range jobsToSchedule {
		// select {
		// case <-done:
		// 	break outer
		// default:
		// }
		mbStartJob(ctx, limiter, mu, done, totalStartedCnt, processedCnt, canceledJobsCnt, bundleIdx, i)
	}
}

func mbStartJob(
	ctx context.Context,
	limiter *Limiter,
	mu *sync.Mutex,
	done <-chan struct{},
	totalJobsScheduled, doneJobsCnt, canceledJobsCnt *int,
	bundleIdx, idx int) {

	go func() {
		mu.Lock()
		*totalJobsScheduled++
		mu.Unlock()

		ctx = context.WithValue(ctx, 'b', bundleIdx)
		ctx = context.WithValue(ctx, 'i', idx)

		err := limiter.Wait(ctx)
		if err != nil {
			select {
			case <-done:
			default:
				mu.Lock()
				*canceledJobsCnt++
				mu.Unlock()
			}

			return
		}

		select {
		case <-done:
		default:
			mu.Lock()
			*doneJobsCnt++
			mu.Unlock()
		}
	}()
}
