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
		t.Errorf(
			"with limiter of 50 permits per 1 second expect to have at least %d permits after 333 ms",
			expectedAtLeast)
	}

	expectedNotAvailable := int64(permitsLimit / 2)
	got2 := limiter.AllowNow(expectedNotAvailable)

	if got2 {
		t.Errorf(
			"with limiter of 50 permits per 1 second expect to have %d permits after 333 ms, but at least %d were available",
			expectedAtLeast,
			expectedNotAvailable)
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
		t.Errorf(
			"with limiter of 50 permits per 1 second expect to have produce %d permits after 333 ms, with %d waisted, %d should be available but were not",
			producedPermits,
			waistPermits,
			expectedAvailable)
	}

	if got2 {
		t.Errorf(
			"with limiter of 50 permits per 1 second expect to have produce %d permits after 333 ms, but %d were waisted, so %d should not be available but were available",
			producedPermits,
			waistPermits,
			expectedNotAvailable)
	}
}

func TestAllow(t *testing.T) {
	t.Parallel()

	var permitsLimit float64 = 50.0
	var timeLimitFraction float64 = 0.333
	timeUnit := time.Second

	limiter := NewLimiter(PermitsLimit(permitsLimit), timeUnit)

	d := time.Duration(timeLimitFraction * float64(timeUnit))
	time.Sleep(d)

	timeAt := time.Now().Add(time.Second)
	expectedAvailable := int64(permitsLimit + permitsLimit*timeLimitFraction)

	// TODO: rethink Allow, should it report true for timeAt that is later than now+timeUnit
	// and number of needed permits exceed permits limit?
	// the caller would have to consume permits in realtime to prevent their expiration;
	got := limiter.Allow(expectedAvailable, timeAt)

	if !got {
		t.Errorf("with limiter of 50 permits per 1 second expect to have ~%d permits after at %s, but didn't",
			expectedAvailable,
			timeAt)
	}

	expectedNotAvailable := int64(expectedAvailable) + 5
	got2 := limiter.Allow(expectedNotAvailable, timeAt)

	if got2 {
		t.Errorf(
			"with limiter of 50 permits per 1 second expect to have no more than ~%d permits after at %s, but got at least %d",
			expectedAvailable,
			timeAt,
			expectedNotAvailable)
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
		t.Errorf(
			"with limiter of 50 permits per 1 second expect to have ~%d permits after at %s, but didn't",
			expectedAvailable,
			timeAt)
	}

	expectedNotAvailable := int64(expectedAvailable) + 2
	got2 := limiter.Allow(expectedNotAvailable, timeAt)

	if got2 {
		t.Errorf(
			"with limiter of 50 permits per 1 second expect to have no more than ~%d permits after at %s, but got at least %d",
			expectedAvailable,
			timeAt,
			expectedNotAvailable)
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
		t.Errorf(
			"with limiter of 50 permits per 1 second expect to have produce %d permits after 1s 333 ms, with %d waisted, %d should be available but were not",
			producedPermits,
			waistPermits,
			expectedAvailable)
	}

	if got2 {
		t.Errorf(
			"with limiter of 50 permits per 1 second expect to have produce %d permits after 1s 333 ms, but %d were waisted, so %d should not be available but were available",
			producedPermits,
			waistPermits,
			expectedNotAvailable)
	}
}

func TestAvailableDoNotAccumulateOverLimit(t *testing.T) {
	t.Parallel()

	var permitsLimit float64 = 50.0
	var timeUnit time.Duration = time.Second

	limiter := NewLimiter(PermitsLimit(permitsLimit), timeUnit)

	timeAt := time.Now().Add(2 * timeUnit)
	expectedAvailable := int64(permitsLimit)

	got := limiter.Allow(expectedAvailable, timeAt)

	if !got {
		t.Errorf(
			"with limiter of 50 permits per 1 second expect to have %d permits after at %s, but didn't",
			expectedAvailable,
			timeAt)
	}

	expectedNotAvailable := int64(expectedAvailable) + 1
	got2 := limiter.Allow(expectedNotAvailable, timeAt)

	if got2 {
		t.Errorf("limiter should expire older permits")
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

	if processedCnt > 50 || processedCnt < 45 {
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

	if processedCnt > 50 || processedCnt < 45 {
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
	scheduleJobsBundle(ctx, done, limiter, mu, 0, 10, &totalStartedCnt, &processedCnt, &cancelledCnt)

	<-done
	cancel()

	if processedCnt != 0 {
		t.Errorf(
			"all jobs should be cancel due to their context was cancelled before, processed jobs: got=%d",
			processedCnt)
	}

	if cancelledCnt != 10 {
		t.Errorf(
			"all jobs should be cancel due to their context was cancelled before, cancelled jobs: got=%d",
			cancelledCnt)
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
	time.AfterFunc(time.Second+15*time.Millisecond, func() {
		close(done)
	})

	ctx, cancel := context.WithCancel(t.Context())

	<-time.After(time.Second)
	scheduleJobsBundle(ctx, done, limiter, mu, 2, 101, &totalStartedCnt, &processedCnt, &cancelledCnt)

	<-done
	cancel()

	if processedCnt < 49 {
		t.Errorf(
			"with limiter set to process ~50 jobs per 1s and 101 jobs scheduled after one second I would expect to have processed ~50 of them immediately: got=%d",
			processedCnt)
	}

	want := 50 + 1
	if processedCnt >= want {
		t.Errorf("should not process more than %d jobs in given time, got=%d", want, processedCnt)
	}
}

func TestLimiterPermitsDoesNotAccumulateOverTime(t *testing.T) {
	t.Parallel()

	timeUnit := 500 * time.Millisecond
	limiter := NewLimiter(50, timeUnit)

	mu := &sync.Mutex{}
	processedCnt := 0
	cancelledCnt := 0
	totalStartedCnt := 0

	done := make(chan struct{})

	time.AfterFunc(2*timeUnit+20*time.Millisecond, func() {
		close(done)
	})

	ctx, cancel := context.WithCancel(t.Context())

	<-time.After(2 * timeUnit)
	scheduleJobsBundle(ctx, done, limiter, mu, 2, 201, &totalStartedCnt, &processedCnt, &cancelledCnt)

	<-done
	cancel()

	if processedCnt < 49 {
		t.Errorf(
			"with limiter set to process ~50 jobs per 0.5s and 201 jobs scheduled after one second I would expect to have processed at ~50 of them: got=%d",
			processedCnt)
		// because permits older than 'time unit' should expire
	}

	want := 50 + 1
	if processedCnt < want {
		t.Errorf(
			"permits older than %s should expire. expect to process no more than %d jobs in given time, got=%d",
			timeUnit,
			want,
			processedCnt)
	}
}

////////////////// wait many

func TestLimiterWaitABunchAndAskRightAway(t *testing.T) {
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
	scheduleManyTaskJobsBundle(ctx, done, limiter, mu, 0, 20, 5, &totalStartedCnt, &processedCnt, &cancelledCnt)

	<-done
	cancel()

	if processedCnt != 2 {
		t.Errorf("with limiter set to process ~50 jobs per 1s: got=%d", processedCnt)
	}
}

func TestLimiterWaitABunchWithJobsScheduledInPackagesOverTime(t *testing.T) {
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

	scheduleManyTaskJobsBundle(ctx, done, limiter, mu, 0, 10, 1, &totalStartedCnt, &processedCnt, &cancelledCnt)

	select {
	case <-done:
	case <-time.After(400 * time.Millisecond):
	}

	scheduleManyTaskJobsBundle(ctx, done, limiter, mu, 1, 20, 1, &totalStartedCnt, &processedCnt, &cancelledCnt)

	select {
	case <-done:
	case <-time.After(400 * time.Millisecond):
	}

	scheduleManyTaskJobsBundle(ctx, done, limiter, mu, 2, 110, 1, &totalStartedCnt, &processedCnt, &cancelledCnt)

	<-done
	cancel()

	if processedCnt != 2 {
		t.Errorf("with limiter set to process ~50 jobs per 1s: got=%d", processedCnt)
	}
}

func TestLimiterWaitABunchCtxCanceled(t *testing.T) {
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
	scheduleManyTaskJobsBundle(ctx, done, limiter, mu, 0, 2, 10, &totalStartedCnt, &processedCnt, &cancelledCnt)

	<-done
	cancel()

	if processedCnt != 0 {
		t.Errorf(
			"all jobs should be cancel due to their context was cancelled before, processed jobs: got=%d",
			processedCnt)
	}

	if cancelledCnt != 10 {
		t.Errorf(
			"all jobs should be cancel due to their context was cancelled before, cancelled jobs: got=%d",
			cancelledCnt)
	}
}

func TestLimiterWaitABunchPermitsDoesNotAccumulateOverLimit(t *testing.T) {
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
	scheduleManyTaskJobsBundle(ctx, done, limiter, mu, 2, 5, 21, &totalStartedCnt, &processedCnt, &cancelledCnt)

	<-done
	cancel()

	if processedCnt < 5 {
		t.Errorf(
			"with limiter set to process ~50 jobs per 0.5s, after one second I would expect to have processed at ~50 of them: got=%d",
			processedCnt)
	}

	want := 5 + 1
	if processedCnt < want {
		t.Errorf(
			"should not over-accumulate permits and process no more than %d jobs in given time, got=%d",
			want,
			processedCnt)
	}
}

func scheduleJobsBundle(
	ctx context.Context,
	done chan struct{},
	limiter *Limiter,
	mu *sync.Mutex,
	bundleIdx, jobsToSchedule int,
	totalStartedCnt, processedCnt, canceledJobsCnt *int,
) {
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

func scheduleManyTaskJobsBundle(
	ctx context.Context,
	done chan struct{},
	limiter *Limiter,
	mu *sync.Mutex,
	bundleIdx, neededPermits int,
	jobsToSchedule int,
	totalStartedCnt, processedCnt, canceledJobsCnt *int,
) {
	// outer:
	for i := range jobsToSchedule {
		// select {
		// case <-done:
		// 	break outer
		// default:
		// }
		mbStartMultiTaskJob(ctx, limiter, mu, done, neededPermits, totalStartedCnt, processedCnt, canceledJobsCnt, bundleIdx, i)
	}
}

func mbStartMultiTaskJob(
	ctx context.Context,
	limiter *Limiter,
	mu *sync.Mutex,
	done <-chan struct{},
	neededPermits int,
	totalJobsScheduled, doneJobsCnt, canceledJobsCnt *int,
	bundleIdx, idx int) {

	go func() {
		mu.Lock()
		*totalJobsScheduled++
		mu.Unlock()

		ctx = context.WithValue(ctx, 'b', bundleIdx)
		ctx = context.WithValue(ctx, 'i', idx)

		err := limiter.WaitMany(ctx, int64(neededPermits))
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

///// bunch

func TestReserveABunch(t *testing.T) {
	t.Parallel()

	var limit PermitsLimit = 50
	var timeUnit time.Duration = time.Second

	lim := NewLimiter(limit, timeUnit)

	var n int64 = 10 // -> ~200ms
	ctx := t.Context()

	r := lim.Reserve(ctx, n)

	now := time.Now()
	expectedDue := now.Add(200 * time.Millisecond)

	assertSuccessfulReservation(t, lim, ctx, r, n, expectedDue, now)
}

func TestReserveABunchWithDeadline(t *testing.T) {
	t.Parallel()

	var limit PermitsLimit = 50
	var timeUnit time.Duration = time.Second

	lim := NewLimiter(limit, timeUnit)

	var n int64 = 10 // -> ~200ms
	ctx, stop := context.WithDeadline(t.Context(), time.Now().Add(220*time.Millisecond))
	defer stop()

	r := lim.Reserve(ctx, n)
	now := time.Now()
	expectedDue := now.Add(200 * time.Millisecond)

	assertSuccessfulReservation(t, lim, ctx, r, n, expectedDue, now)
}

func TestReserveABunchWithUnrealisticDeadline(t *testing.T) {
	t.Parallel()

	var limit PermitsLimit = 50
	var timeUnit time.Duration = time.Second

	lim := NewLimiter(limit, timeUnit)

	var n int64 = 10 // -> ~200ms
	ctx, stop := context.WithDeadline(t.Context(), time.Now().Add(180*time.Millisecond))
	defer stop()

	r := lim.Reserve(ctx, n)
	assertFailedReservation(t, r)
}

func TestReserveABunchOverUnitPermitsLimit(t *testing.T) {
	t.Parallel()

	var limit PermitsLimit = 50
	var timeUnit time.Duration = time.Second

	lim := NewLimiter(limit, timeUnit)

	var n int64 = 50 + 10 // -> ~1200ms
	ctx, stop := context.WithDeadline(t.Context(), time.Now().Add(1280*time.Millisecond))
	defer stop()

	r := lim.Reserve(ctx, n)
	now := time.Now()
	expectedDue := now.Add(1200 * time.Millisecond)
	assertSuccessfulReservation(t, lim, ctx, r, n, expectedDue, now)
}

func TestReserveABunchRestore(t *testing.T) {
	t.Parallel()

	var limit PermitsLimit = 50
	var timeUnit time.Duration = time.Second

	t.Run("when no time passed nothing should be restored", func(t *testing.T) {
		t.Parallel()

		lim := NewLimiter(limit, timeUnit)

		initialPermits := lim.permits
		initialLastAt := lim.lastAt
		ctx := t.Context()

		now := time.Now()
		expectedDue := now.Add(200 * time.Millisecond)
		var n int64 = 10

		r := lim.Reserve(ctx, n)

		assertSuccessfulReservation(t, lim, ctx, r, n, expectedDue, now)

		wantAvailablePermits := int64(initialPermits)
		currPermits := int64(lim.permits)

		err := r.Restore()
		assertSuccessfulRestore(t, lim, err, initialLastAt, currPermits, wantAvailablePermits)
	})

	t.Run("when some time passed only corresponding part of permits should be restored", func(t *testing.T) {
		t.Parallel()

		lim := NewLimiter(limit, timeUnit)

		initialPermits := lim.permits
		initialLastAt := lim.lastAt
		ctx := t.Context()

		now := time.Now()
		expectedDue := now.Add(200 * time.Millisecond)
		var n int64 = 10

		r := lim.Reserve(ctx, n)

		assertSuccessfulReservation(t, lim, ctx, r, n, expectedDue, now)

		time.Sleep(100 * time.Millisecond)

		wantAvailablePermits := int64(initialPermits) + 5
		currPermits := int64(lim.permits)

		err := r.Restore()
		assertSuccessfulRestore(t, lim, err, initialLastAt, currPermits, wantAvailablePermits)
	})

	t.Run("when reservation time passed nothing should be restored", func(t *testing.T) {
		t.Parallel()

		lim := NewLimiter(limit, timeUnit)

		ctx := t.Context()

		now := time.Now()
		expectedDue := now.Add(200 * time.Millisecond)
		var n int64 = 10

		r := lim.Reserve(ctx, n)

		assertSuccessfulReservation(t, lim, ctx, r, n, expectedDue, now)

		time.Sleep(200 * time.Millisecond)

		wantAvailablePermits := int64(lim.permits)
		currPermits := int64(lim.permits) // TODO: start here

		err := r.Restore()
		assertSuccessfulRestore(t, lim, err, time.Time{}, currPermits, wantAvailablePermits)
	})

	t.Run("restore should not exceed the permits limit", func(t *testing.T) {
		t.Parallel()

		lim := NewLimiter(limit, timeUnit)

		initialLastAt := lim.lastAt
		ctx := t.Context()

		now := time.Now()
		expectedDue := now.Add(1200 * time.Millisecond)
		var n int64 = 60

		r := lim.Reserve(ctx, n)

		assertSuccessfulReservation(t, lim, ctx, r, n, expectedDue, now)

		time.Sleep(1100 * time.Millisecond)

		wantAvailablePermits := int64(limit)
		currPermits := int64(lim.permits)

		err := r.Restore()
		assertSuccessfulRestore(t, lim, err, initialLastAt, currPermits, wantAvailablePermits)
	})

	t.Run("restoring of failed reservation should not be possible", func(t *testing.T) {
		t.Parallel()

		lim := NewLimiter(limit, timeUnit)

		ctx := t.Context()

		var n int64 = 60

		r := lim.Reserve(ctx, n)
		assertFailedReservation(t, r)

		err := r.Restore()
		assertFailedRestore(t, err)
	})
}

func assertFailedRestore(t *testing.T, err error) {
	t.Helper()

	if err == nil {
		t.Fatalf("restoring failed reservation did not return error, but should")
	}
}

func assertSuccessfulRestore(t *testing.T, lim *Limiter, err error, prevLastAt time.Time, currPermits, wantAvailablePermits int64) {
	t.Helper()

	if err != nil {
		t.Fatalf("restoring reserved permits failed: %s", err)
	}

	if currPermits != wantAvailablePermits {
		t.Fatalf("wrong number of permits after restore, want=%d, got=%d", wantAvailablePermits, currPermits)
	}

	if !around(lim.lastAt, prevLastAt) {
		t.Fatalf("wrong lastAt after restore, want=~%s, got=%s", prevLastAt, lim.lastAt)
	}
}

func assertFailedReservation(t *testing.T, r Reservation) {
	t.Helper()

	if r.Ok {
		t.Fatal("reservation was successful, but expected failed")
	}
}

func assertSuccessfulReservation(
	t *testing.T,
	lim *Limiter,
	ctx context.Context,
	r Reservation,
	expectedPermits int64,
	expectedDue time.Time,
	now time.Time,
) {
	t.Helper()

	if !r.Ok {
		t.Fatalf("reservation failed, but expected successful")
	}

	if r.Permits != expectedPermits {
		t.Fatalf("wrong number of reserved permits. want=%d, got=%d", expectedPermits, r.Permits)
	}

	if !around(r.Due, expectedDue) {
		t.Fatalf("wrong due time, expected around %s in the future, got=%s", expectedDue.Sub(now), r.Due.Sub(now))
	}

	if lim.lastAt != r.Due {
		t.Fatalf("lastAt should be set to due")
	}

	if deadline, ok := ctx.Deadline(); ok {
		if deadline.Before(r.Due) {
			t.Fatalf("due exceeds deadline")
		}
	}
}

func around(t time.Time, nearTo time.Time, plusMinus ...time.Duration) bool {
	offset := 5 * time.Millisecond

	if len(plusMinus) > 0 {
		offset = plusMinus[0]
	}

	min := nearTo.Add(-offset)
	max := nearTo.Add(offset)

	return t.After(min) && t.Before(max)
}
