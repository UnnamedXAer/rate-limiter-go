package main

import (
	"context"
	"log"
	"os"
	"sync"
	"time"

	"github.com/unnamedxaer/rate/rate"
)

var mlog = log.New(os.Stdout, "M > ", log.Lmicroseconds)

var totalJobsScheduled int
var processedJobsCnt int
var enabled bool

var mu sync.Mutex

func main() {
	mlog.Print("starting...")

	limiter := rate.NewLimiter(50)

	exit := make(chan struct{})

	enabled = true
	// time.AfterFunc(time.Second*60, func() {
	time.AfterFunc(time.Second*20+time.Millisecond*50, func() {
		mu.Lock()
		enabled = false
		cnt := processedJobsCnt
		mu.Unlock()

		mlog.Printf("Jobs done: %d | total jobs scheduled: ~%d", cnt, totalJobsScheduled)

		// time.Sleep(10 * time.Millisecond)
		exit <- struct{}{}
	})

	for bIdx := range 2 {

		if !enabled {
			break
		}

		// time.Sleep(time.Duration(1000+bIdx) * time.Millisecond)

		// if totalJobsScheduled-processedJobsCnt > 10_000 {
		// 	time.Sleep(2 * time.Millisecond)
		// 	continue
		// }
		time.Sleep(time.Second * 12)
		startJobs(bIdx, 10, limiter)

		time.Sleep(time.Second * 7)

		startJobs(bIdx, 70, limiter)

		// time.Sleep(time.Second * 7)

		// startJobs(bIdx, 38, limiter)

		// t := (rand.Intn(100) + 100)
		// time.Sleep(time.Millisecond * time.Duration(t))

		// time.Sleep(time.Second * 4)
		break

	}

	<-exit
	mlog.Print("program done")

	time.Sleep(3000)
}

func startJobs(bIdx, cnt int, limiter *rate.Limiter) {
	if !enabled {
		return
	}

	for i := range cnt {

		if enabled {
			totalJobsScheduled++
		} else {
			return
		}

		go func(i int) {
			ctx := context.WithValue(context.Background(), 'i', i)
			ctx = context.WithValue(ctx, 'b', bIdx)
			// if i < 3 {
			// 	var cancel context.CancelFunc
			// 	ctx, cancel = context.WithCancel(ctx)
			// 	defer cancel()

			// 	time.AfterFunc(time.Millisecond*1234, func() {
			// 		mlog.Printf("[%2d][%2d] canceling", bIdx, i)
			// 		cancel()
			// 	})
			// }
			// if i > 3 && i < 5 {
			// 	var cancel context.CancelFunc
			// 	ctx, cancel = context.WithDeadline(ctx, time.Now().Add(time.Millisecond*1111))
			// 	defer cancel()
			// }

			err := limiter.Wait(ctx)
			if err != nil {
				mlog.Printf("[%2d][%2d] wait error: %s", bIdx, i, err)
				return
			}

			mu.Lock()
			if enabled {
				processedJobsCnt++
			}
			mu.Unlock()
		}(i)
	}
}
