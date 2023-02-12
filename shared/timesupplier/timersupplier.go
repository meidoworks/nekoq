package timesupplier

import (
	"time"

	"github.com/meidoworks/nekoq/shared/workgroup"
)

var (
	cachedTime time.Time
)

const (
	ResolutionInMillis = 30
)

func init() {
	// if someone use the function, initialize it
	// otherwise, keep quiet
	workgroup.WithFailOver().Run(func() bool {
		ticker := time.NewTicker(ResolutionInMillis * time.Millisecond)
		ch := ticker.C
		for {
			cachedTime = <-ch
		}
	})
	cachedTime = time.Now() // ensure there is an initial value to avoid go sched delay of the timer task
}

func CachedTime() time.Time {
	return cachedTime
}
