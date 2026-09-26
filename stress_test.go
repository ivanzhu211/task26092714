package vmplace

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type vmRecord struct {
	home      string
	destroyed bool
	vcpu      int
	mem       int
}

func TestStress(t *testing.T) {
	c := New()
	hostIDs := []string{"h0", "h1", "h2", "h3", "h4"}
	for i, id := range hostIDs {
		if err := c.AddHost(id, 32, 128*1024+i*1024); err != nil {
			t.Fatal(err)
		}
	}

	var recMu sync.Mutex
	records := map[string]*vmRecord{}
	var vmCounter atomic.Int64

	pickLive := func(r *rand.Rand) (string, bool) {
		recMu.Lock()
		defer recMu.Unlock()
		var live []string
		for id, rec := range records {
			if !rec.destroyed {
				live = append(live, id)
			}
		}
		if len(live) == 0 {
			return "", false
		}
		return live[r.Intn(len(live))], true
	}

	// Observer: continuously sample every host; any limit violation fails.
	var obsMu sync.Mutex
	var violation string
	stopObs := make(chan struct{})
	obsDone := make(chan struct{})
	go func() {
		defer close(obsDone)
		for {
			select {
			case <-stopObs:
				return
			default:
			}
			for _, id := range hostIDs {
				s, err := c.Inspect(id)
				if err != nil {
					continue
				}
				if s.Usage.VCPU > s.VCPULimit || s.Usage.MemMB > s.MemLimit || s.Usage.VMs > s.VMLimit {
					obsMu.Lock()
					if violation == "" {
						violation = fmt.Sprintf("host %s over limit: %+v", id, s)
					}
					obsMu.Unlock()
				}
			}
			time.Sleep(100 * time.Microsecond)
		}
	}()

	var wg sync.WaitGroup
	for g := 0; g < 24; g++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			r := rand.New(rand.NewSource(seed))
			for i := 0; i < 150; i++ {
				switch r.Intn(100) {
				case 0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
					10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
					20, 21, 22, 23, 24, 25, 26, 27, 28, 29,
					30, 31, 32, 33, 34, 35, 36, 37, 38, 39: // Place
					id := fmt.Sprintf("vm-%d", vmCounter.Add(1))
					vcpu := 1 + r.Intn(8)
					mem := 512 + r.Intn(8)*512
					home, err := c.Place(id, vcpu, mem)
					if err == nil {
						recMu.Lock()
						records[id] = &vmRecord{home: home, vcpu: vcpu, mem: mem}
						recMu.Unlock()
					} else {
						var pe *PlaceError
						if !errors.As(err, &pe) && !errors.Is(err, ErrNotFound) {
							t.Errorf("unexpected place error: %v", err)
						}
					}
				case 40, 41, 42, 43, 44, 45, 46, 47, 48, 49,
					50, 51, 52, 53, 54, 55, 56, 57, 58, 59: // Migrate
					id, ok := pickLive(r)
					if !ok {
						continue
					}
					target := hostIDs[r.Intn(len(hostIDs))]
					ctx := context.Background()
					var cancel context.CancelFunc
					if r.Intn(10) == 0 {
						ctx, cancel = context.WithCancel(ctx)
						cancel() // pre-cancelled
					}
					fail := r.Intn(10) < 3
					err := c.Migrate(ctx, id, target, func(ctx context.Context) error {
						time.Sleep(time.Duration(r.Intn(2000)) * time.Microsecond)
						if fail {
							return errors.New("copy failed")
						}
						return nil
					})
					if cancel != nil {
						cancel()
					}
					if err == nil {
						recMu.Lock()
						if rec, ok := records[id]; ok {
							rec.home = target
						}
						recMu.Unlock()
					}
				case 60, 61, 62, 63, 64, 65, 66, 67, 68, 69,
					70, 71, 72, 73, 74: // Destroy
					id, ok := pickLive(r)
					if !ok {
						continue
					}
					recMu.Lock()
					records[id].destroyed = true
					recMu.Unlock()
					// A racing goroutine may destroy the same VM first.
					if err := c.Destroy(id); err != nil &&
						!errors.Is(err, ErrVMDestroyed) && !errors.Is(err, ErrNotFound) {
						t.Errorf("destroy %s: %v", id, err)
					}
				case 75, 76, 77, 78, 79, 80, 81, 82, 83, 84,
					85, 86, 87, 88, 89: // SetHostState
					id := hostIDs[r.Intn(len(hostIDs))]
					state := HostState(r.Intn(3))
					if err := c.SetHostState(id, state); err != nil {
						t.Errorf("set state: %v", err)
					}
				default: // SetOvercommit
					id := hostIDs[r.Intn(len(hostIDs))]
					cpuPct := 100 + r.Intn(301)
					memPct := 100 + r.Intn(201)
					if _, err := c.SetOvercommit(id, cpuPct, memPct); err != nil &&
						!errors.Is(err, ErrOvercommitTooLow) {
						t.Errorf("set overcommit: %v", err)
					}
				}
			}
		}(int64(g)*7919 + 1)
	}
	wg.Wait()
	close(stopObs)
	<-obsDone

	obsMu.Lock()
	if violation != "" {
		t.Fatalf("limit violated: %s", violation)
	}
	obsMu.Unlock()

	// Reconcile: committed usage must equal the sum over resident VMs
	// implied by the recorded operation results.
	expected := map[string]*Usage{}
	for _, id := range hostIDs {
		expected[id] = &Usage{}
	}
	recMu.Lock()
	for _, rec := range records {
		if rec.destroyed {
			continue
		}
		e := expected[rec.home]
		e.VMs++
		e.VCPU += rec.vcpu
		e.MemMB += rec.mem
	}
	recMu.Unlock()

	for _, id := range hostIDs {
		u, err := c.Usage(id)
		if err != nil {
			t.Fatal(err)
		}
		if u != *expected[id] {
			t.Fatalf("host %s: committed %+v, expected %+v", id, u, *expected[id])
		}
	}
}
