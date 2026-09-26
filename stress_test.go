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

type vmInfo struct {
	host string
	vcpu int
	mem  int
}

// tracker derives expected residency purely from operation results.
type tracker struct {
	mu         sync.Mutex
	resident   map[string]vmInfo
	tombstoned map[string]bool
}

func newTracker() *tracker {
	return &tracker{resident: make(map[string]vmInfo), tombstoned: make(map[string]bool)}
}

func (tr *tracker) placed(id, host string, vcpu, mem int) {
	tr.mu.Lock()
	tr.resident[id] = vmInfo{host: host, vcpu: vcpu, mem: mem}
	tr.mu.Unlock()
}

func (tr *tracker) migrated(id, target string) {
	tr.mu.Lock()
	// A concurrent Destroy may have linearized after Migrate committed;
	// if it already tombstoned the VM, the VM is gone and we must not
	// resurrect it.
	if !tr.tombstoned[id] {
		if info, ok := tr.resident[id]; ok {
			info.host = target
			tr.resident[id] = info
		}
	}
	tr.mu.Unlock()
}

func (tr *tracker) destroyed(id string) {
	tr.mu.Lock()
	tr.tombstoned[id] = true
	delete(tr.resident, id)
	tr.mu.Unlock()
}

func (tr *tracker) randomVM(r *rand.Rand) (string, bool) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	if len(tr.resident) == 0 {
		return "", false
	}
	keys := make([]string, 0, len(tr.resident))
	for k := range tr.resident {
		keys = append(keys, k)
	}
	return keys[r.Intn(len(keys))], true
}

func (tr *tracker) sums() map[string]Usage {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	out := make(map[string]Usage)
	for _, info := range tr.resident {
		u := out[info.host]
		u.VCPU += info.vcpu
		u.MemMB += info.mem
		u.VMs++
		out[info.host] = u
	}
	return out
}

func TestStress(t *testing.T) {
	hostIDs := []string{"h1", "h2", "h3", "h4"}
	m := NewManager()
	for _, id := range hostIDs {
		if err := m.AddHost(id, 16, 32*1024); err != nil {
			t.Fatal(err)
		}
	}

	tr := newTracker()
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var observerViolation atomic.Int32

	// Observer: continuously sample commitments vs limits.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			for _, id := range hostIDs {
				committed, limits, err := m.Snapshot(id)
				if err != nil {
					continue
				}
				if committed.VCPU > limits.VCPU || committed.MemMB > limits.MemMB || committed.VMs > limits.VMs {
					observerViolation.Add(1)
				}
			}
		}
	}()

	// Churn: host states and overcommit ratios.
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			r := rand.New(rand.NewSource(seed))
			for {
				select {
				case <-stop:
					return
				default:
				}
				id := hostIDs[r.Intn(len(hostIDs))]
				if r.Intn(2) == 0 {
					st := Available
					switch r.Intn(4) {
					case 1:
						st = Maintenance
					case 2:
						st = Offline
					}
					_ = m.SetHostState(id, st)
				} else {
					cpuPct := 100 + r.Intn(301)
					memPct := 100 + r.Intn(201)
					if r.Intn(4) == 0 {
						cpuPct = 50 // provoke rejections
					}
					_, _ = m.SetOvercommit(id, cpuPct, memPct)
				}
				time.Sleep(time.Duration(r.Intn(3)) * time.Millisecond)
			}
		}(int64(i) + 1)
	}

	// Workers: Place / Migrate / Destroy.
	const workers = 24
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			r := rand.New(rand.NewSource(seed))
			seq := 0
			for {
				select {
				case <-stop:
					return
				default:
				}
				switch r.Intn(10) {
				case 0, 1, 2, 3: // Place
					seq++
					id := fmt.Sprintf("vm-%d-%d", seed, seq)
					vcpu := 1 + r.Intn(8)
					mem := 512 * (1 + r.Intn(8))
					host, err := m.Place(id, vcpu, mem)
					if err == nil {
						tr.placed(id, host, vcpu, mem)
					}
				case 4, 5, 6: // Migrate
					id, ok := tr.randomVM(r)
					if !ok {
						continue
					}
					target := hostIDs[r.Intn(len(hostIDs))]
					ctx := context.Background()
					var cancel context.CancelFunc
					if r.Intn(6) == 0 {
						ctx, cancel = context.WithCancel(ctx)
					}
					err := m.Migrate(ctx, id, target, func(context.Context) error {
						if cancel != nil && r.Intn(2) == 0 {
							cancel()
						}
						time.Sleep(time.Duration(r.Intn(5)) * time.Millisecond)
						switch r.Intn(10) {
						case 0, 1, 2:
							return errors.New("copy failed")
						case 3:
							panic("boom")
						}
						return nil
					})
					if cancel != nil {
						cancel()
					}
					if err == nil {
						tr.migrated(id, target)
					}
				default: // Destroy
					id, ok := tr.randomVM(r)
					if !ok {
						continue
					}
					if err := m.Destroy(id); err == nil {
						tr.destroyed(id)
					}
				}
			}
		}(int64(w) + 100)
	}

	time.Sleep(5 * time.Second)
	close(stop)
	wg.Wait()

	if n := observerViolation.Load(); n > 0 {
		t.Fatalf("observer saw %d overcommit violations", n)
	}

	// Final reconciliation: per-host commitments must equal the sums
	// derived from operation results.
	want := tr.sums()
	for _, id := range hostIDs {
		got, err := m.Usage(id)
		if err != nil {
			t.Fatal(err)
		}
		if got != want[id] {
			t.Fatalf("host %s: committed %+v != derived residency %+v", id, got, want[id])
		}
	}
}
