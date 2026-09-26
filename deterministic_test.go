package vmplace

import (
	"context"
	"errors"
	"testing"
	"time"
)

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestPlaceBestFit(t *testing.T) {
	c := New()
	if err := c.AddHost("h1", 4, 4096); err != nil {
		t.Fatal(err)
	}
	if err := c.AddHost("h2", 64, 65536); err != nil {
		t.Fatal(err)
	}
	// h1 is fuller after placement (cpu 2/8 vs 2/128), so it wins.
	got, err := c.Place("vm1", 2, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if got != "h1" {
		t.Fatalf("expected h1, got %s", got)
	}
	// Tie between identical hosts goes to the smaller id. vm2 does not
	// fit on h1 (2+7 > 8 vCPU limit), so only a1/a2 compete.
	if err := c.AddHost("a2", 8, 8192); err != nil {
		t.Fatal(err)
	}
	if err := c.AddHost("a1", 8, 8192); err != nil {
		t.Fatal(err)
	}
	got, err = c.Place("vm2", 7, 512)
	if err != nil {
		t.Fatal(err)
	}
	if got != "a1" {
		t.Fatalf("expected a1, got %s", got)
	}
}

// Test 1: while a migration reservation is in effect (copy not yet
// returned), lowering the target's overcommit below the committed
// usage (including the reservation) must be rejected.
func TestOvercommitRejectedDuringMigration(t *testing.T) {
	c := New()
	if err := c.AddHost("a", 64, 65536); err != nil {
		t.Fatal(err)
	}
	if err := c.AddHost("b", 8, 8192); err != nil {
		t.Fatal(err)
	}
	if err := c.SetHostState("b", HostMaintenance); err != nil {
		t.Fatal(err)
	}
	got, err := c.Place("vm1", 4, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if got != "a" {
		t.Fatalf("expected vm1 on a, got %s", got)
	}
	if err := c.SetHostState("b", HostAvailable); err != nil {
		t.Fatal(err)
	}

	release := make(chan struct{})
	migDone := make(chan error, 1)
	go func() {
		migDone <- c.Migrate(context.Background(), "vm1", "b",
			func(ctx context.Context) error {
				<-release
				return nil
			})
	}()

	waitFor(t, "reservation on b", func() bool {
		s, err := c.Inspect("b")
		return err == nil && s.Usage.VMs == 1
	})

	// b now commits 4 vCPU / 1024 MB. New limits 8*25/100=2 vCPU and
	// 8192*10/100=819 MB are both below that: must be rejected.
	u, err := c.SetOvercommit("b", 25, 10)
	if !errors.Is(err, ErrOvercommitTooLow) {
		t.Fatalf("expected ErrOvercommitTooLow, got %v", err)
	}
	want := Usage{VCPU: 4, MemMB: 1024, VMs: 1}
	if u != want {
		t.Fatalf("expected committed %v, got %v", want, u)
	}
	// Limits must be unchanged.
	s, err := c.Inspect("b")
	if err != nil {
		t.Fatal(err)
	}
	if s.VCPULimit != 16 || s.MemLimit != 12288 {
		t.Fatalf("limits changed after rejection: %+v", s)
	}

	close(release)
	if err := <-migDone; err != nil {
		t.Fatalf("migrate failed: %v", err)
	}
	sa, _ := c.Inspect("a")
	sb, _ := c.Inspect("b")
	if sa.Usage != (Usage{}) {
		t.Fatalf("source not released: %+v", sa.Usage)
	}
	if sb.Usage != want {
		t.Fatalf("target usage = %+v, want %+v", sb.Usage, want)
	}
}

// Test 2: Destroy while copy is in flight; copy then succeeds. After
// both return, neither host may retain the VM's commitment.
func TestDestroyDuringCopy(t *testing.T) {
	c := New()
	if err := c.AddHost("a", 64, 65536); err != nil {
		t.Fatal(err)
	}
	if err := c.AddHost("b", 64, 65536); err != nil {
		t.Fatal(err)
	}
	if err := c.SetHostState("b", HostMaintenance); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Place("vm1", 4, 2048); err != nil {
		t.Fatal(err)
	}
	if err := c.SetHostState("b", HostAvailable); err != nil {
		t.Fatal(err)
	}

	release := make(chan struct{})
	migDone := make(chan error, 1)
	go func() {
		migDone <- c.Migrate(context.Background(), "vm1", "b",
			func(ctx context.Context) error {
				<-release
				return nil
			})
	}()

	waitFor(t, "reservation on b", func() bool {
		s, err := c.Inspect("b")
		return err == nil && s.Usage.VMs == 1
	})

	if err := c.Destroy("vm1"); err != nil {
		t.Fatalf("destroy failed: %v", err)
	}
	close(release)
	if err := <-migDone; err != nil {
		t.Fatalf("migrate returned: %v", err)
	}

	for _, id := range []string{"a", "b"} {
		s, err := c.Inspect(id)
		if err != nil {
			t.Fatal(err)
		}
		if s.Usage != (Usage{}) {
			t.Fatalf("host %s still commits %+v", id, s.Usage)
		}
	}
}
