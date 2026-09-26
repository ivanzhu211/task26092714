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

// Test 1: while a migration reservation is held on the target and copy
// has not returned, lowering the target's overcommit below the
// reservation-inclusive commitment must be rejected.
func TestSetOvercommitRejectedDuringMigration(t *testing.T) {
	m := NewManager()
	if err := m.AddHost("h1", 64, 64*1024); err != nil {
		t.Fatal(err)
	}
	if err := m.AddHost("h2", 4, 4096); err != nil {
		t.Fatal(err)
	}
	// h2 limits: 8 vCPU / 6144 MB. vm fits only via h2 after h1 is full?
	// Simpler: make h1 unavailable so the VM lands on h2... we want the
	// VM to start on h1 and migrate to h2, so place while h2 is offline.
	if err := m.SetHostState("h2", Maintenance); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Place("vm1", 4, 2048); err != nil {
		t.Fatal(err)
	}
	if err := m.SetHostState("h2", Available); err != nil {
		t.Fatal(err)
	}

	copyStarted := make(chan struct{})
	releaseCopy := make(chan struct{})
	migrateDone := make(chan error, 1)
	go func() {
		migrateDone <- m.Migrate(context.Background(), "vm1", "h2", func(context.Context) error {
			close(copyStarted)
			<-releaseCopy
			return nil
		})
	}()

	<-copyStarted
	// Reservation must be visible on h2 while h1 still accounts for it.
	waitFor(t, "reservation on h2", func() bool {
		u, _ := m.Usage("h2")
		return u.VMs == 1 && u.VCPU == 4 && u.MemMB == 2048
	})
	waitFor(t, "source still committed on h1", func() bool {
		u, _ := m.Usage("h1")
		return u.VMs == 1
	})

	// New limits 2 vCPU / 1024 MB are below the committed 4 / 2048.
	committed, err := m.SetOvercommit("h2", 50, 25)
	if err == nil {
		t.Fatal("expected SetOvercommit to be rejected")
	}
	var rej *OvercommitRejected
	if !errors.As(err, &rej) {
		t.Fatalf("expected OvercommitRejected, got %T: %v", err, err)
	}
	want := Usage{VCPU: 4, MemMB: 2048, VMs: 1}
	if rej.Committed != want || committed != want {
		t.Fatalf("rejection must report committed %+v, got %+v / %+v", want, committed, rej.Committed)
	}

	close(releaseCopy)
	if err := <-migrateDone; err != nil {
		t.Fatalf("migrate: %v", err)
	}
	u1, _ := m.Usage("h1")
	u2, _ := m.Usage("h2")
	if u1 != (Usage{}) {
		t.Fatalf("h1 should be empty, got %+v", u1)
	}
	if u2 != want {
		t.Fatalf("h2 should hold the vm %+v, got %+v", want, u2)
	}
}

// Test 2: Destroy while copy is in flight; copy then succeeds. Neither
// host may keep the VM's commitment.
func TestDestroyDuringMigrationCopy(t *testing.T) {
	m := NewManager()
	if err := m.AddHost("h1", 64, 64*1024); err != nil {
		t.Fatal(err)
	}
	if err := m.AddHost("h2", 64, 64*1024); err != nil {
		t.Fatal(err)
	}
	if err := m.SetHostState("h2", Maintenance); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Place("vm1", 8, 4096); err != nil {
		t.Fatal(err)
	}
	if err := m.SetHostState("h2", Available); err != nil {
		t.Fatal(err)
	}

	copyStarted := make(chan struct{})
	releaseCopy := make(chan struct{})
	migrateDone := make(chan error, 1)
	go func() {
		migrateDone <- m.Migrate(context.Background(), "vm1", "h2", func(context.Context) error {
			close(copyStarted)
			<-releaseCopy
			return nil // copy succeeds after Destroy
		})
	}()

	<-copyStarted
	waitFor(t, "reservation on h2", func() bool {
		u, _ := m.Usage("h2")
		return u.VMs == 1
	})

	if err := m.Destroy("vm1"); err != nil {
		t.Fatalf("destroy during copy: %v", err)
	}
	close(releaseCopy)

	if err := <-migrateDone; !errors.Is(err, ErrVMNotFound) {
		t.Fatalf("migrate after destroy should fail with ErrVMNotFound, got %v", err)
	}
	for _, h := range []string{"h1", "h2"} {
		u, _ := m.Usage(h)
		if u != (Usage{}) {
			t.Fatalf("%s must not retain the vm, got %+v", h, u)
		}
	}
}
