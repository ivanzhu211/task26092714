package vmplace

import (
	"context"
	"fmt"
)

// Migrate moves a VM to the target host. While copyFn runs, the VM is
// accounted on both the source and the target host. On success the
// source commitment is released; if copyFn errors or panics, or ctx is
// cancelled, the target reservation is released instead. Destroy may
// run concurrently: once both return, no host accounts for the VM.
func (m *Manager) Migrate(ctx context.Context, vmID, targetID string, copyFn func(context.Context) error) error {
	vm := m.getVM(vmID)
	if vm == nil {
		return ErrVMNotFound
	}
	th := m.getHost(targetID)
	if th == nil {
		return ErrHostNotFound
	}

	// Reserve on the target. Lock order is always vm.mu -> host.mu.
	vm.mu.Lock()
	if vm.destroyed {
		vm.mu.Unlock()
		return ErrVMNotFound
	}
	if vm.migrating {
		vm.mu.Unlock()
		return ErrVMMigrating
	}
	th.mu.Lock()
	if th.state != Available {
		th.mu.Unlock()
		vm.mu.Unlock()
		return ErrNotAvailable
	}
	if !th.fits(vm.vcpu, vm.memMB) {
		th.mu.Unlock()
		vm.mu.Unlock()
		return ErrNoCapacity
	}
	th.add(vm.vcpu, vm.memMB)
	vm.dst = th
	vm.migrating = true
	th.mu.Unlock()
	vm.mu.Unlock()

	err := runCopy(ctx, copyFn)
	if err == nil && ctx.Err() != nil {
		err = ctx.Err()
	}

	vm.mu.Lock()
	vm.migrating = false
	if err == nil && !vm.destroyed {
		src := vm.src
		if src != nil {
			src.mu.Lock()
			src.remove(vm.vcpu, vm.memMB)
			src.mu.Unlock()
		}
		vm.src = th
		vm.dst = nil
		vm.mu.Unlock()
		return nil
	}
	// Failure, or Destroy intervened (in which case it already released
	// both sides and cleared vm.dst).
	if vm.dst != nil {
		vm.dst.mu.Lock()
		vm.dst.remove(vm.vcpu, vm.memMB)
		vm.dst.mu.Unlock()
		vm.dst = nil
	}
	destroyed := vm.destroyed
	vm.mu.Unlock()
	if err != nil {
		return err
	}
	if destroyed {
		return ErrVMNotFound
	}
	return nil
}

// runCopy invokes copyFn, converting a panic into an error so the
// target reservation is rolled back instead of leaking.
func runCopy(ctx context.Context, copyFn func(context.Context) error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("vmplace: copy panicked: %v", r)
		}
	}()
	return copyFn(ctx)
}
