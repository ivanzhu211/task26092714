package vmplace

import (
	"context"
	"errors"
	"fmt"
)

var (
	ErrVMDestroyed      = errors.New("vmplace: vm destroyed")
	ErrAlreadyMigrating = errors.New("vmplace: vm already migrating")
	ErrHostNotAvailable = errors.New("vmplace: target host not available")
	ErrNoCapacity       = errors.New("vmplace: target host lacks capacity")
)

// runCopy executes copyFn, converting panics and context cancellation
// into errors.
func runCopy(ctx context.Context, copyFn func(context.Context) error) (err error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("vmplace: copy panicked: %v", r)
		}
	}()
	if err = copyFn(ctx); err != nil {
		return err
	}
	if cerr := ctx.Err(); cerr != nil {
		return cerr
	}
	return nil
}

// Migrate reserves capacity for the VM on the target host, then runs
// copyFn. During the copy the VM is committed on both source and target.
// On success the source commitment is released; on any failure (error,
// panic, context cancellation) the target reservation is rolled back.
func (c *Cluster) Migrate(ctx context.Context, vmID, targetID string, copyFn func(context.Context) error) error {
	c.mu.RLock()
	v := c.vms[vmID]
	target := c.hosts[targetID]
	c.mu.RUnlock()
	if v == nil {
		return fmt.Errorf("%w: vm %q", ErrNotFound, vmID)
	}
	if target == nil {
		return fmt.Errorf("%w: host %q", ErrNotFound, targetID)
	}

	v.mu.Lock()
	if v.destroyed {
		v.mu.Unlock()
		return fmt.Errorf("%w: vm %q", ErrVMDestroyed, vmID)
	}
	if v.src == nil {
		v.mu.Unlock()
		return fmt.Errorf("%w: vm %q not placed", ErrNotFound, vmID)
	}
	if v.dst != nil {
		v.mu.Unlock()
		return fmt.Errorf("%w: vm %q", ErrAlreadyMigrating, vmID)
	}
	v.mu.Unlock()

	target.mu.Lock()
	if target.state != HostAvailable {
		target.mu.Unlock()
		return fmt.Errorf("%w: host %q", ErrHostNotAvailable, targetID)
	}
	if !target.fits(v.vcpu, v.mem) {
		target.mu.Unlock()
		return fmt.Errorf("%w: host %q", ErrNoCapacity, targetID)
	}
	target.commit(v.vcpu, v.mem)
	target.mu.Unlock()

	v.mu.Lock()
	if v.destroyed {
		// Destroy ran concurrently and already released the source;
		// it could not see the reservation, so roll it back here.
		v.mu.Unlock()
		target.release(v.vcpu, v.mem)
		return fmt.Errorf("%w: vm %q", ErrVMDestroyed, vmID)
	}
	v.dst = target
	v.mu.Unlock()

	err := runCopy(ctx, copyFn)

	if err == nil {
		v.mu.Lock()
		old := v.src
		if v.dst == target {
			v.src = target
			v.dst = nil
		}
		v.mu.Unlock()
		if old != nil {
			old.release(v.vcpu, v.mem)
		}
		return nil
	}

	v.mu.Lock()
	rollback := v.dst == target
	if rollback {
		v.dst = nil
	}
	v.mu.Unlock()
	if rollback {
		target.release(v.vcpu, v.mem)
	}
	return err
}

// Destroy releases every commitment the VM holds (including an in-flight
// migration reservation). It is safe to run concurrently with Migrate.
func (c *Cluster) Destroy(vmID string) error {
	c.mu.RLock()
	v := c.vms[vmID]
	c.mu.RUnlock()
	if v == nil {
		return fmt.Errorf("%w: vm %q", ErrNotFound, vmID)
	}

	v.mu.Lock()
	if v.destroyed {
		v.mu.Unlock()
		return fmt.Errorf("%w: vm %q", ErrVMDestroyed, vmID)
	}
	v.destroyed = true
	src, dst := v.src, v.dst
	v.src, v.dst = nil, nil
	v.mu.Unlock()

	c.mu.Lock()
	delete(c.vms, vmID)
	c.mu.Unlock()

	if src != nil {
		src.release(v.vcpu, v.mem)
	}
	if dst != nil {
		dst.release(v.vcpu, v.mem)
	}
	return nil
}
