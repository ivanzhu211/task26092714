package vmplace

import "sync"

// Manager owns all hosts and VMs. Its lock only guards the registries
// and is never held while host-level or vm-level work happens, so
// operations on different hosts proceed in parallel.
type Manager struct {
	mu    sync.Mutex
	hosts map[string]*host
	vms   map[string]*vmRec
}

func NewManager() *Manager {
	return &Manager{
		hosts: make(map[string]*host),
		vms:   make(map[string]*vmRec),
	}
}

func (m *Manager) getHost(id string) *host {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.hosts[id]
}

func (m *Manager) getVM(id string) *vmRec {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.vms[id]
}

// AddHost registers a host in Available state with the default
// overcommit ratios (200% CPU, 150% memory).
func (m *Manager) AddHost(id string, cores, memMB int) error {
	if cores <= 0 || memMB <= 0 {
		return ErrInvalidHost
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.hosts[id]; ok {
		return ErrHostExists
	}
	m.hosts[id] = newHost(id, cores, memMB)
	return nil
}

// SetHostState switches the administrative state. Once it returns, the
// host accepts no new commitments or migration reservations: Place and
// Migrate re-check the state under the host lock before committing.
func (m *Manager) SetHostState(id string, state State) error {
	h := m.getHost(id)
	if h == nil {
		return ErrHostNotFound
	}
	h.mu.Lock()
	h.state = state
	h.mu.Unlock()
	return nil
}

// SetOvercommit changes the per-host overcommit percentages. If the new
// limits would fall below the current commitments (including in-flight
// migration reservations), the change is rejected and the current
// committed amounts are returned alongside the error.
func (m *Manager) SetOvercommit(id string, cpuPct, memPct int) (Usage, error) {
	h := m.getHost(id)
	if h == nil {
		return Usage{}, ErrHostNotFound
	}
	if cpuPct <= 0 || memPct <= 0 {
		return Usage{}, ErrInvalidPct
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	newCPULimit := h.cores * cpuPct / 100
	newMemLimit := h.memMB * memPct / 100
	if newCPULimit < h.committedVCPU || newMemLimit < h.committedMemMB {
		return h.usage(), &OvercommitRejected{HostID: id, Committed: h.usage()}
	}
	h.cpuPct = cpuPct
	h.memPct = memPct
	return h.usage(), nil
}

// Usage returns a consistent snapshot of the host's commitments,
// including in-flight migration reservations.
func (m *Manager) Usage(id string) (Usage, error) {
	h := m.getHost(id)
	if h == nil {
		return Usage{}, ErrHostNotFound
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.usage(), nil
}

// Limits returns the host's current committable ceilings.
func (m *Manager) Limits(id string) (Usage, error) {
	h := m.getHost(id)
	if h == nil {
		return Usage{}, ErrHostNotFound
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.limits(), nil
}

// Destroy releases every commitment the VM holds, on its home host and
// on any migration target reservation. It is safe to run concurrently
// with an in-flight Migrate of the same VM: once both return, neither
// host accounts for the VM anymore.
func (m *Manager) Destroy(vmID string) error {
	vm := m.getVM(vmID)
	if vm == nil {
		return ErrVMNotFound
	}
	vm.mu.Lock()
	if vm.destroyed {
		vm.mu.Unlock()
		return ErrVMNotFound
	}
	vm.destroyed = true
	src, dst := vm.src, vm.dst
	vm.src, vm.dst = nil, nil
	vm.migrating = false
	// Lock order is always vm.mu -> host.mu; Place and Migrate take
	// host.mu without vm.mu, so there is no cycle.
	if src != nil {
		src.mu.Lock()
		src.remove(vm.vcpu, vm.memMB)
		src.mu.Unlock()
	}
	if dst != nil {
		dst.mu.Lock()
		dst.remove(vm.vcpu, vm.memMB)
		dst.mu.Unlock()
	}
	vm.mu.Unlock()

	m.mu.Lock()
	delete(m.vms, vmID)
	m.mu.Unlock()
	return nil
}

// Snapshot returns the host's commitments and limits from a single
// critical section, so callers can compare them without racing
// concurrent updates.
func (m *Manager) Snapshot(id string) (committed, limits Usage, err error) {
	h := m.getHost(id)
	if h == nil {
		return Usage{}, Usage{}, ErrHostNotFound
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.usage(), h.limits(), nil
}
