package vmplace

import (
	"errors"
	"fmt"
	"sync"
)

var (
	ErrHostExists       = errors.New("vmplace: host already exists")
	ErrVMExists         = errors.New("vmplace: vm already exists")
	ErrNotFound         = errors.New("vmplace: not found")
	ErrInvalidSpec      = errors.New("vmplace: invalid spec")
	ErrOvercommitTooLow = errors.New("vmplace: overcommit below current committed usage")
)

const maxVMsPerHost = 200

type host struct {
	mu       sync.Mutex
	id       string
	cores    int
	memMB    int
	state    HostState
	cpuPct   int
	memPct   int
	usedVCPU int
	usedMem  int
	usedVMs  int
}

func (h *host) cpuLimit() int { return h.cores * h.cpuPct / 100 }
func (h *host) memLimit() int { return h.memMB * h.memPct / 100 }

// fits reports whether adding the given spec stays within limits.
// Caller must hold h.mu.
func (h *host) fits(vcpu, mem int) bool {
	return h.usedVCPU+vcpu <= h.cpuLimit() &&
		h.usedMem+mem <= h.memLimit() &&
		h.usedVMs+1 <= maxVMsPerHost
}

// Caller must hold h.mu.
func (h *host) commit(vcpu, mem int) {
	h.usedVCPU += vcpu
	h.usedMem += mem
	h.usedVMs++
}

func (h *host) release(vcpu, mem int) {
	h.mu.Lock()
	h.usedVCPU -= vcpu
	h.usedMem -= mem
	h.usedVMs--
	h.mu.Unlock()
}

type vm struct {
	mu        sync.Mutex
	id        string
	vcpu      int
	mem       int
	src       *host
	dst       *host
	destroyed bool
}

type Cluster struct {
	mu    sync.RWMutex
	hosts map[string]*host
	vms   map[string]*vm
}

func New() *Cluster {
	return &Cluster{
		hosts: make(map[string]*host),
		vms:   make(map[string]*vm),
	}
}

func (c *Cluster) AddHost(id string, cores, memMB int) error {
	if cores <= 0 || memMB <= 0 {
		return fmt.Errorf("%w: cores and memMB must be positive", ErrInvalidSpec)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.hosts[id]; ok {
		return ErrHostExists
	}
	c.hosts[id] = &host{
		id:     id,
		cores:  cores,
		memMB:  memMB,
		state:  HostAvailable,
		cpuPct: 200,
		memPct: 150,
	}
	return nil
}

func (c *Cluster) getHost(id string) *host {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.hosts[id]
}

func (c *Cluster) SetHostState(id string, state HostState) error {
	h := c.getHost(id)
	if h == nil {
		return fmt.Errorf("%w: host %q", ErrNotFound, id)
	}
	h.mu.Lock()
	h.state = state
	h.mu.Unlock()
	return nil
}

// SetOvercommit updates the host's overcommit ratios. If the new limits
// would fall below the currently committed usage, it is rejected and the
// current committed usage is returned along with ErrOvercommitTooLow.
func (c *Cluster) SetOvercommit(id string, cpuPct, memPct int) (Usage, error) {
	h := c.getHost(id)
	if h == nil {
		return Usage{}, fmt.Errorf("%w: host %q", ErrNotFound, id)
	}
	if cpuPct <= 0 || memPct <= 0 {
		return Usage{}, fmt.Errorf("%w: percentages must be positive", ErrInvalidSpec)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	cur := Usage{VCPU: h.usedVCPU, MemMB: h.usedMem, VMs: h.usedVMs}
	if h.cores*cpuPct/100 < h.usedVCPU || h.memMB*memPct/100 < h.usedMem {
		return cur, ErrOvercommitTooLow
	}
	h.cpuPct = cpuPct
	h.memPct = memPct
	return cur, nil
}

func (c *Cluster) Usage(id string) (Usage, error) {
	h := c.getHost(id)
	if h == nil {
		return Usage{}, fmt.Errorf("%w: host %q", ErrNotFound, id)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return Usage{VCPU: h.usedVCPU, MemMB: h.usedMem, VMs: h.usedVMs}, nil
}

// Inspect returns an atomic snapshot of the host's state, committed usage
// and current limits.
func (c *Cluster) Inspect(id string) (HostSnapshot, error) {
	h := c.getHost(id)
	if h == nil {
		return HostSnapshot{}, fmt.Errorf("%w: host %q", ErrNotFound, id)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return HostSnapshot{
		State:     h.state,
		Usage:     Usage{VCPU: h.usedVCPU, MemMB: h.usedMem, VMs: h.usedVMs},
		VCPULimit: h.cpuLimit(),
		MemLimit:  h.memLimit(),
		VMLimit:   maxVMsPerHost,
	}, nil
}
