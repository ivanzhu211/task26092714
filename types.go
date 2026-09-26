// Package vmplace implements an in-process VM placement and capacity
// accounting library. All state lives in memory.
package vmplace

import "sync"

// State is the administrative state of a host.
type State int

const (
	Available State = iota
	Maintenance
	Offline
)

const (
	defaultCPUPct = 200
	defaultMemPct = 150
	maxVMsPerHost = 200

	minVCPU  = 1
	maxVCPU  = 128
	minMemMB = 512
	maxMemMB = 2 * 1024 * 1024 // 2 TB
)

// Usage is a consistent snapshot of committed resources on a host.
// It is also used to report capacity limits.
type Usage struct {
	VCPU  int
	MemMB int
	VMs   int
}

// HostGap describes how much a single available host is short when a
// placement cannot be satisfied.
type HostGap struct {
	HostID string
	VCPU   int
	MemMB  int
	VMs    int
}

// host is a single physical host with its own lock. Operations touching
// only this host proceed in parallel with operations on other hosts.
type host struct {
	mu     sync.Mutex
	id     string
	cores  int
	memMB  int
	state  State
	cpuPct int
	memPct int

	committedVCPU  int
	committedMemMB int
	vmCount        int
}

// vmRec tracks one VM and which host(s) currently account for it.
// During migration dst is non-nil and the VM is counted on both hosts.
type vmRec struct {
	mu        sync.Mutex
	id        string
	vcpu      int
	memMB     int
	src       *host
	dst       *host
	migrating bool
	destroyed bool
}

func newHost(id string, cores, memMB int) *host {
	return &host{
		id:     id,
		cores:  cores,
		memMB:  memMB,
		state:  Available,
		cpuPct: defaultCPUPct,
		memPct: defaultMemPct,
	}
}

// cpuLimit is the committable vCPU ceiling: floor(cores * cpuPct / 100).
// Callers must hold h.mu.
func (h *host) cpuLimit() int { return h.cores * h.cpuPct / 100 }

// memLimit is the committable memory ceiling in MB.
// Callers must hold h.mu.
func (h *host) memLimit() int { return h.memMB * h.memPct / 100 }

// fits reports whether vcpu/memMB plus one more VM stay within limits.
// Callers must hold h.mu.
func (h *host) fits(vcpu, memMB int) bool {
	return h.committedVCPU+vcpu <= h.cpuLimit() &&
		h.committedMemMB+memMB <= h.memLimit() &&
		h.vmCount+1 <= maxVMsPerHost
}

// add commits the resources. Callers must hold h.mu and have checked fits.
func (h *host) add(vcpu, memMB int) {
	h.committedVCPU += vcpu
	h.committedMemMB += memMB
	h.vmCount++
}

// remove releases the resources. Callers must hold h.mu.
func (h *host) remove(vcpu, memMB int) {
	h.committedVCPU -= vcpu
	h.committedMemMB -= memMB
	h.vmCount--
}

// usage snapshots the committed amounts. Callers must hold h.mu.
func (h *host) usage() Usage {
	return Usage{VCPU: h.committedVCPU, MemMB: h.committedMemMB, VMs: h.vmCount}
}

// limits snapshots the current committable ceilings. Callers must hold h.mu.
func (h *host) limits() Usage {
	return Usage{VCPU: h.cpuLimit(), MemMB: h.memLimit(), VMs: maxVMsPerHost}
}
