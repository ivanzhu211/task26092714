package vmplace

import "sort"

// candidate is a host that could fit the VM at scan time.
type candidate struct {
	h     *host
	score float64
}

// Place picks, among available hosts that fit the VM, the one with the
// highest post-placement max(cpuUtil, memUtil); ties go to the smallest
// host ID. The choice is deterministic under a single thread.
//
// The scan/commit split never blocks other hosts: scores are computed
// under each host's lock, then the commit re-validates under the same
// lock. If every candidate changed underneath us, the scan restarts, so
// a host that stays available with sufficient margin for the whole call
// guarantees the placement succeeds.
func (m *Manager) Place(vmID string, vcpu, memMB int) (string, error) {
	if vcpu < minVCPU || vcpu > maxVCPU || memMB < minMemMB || memMB > maxMemMB {
		return "", ErrInvalidSpec
	}

	vm := &vmRec{id: vmID, vcpu: vcpu, memMB: memMB}
	m.mu.Lock()
	if _, ok := m.vms[vmID]; ok {
		m.mu.Unlock()
		return "", ErrVMExists
	}
	m.vms[vmID] = vm
	m.mu.Unlock()

	hostID, err := m.place(vm)
	if err != nil {
		m.mu.Lock()
		delete(m.vms, vmID)
		m.mu.Unlock()
		return "", err
	}
	return hostID, nil
}

func (m *Manager) place(vm *vmRec) (string, error) {
	for {
		if m.isDestroyed(vm) {
			return "", ErrVMNotFound
		}

		hosts := m.snapshotHosts()
		cands := make([]candidate, 0, len(hosts))
		for _, h := range hosts {
			h.mu.Lock()
			if h.state == Available && h.fits(vm.vcpu, vm.memMB) {
				cpuUtil := float64(h.committedVCPU+vm.vcpu) / float64(h.cpuLimit())
				memUtil := float64(h.committedMemMB+vm.memMB) / float64(h.memLimit())
				cands = append(cands, candidate{h: h, score: max(cpuUtil, memUtil)})
			}
			h.mu.Unlock()
		}

		if len(cands) == 0 {
			return "", &PlacementFailure{Gaps: m.gaps(hosts, vm.vcpu, vm.memMB)}
		}

		// Highest score first; ties broken by smallest host ID.
		sort.Slice(cands, func(i, j int) bool {
			if cands[i].score != cands[j].score {
				return cands[i].score > cands[j].score
			}
			return cands[i].h.id < cands[j].h.id
		})

		for _, c := range cands {
			// Lock order is always vm.mu -> host.mu.
			vm.mu.Lock()
			c.h.mu.Lock()
			if !vm.destroyed && c.h.state == Available && c.h.fits(vm.vcpu, vm.memMB) {
				c.h.add(vm.vcpu, vm.memMB)
				vm.src = c.h
				c.h.mu.Unlock()
				vm.mu.Unlock()
				return c.h.id, nil
			}
			c.h.mu.Unlock()
			vm.mu.Unlock()
		}
		// Every candidate changed under us; rescan.
	}
}

func (m *Manager) isDestroyed(vm *vmRec) bool {
	vm.mu.Lock()
	defer vm.mu.Unlock()
	return vm.destroyed
}

func (m *Manager) snapshotHosts() []*host {
	m.mu.Lock()
	defer m.mu.Unlock()
	hosts := make([]*host, 0, len(m.hosts))
	for _, h := range m.hosts {
		hosts = append(hosts, h)
	}
	return hosts
}

// gaps reports, per currently available host, how much it is short in
// each dimension (0 means that dimension fits).
func (m *Manager) gaps(hosts []*host, vcpu, memMB int) []HostGap {
	var out []HostGap
	for _, h := range hosts {
		h.mu.Lock()
		if h.state != Available {
			h.mu.Unlock()
			continue
		}
		g := HostGap{HostID: h.id}
		if short := vcpu - (h.cpuLimit() - h.committedVCPU); short > 0 {
			g.VCPU = short
		}
		if short := memMB - (h.memLimit() - h.committedMemMB); short > 0 {
			g.MemMB = short
		}
		if h.vmCount+1 > maxVMsPerHost {
			g.VMs = h.vmCount + 1 - maxVMsPerHost
		}
		h.mu.Unlock()
		out = append(out, g)
	}
	return out
}
