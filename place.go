package vmplace

import (
	"fmt"
	"sort"
)

const (
	minVCPU = 1
	maxVCPU = 128
	minMem  = 512
	maxMem  = 2 * 1024 * 1024 // 2 TB in MB
)

// frac is a rational number used to compare utilization scores exactly.
type frac struct {
	num int64
	den int64
}

func maxFrac(a, b frac) frac {
	if a.num*b.den >= b.num*a.den {
		return a
	}
	return b
}

// less reports whether a < b; ties are broken by smaller host id.
func lessScore(a frac, aID string, b frac, bID string) bool {
	lhs := a.num * b.den
	rhs := b.num * a.den
	if lhs != rhs {
		return lhs < rhs
	}
	return aID > bID // a is "less preferred" when its id is larger
}

type candidate struct {
	h     *host
	score frac
}

// Place picks the available host that, after placing the VM, has the
// highest max(cpuUtil, memUtil); ties go to the smallest host id.
// On failure it returns a *PlaceError with per-host deficits.
func (c *Cluster) Place(vmID string, vcpu, memMB int) (string, error) {
	if vcpu < minVCPU || vcpu > maxVCPU || memMB < minMem || memMB > maxMem {
		return "", fmt.Errorf("%w: vcpu must be 1..128, memMB must be 512..2097152", ErrInvalidSpec)
	}

	c.mu.Lock()
	if _, ok := c.vms[vmID]; ok {
		c.mu.Unlock()
		return "", fmt.Errorf("%w: vm %q", ErrVMExists, vmID)
	}
	v := &vm{id: vmID, vcpu: vcpu, mem: memMB}
	c.vms[vmID] = v
	hosts := make([]*host, 0, len(c.hosts))
	for _, h := range c.hosts {
		hosts = append(hosts, h)
	}
	c.mu.Unlock()

	sort.Slice(hosts, func(i, j int) bool { return hosts[i].id < hosts[j].id })

	for {
		var cands []candidate
		var deficits []Deficit
		for _, h := range hosts {
			h.mu.Lock()
			if h.state != HostAvailable {
				h.mu.Unlock()
				continue
			}
			if h.fits(vcpu, memMB) {
				cpu := frac{int64(h.usedVCPU + vcpu), int64(h.cpuLimit())}
				mem := frac{int64(h.usedMem + memMB), int64(h.memLimit())}
				cands = append(cands, candidate{h: h, score: maxFrac(cpu, mem)})
			} else {
				d := Deficit{HostID: h.id}
				if s := h.usedVCPU + vcpu - h.cpuLimit(); s > 0 {
					d.VCPU = s
				}
				if s := h.usedMem + memMB - h.memLimit(); s > 0 {
					d.MemMB = s
				}
				if s := h.usedVMs + 1 - maxVMsPerHost; s > 0 {
					d.VMs = s
				}
				deficits = append(deficits, d)
			}
			h.mu.Unlock()
		}

		if len(cands) == 0 {
			c.mu.Lock()
			delete(c.vms, vmID)
			c.mu.Unlock()
			return "", &PlaceError{Deficits: deficits}
		}

		// Best score first; ties broken by smaller host id.
		sort.Slice(cands, func(i, j int) bool {
			return lessScore(cands[j].score, cands[j].h.id, cands[i].score, cands[i].h.id)
		})

		placed := ""
		for _, cand := range cands {
			h := cand.h
			v.mu.Lock()
			if v.destroyed {
				v.mu.Unlock()
				break
			}
			h.mu.Lock()
			if h.state == HostAvailable && h.fits(vcpu, memMB) {
				h.commit(vcpu, memMB)
				v.src = h
				placed = h.id
			}
			h.mu.Unlock()
			v.mu.Unlock()
			if placed != "" {
				break
			}
		}
		if placed != "" {
			return placed, nil
		}

		v.mu.Lock()
		destroyed := v.destroyed
		v.mu.Unlock()
		if destroyed {
			c.mu.Lock()
			delete(c.vms, vmID)
			c.mu.Unlock()
			return "", fmt.Errorf("%w: vm %q destroyed during placement", ErrNotFound, vmID)
		}
		// All candidates lost the race; rescan.
	}
}
