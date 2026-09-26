package vmplace

import "fmt"

type HostState int

const (
	HostAvailable HostState = iota
	HostMaintenance
	HostOffline
)

type Usage struct {
	VCPU  int
	MemMB int
	VMs   int
}

type Deficit struct {
	HostID string
	VCPU   int
	MemMB  int
	VMs    int
}

type PlaceError struct {
	Deficits []Deficit
}

func (e *PlaceError) Error() string {
	return fmt.Sprintf("vmplace: no host can fit vm (%d host(s) short)", len(e.Deficits))
}

type HostSnapshot struct {
	State     HostState
	Usage     Usage
	VCPULimit int
	MemLimit  int
	VMLimit   int
}
