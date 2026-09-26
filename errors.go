package vmplace

import (
	"errors"
	"fmt"
)

var (
	ErrHostNotFound  = errors.New("vmplace: host not found")
	ErrHostExists    = errors.New("vmplace: host already exists")
	ErrVMNotFound    = errors.New("vmplace: vm not found")
	ErrVMExists      = errors.New("vmplace: vm already exists")
	ErrVMMigrating   = errors.New("vmplace: vm is already migrating")
	ErrInvalidSpec   = errors.New("vmplace: vm spec out of range")
	ErrInvalidHost   = errors.New("vmplace: invalid host spec")
	ErrNotAvailable  = errors.New("vmplace: target host is not available")
	ErrNoCapacity    = errors.New("vmplace: target host has no capacity")
	ErrInvalidPct    = errors.New("vmplace: overcommit percentage must be positive")
)

// OvercommitRejected is returned by SetOvercommit when the requested
// limits would drop below what is currently committed.
type OvercommitRejected struct {
	HostID    string
	Committed Usage
}

func (e *OvercommitRejected) Error() string {
	return fmt.Sprintf("vmplace: overcommit on %s rejected: committed %+v exceeds new limits",
		e.HostID, e.Committed)
}

// PlacementFailure is returned by Place when no available host can fit
// the VM. Gaps lists, per available host, the shortfall in each
// dimension (0 means that dimension fits).
type PlacementFailure struct {
	Gaps []HostGap
}

func (e *PlacementFailure) Error() string {
	return fmt.Sprintf("vmplace: no available host can fit the vm (%d hosts reported gaps)", len(e.Gaps))
}
