// Code generated by "stringer -type=StateLocal -output=zzz_state_local_string.go"; DO NOT EDIT.

package ranje

import "strconv"

func _() {
	// An "invalid array index" compiler error signifies that the constant values have changed.
	// Re-run the stringer command to generate them again.
	var x [1]struct{}
	_ = x[Unknown-0]
	_ = x[Pending-1]
	_ = x[Placing-2]
	_ = x[Ready-3]
	_ = x[Moving-4]
	_ = x[Splitting-5]
	_ = x[Joining-6]
	_ = x[Obsolete-7]
}

const _StateLocal_name = "UnknownPendingPlacingReadyMovingSplittingJoiningObsolete"

var _StateLocal_index = [...]uint8{0, 7, 14, 21, 26, 32, 41, 48, 56}

func (i StateLocal) String() string {
	if i >= StateLocal(len(_StateLocal_index)-1) {
		return "StateLocal(" + strconv.FormatInt(int64(i), 10) + ")"
	}
	return _StateLocal_name[_StateLocal_index[i]:_StateLocal_index[i+1]]
}
