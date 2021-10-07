// Code generated by "stringer -type=State"; DO NOT EDIT.

package fsm

import "strconv"

func _() {
	// An "invalid array index" compiler error signifies that the constant values have changed.
	// Re-run the stringer command to generate them again.
	var x [1]struct{}
	_ = x[Unknown-0]
	_ = x[Pending-1]
	_ = x[Ready-2]
	_ = x[Splitting-3]
	_ = x[Joining-4]
	_ = x[Obsolete-5]
}

const _State_name = "UnknownPendingReadySplittingJoiningObsolete"

var _State_index = [...]uint8{0, 7, 14, 19, 28, 35, 43}

func (i State) String() string {
	if i >= State(len(_State_index)-1) {
		return "State(" + strconv.FormatInt(int64(i), 10) + ")"
	}
	return _State_name[_State_index[i]:_State_index[i+1]]
}
