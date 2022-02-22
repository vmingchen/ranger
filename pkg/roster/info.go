package roster

import (
	"time"

	"github.com/adammck/ranger/pkg/ranje"
)

type NodeInfo struct {
	Time   time.Time
	NodeID string
	Ranges []RangeInfo
}

// RangeInfo represents something we know about a Range on a Node at a moment in
// time. These are emitted and cached by the Roster to anyone who cares.
type RangeInfo struct {
	Meta  ranje.Meta
	State State
	// TODO: LoadInfo goes here!!
}
