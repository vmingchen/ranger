package move

import (
	"fmt"
	"log"

	"github.com/adammck/ranger/pkg/operations/utils"
	"github.com/adammck/ranger/pkg/ranje"
	"github.com/adammck/ranger/pkg/roster"
)

type state uint8

const (
	Init state = iota
	Failed
	Complete
	Take
	Give
	Untake
	FetchWait
	Serve
	Drop
)

type MoveOp struct {
	Keyspace *ranje.Keyspace
	Roster   *roster.Roster
	Done     func()
	state    state

	// Inputs
	Range ranje.Ident
	Node  string
}

// This is run synchronously, to determine whether the operation can proceed. If
// so, the rest of the operation is run in a goroutine.
func (op *MoveOp) Init() error {
	var err error

	r, err := op.Keyspace.GetByIdent(op.Range)
	if err != nil {
		return fmt.Errorf("can't initiate move; GetByIdent failed: %v", err)
	}

	// If the range is currently ready, it's placed on some node.
	// TODO: Now that we have MoveOpState, do we even need a special range state
	// to indicates that it's moving? Perhaps we can unify the op states into a
	// single 'some op is happening' state on the range.
	if r.State() == ranje.Ready {

		// TODO: Sanity check here that we're not trying to move the range to
		// the node it's already on. The operation fails gracefully even if we
		// do try to do this, but involves a brief unavailability because it
		// will Take, then try to Give (and fail), then Untake.

		r.MustState(ranje.Moving)
		op.state = Take
		return nil

	} else if r.State() == ranje.Quarantined || r.State() == ranje.Pending {
		// Not ready, but still eligible to be placed. (This isn't necessarily
		// an error state. All ranges are pending when created.)
		r.MustState(ranje.Placing)
		op.state = Give
		return nil

	}

	return fmt.Errorf("can't initiate move of range in state %q", r.State())
}

func (op *MoveOp) Run() {
	s := op.state

	for {
		switch s {
		case Complete, Failed:
			if op.Done != nil {
				op.Done() // TODO: Send an error?
			}
			return

		case Init:
			panic("move operation re-entered init state")

		case Take:
			s = op.take()

		case Give:
			s = op.give()

		case Untake:
			s = op.untake()

		case FetchWait:
			s = op.fetchWait()

		case Serve:
			s = op.serve()

		case Drop:
			s = op.drop()
		}

		log.Printf("Move: %d -> %d", op.state, s)
		op.state = s
	}
}

func (op *MoveOp) take() state {
	r, err := op.Keyspace.GetByIdent(op.Range)
	if err != nil {
		log.Printf("Move (take) failed: %s", err.Error())
		return Failed
	}

	p := r.Placement()
	if p == nil {
		log.Println("Move (take) failed: Placement returned nil")
		return Failed
	}

	err = utils.Take(op.Roster, p)
	if err != nil {
		log.Printf("Move (take) failed: %s", err.Error())
		r.MustState(ranje.Ready) // ???
		return Failed
	}

	return Give
}

func (op *MoveOp) give() state {
	r, err := op.Keyspace.GetByIdent(op.Range)
	if err != nil {
		log.Printf("Move (give) failed: %s", err.Error())
		return Failed
	}

	p, err := ranje.NewPlacement(r, op.Node)
	if err != nil {
		log.Printf("Move (give) failed: %s", err.Error())
		return Failed
	}

	err = utils.Give(op.Roster, r, p)
	if err != nil {
		log.Printf("Move (give) failed: %s", err.Error())

		// Clean up p. No return value.
		r.ClearNextPlacement()

		switch r.State() {
		case ranje.Placing:
			// During initial placement, we can just fail without cleanup. The
			// range is still not assigned. The balancer should retry the
			// placement, perhaps on a different node.
			r.MustState(ranje.PlaceError)

		case ranje.Moving:
			// When moving, we have already taken the range from the src node,
			// but failed to give it to the dest! We must untake it from the
			// src, to avoid failing in a state where nobody has the range.
			return Untake

		default:
			panic(fmt.Sprintf("impossible range state: %s", r.State()))
		}

		return Failed
	}

	// If the placement went straight to Ready, we're done. (This can happen
	// when the range isn't being moved from anywhere, or if the transfer
	// happens very quickly.)
	if p.State() == ranje.SpReady {
		return complete(r)
	}

	return FetchWait
}

func (op *MoveOp) untake() state {
	r, err := op.Keyspace.GetByIdent(op.Range)
	if err != nil {
		log.Printf("Move (untake) failed: %s", err.Error())
		return Failed
	}

	p := r.Placement()
	if p == nil {
		log.Println("Move (untake) failed: Placement returned nil")
		return Failed
	}

	err = utils.Untake(op.Roster, p)
	if err != nil {
		log.Printf("Move (untake) failed: %s", err.Error())
		return Failed // TODO: Try again?!
	}

	// The range is now ready again, because the current placement is ready.
	// (and the next placement is gone.)
	r.MustState(ranje.Ready)

	// Always transition into failed, because even though this step succeeded
	// and service has been restored to src, the move was a failure.
	return Failed
}

func (op *MoveOp) fetchWait() state {
	r, err := op.Keyspace.GetByIdent(op.Range)
	if err != nil {
		log.Printf("Move (fetchWait) failed: %s", err.Error())
		return Failed
	}

	p := r.NextPlacement()
	if p == nil {
		log.Println("Move (fetchWait) failed: NextPlacement is nil")
		return Failed
	}

	err = p.FetchWait()
	if err != nil {
		log.Printf("Move (fetchWait) failed: %s", err.Error())
		return Failed
	}

	return Serve
}

func (op *MoveOp) serve() state {
	r, err := op.Keyspace.GetByIdent(op.Range)
	if err != nil {
		log.Printf("Move (serve) failed: %s", err.Error())
		return Failed
	}

	p := r.NextPlacement()
	if p == nil {
		log.Println("Move (serve) failed: NextPlacement is nil")
		return Failed
	}

	err = utils.Serve(op.Roster, p)
	if err != nil {
		log.Printf("Move (serve) failed: %s", err.Error())
		return Failed
	}

	switch r.State() {
	case ranje.Moving:
		// This is a range move, so even though the next placement is ready to
		// serve, we still have to clean up the current placement. We could mark
		// the range as ready now, to minimize the not-ready window, but a drop
		// operation should be fast, and it would be weird.
		return Drop

	case ranje.Placing:
		// This is an initial placement, so we have no previous node to drop
		// data from. We're done.
		return complete(r)

	default:
		panic(fmt.Sprintf("impossible range state: %s", r.State()))
	}
}

func (op *MoveOp) drop() state {
	r, err := op.Keyspace.GetByIdent(op.Range)
	if err != nil {
		log.Printf("Move (drop) failed: %s", err.Error())
		return Failed
	}

	p := r.Placement()
	if p == nil {
		log.Println("Move (drop) failed: Placement is nil")
		return Failed
	}

	err = utils.Drop(op.Roster, p)
	if err != nil {
		log.Printf("Move (drop) failed: %s", err.Error())
		return Failed
	}

	return complete(r)
}

func complete(r *ranje.Range) state {
	r.CompleteNextPlacement()
	r.MustState(ranje.Ready)
	return Complete
}
