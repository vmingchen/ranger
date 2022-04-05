package balancer

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/adammck/ranger/pkg/config"
	pb "github.com/adammck/ranger/pkg/proto/gen"
	"github.com/adammck/ranger/pkg/ranje"
	"github.com/adammck/ranger/pkg/roster"
	"google.golang.org/grpc"
)

type Balancer struct {
	cfg  config.Config
	ks   *ranje.Keyspace
	rost *roster.Roster
	srv  *grpc.Server
	bs   *balancerServer
	dbg  *debugServer

	// Funcs to be called at the end of the current tick.
	cb    []func()
	cbMu  sync.RWMutex
	rpcWG sync.WaitGroup
}

func New(cfg config.Config, ks *ranje.Keyspace, rost *roster.Roster, srv *grpc.Server) *Balancer {
	b := &Balancer{
		cfg:  cfg,
		ks:   ks,
		rost: rost,
		srv:  srv,
		cb:   []func(){},
	}

	// Register the gRPC server to receive instructions from operators. This
	// will hopefully not be necessary once balancing actually works!
	b.bs = &balancerServer{bal: b}
	pb.RegisterBalancerServer(srv, b.bs)

	// Register the debug server, to fetch info about the state of the world.
	// One could arguably pluck this straight from Consul -- since it's totally
	// consistent *right?* -- but it's a much richer interface to do it here.
	b.dbg = &debugServer{bal: b}
	pb.RegisterDebugServer(srv, b.dbg)

	return b
}

func (b *Balancer) RangesOnNodesWantingDrain() []*ranje.Range {
	out := []*ranje.Range{}

	// TODO: Have the roster keep a list of nodes wanting drain rather than iterating.
	for _, n := range b.rost.Nodes {
		if n.WantDrain() {
			for _, pbnid := range b.ks.PlacementsByNodeID(n.Ident()) {

				// TODO: If the placement is next (i.e. currently moving onto this node), cancel the op.
				if pbnid.Position == 0 {
					out = append(out, pbnid.Range)
				}
			}
		}
	}

	return out
}

func (b *Balancer) Tick() {
	log.Print("tick")

	rs, unlock := b.ks.Ranges()
	defer unlock()

	for _, r := range rs {
		b.tickRange(r)
	}

	// Now that we're finished advancing the ranges and placements, wait for any
	// RPCs emitted to complete.
	b.rpcWG.Wait()

	b.callbacks()

	// Find any unknown and complain about them. There should be NONE of these
	// in the keyspace; it indicates a state bug.
	// for _, r := range b.ks.RangesByState(ranje.RsUnknown) {
	// 	log.Fatalf("range in unknown state: %v", r)
	// }

	// Find any placements on nodes which are unknown to the roster. This can
	// happen if a node goes away while the controller is down, and probably
	// other state snafus.
	// TODO

	// Find any pending ranges and find any node to assign them to.
	// TODO

	// Find any ranges on nodes wanting drain, and move them.
	// for _, r := range b.RangesOnNodesWantingDrain() {
	// 	b.PerformMove(r)
	// }
}

func (b *Balancer) callbacks() {
	b.cbMu.Lock()
	defer b.cbMu.Unlock()

	for _, f := range b.cb {
		f()
	}

	b.cb = []func(){}
}

func (b *Balancer) Queue(f func()) {
	b.cbMu.Lock()
	defer b.cbMu.Unlock()
	b.cb = append(b.cb, f)
}

func (b *Balancer) tickRange(r *ranje.Range) {
	switch r.State {
	case ranje.RsActive:

		// Not enough placements? Create one!
		if len(r.Placements) < b.cfg.Replication {

			nID, err := b.rost.Candidate(r)
			if err != nil {
				log.Printf("error finding candidate node for %v: %v", r, err)
				return
			}

			p := ranje.NewPlacement(r, nID)
			r.Placements = append(r.Placements, p)

		}

		// Placement wants moving? Create another one to replace it.

		moveInProgress := map[string]struct{}{}
		for _, p := range r.Placements {
			if p.IsReplacing != "" {
				moveInProgress[p.IsReplacing] = struct{}{}
			}
		}

		initMove := []*ranje.Placement{}
		for _, p := range r.Placements {
			if p.WantMove {
				_, ok := moveInProgress[p.NodeID]
				if !ok {
					initMove = append(initMove, p)
				}
			}
		}

		for _, p1 := range initMove {
			// TODO: Deduplicate this with the above.
			nID, err := b.rost.Candidate(r)
			if err != nil {
				log.Printf("error finding candidate node for %v: %v", r, err)
				return
			}

			p2 := ranje.NewPlacement(r, nID)
			p2.IsReplacing = p1.NodeID
			r.Placements = append(r.Placements, p2)

			// Only init one per tick, for now!
			// TODO: Think about this, it's dumb.
			if true {
				break
			}
		}

	default:
		panic(fmt.Sprintf("unknown RangeState value: %s", r.State))
	}

	toDestroy := []int{}

	for i, p := range r.Placements {
		destroy := false
		b.tickPlacement(p, &destroy)
		if destroy {
			toDestroy = append(toDestroy, i)
		}
	}

	for _, idx := range toDestroy {
		r.Placements = append(r.Placements[:idx], r.Placements[idx+1:]...)
	}
}

func (b *Balancer) tickPlacement(p *ranje.Placement, destroy *bool) {

	// Get the node that this placement is on.
	// (This is a problem, in most states.)
	n := b.rost.NodeByIdent(p.NodeID)
	if p.State != ranje.PsGiveUp && p.State != ranje.PsDropped {
		if n == nil {
			// The node has disappeared.
			log.Printf("missing node: %s", n.Ident())
			b.ks.PlacementToState(p, ranje.PsGiveUp)
			return
		}
	}

	switch p.State {
	case ranje.PsPending:
		// If the node already has the range (i.e. this is not the first tick
		// where the placement is PsPending, so the RPC may already have been
		// sent), check its remote state, which may have been updated by a
		// response to a Give or by a periodic probe. We may be able to advance.
		ri, ok := n.Get(p.Range().Meta.Ident)
		if ok {
			switch ri.State {
			case roster.NsPreparing:
				log.Printf("node %s still preparing %s", n.Ident(), p.Range().Meta.Ident)

			case roster.NsPrepared:
				b.ks.PlacementToState(p, ranje.PsPrepared)
				return

			case roster.NsPreparingError:
				// TODO: Pass back more information from the node, here. It's
				//       not an RPC error, but there was some failure which we
				//       can log or handle here.
				log.Printf("error placing %s on %s", p.Range().Meta.Ident, n.Ident())
				b.ks.PlacementToState(p, ranje.PsGiveUp)
				return

			default:
				log.Printf("very unexpected remote state: %s (placement state=%s)", ri.State, p.State)
				b.ks.PlacementToState(p, ranje.PsGiveUp)
				return
			}
		} else {
			log.Printf("will give %s to %s", p.Range().Meta.Ident, n.Ident())
		}

		// Send a Give RPC (maybe not the first time; once per tick).
		// TODO: Keep track of how many times we've tried this and for how long.
		//       We'll want to give up if it takes too long to prepare.
		b.RPC(func() {
			err := n.Give(context.Background(), p)
			if err != nil {
				log.Printf("error giving %v to %s: %v", p.LogString(), n.Ident(), err)
			}
		})

	case ranje.PsPrepared:
		ri, ok := n.Get(p.Range().Meta.Ident)
		if ok {
			switch ri.State {
			case roster.NsPrepared:
				// This is the first time around.
				log.Printf("will instruct %s to serve %s", n.Ident(), p.Range().Meta.Ident)

			case roster.NsReadying:
				// We've already sent the Serve RPC at least once, and the node
				// is working on it. Just keep waiting.
				log.Printf("node %s still readying %s", n.Ident(), p.Range().Meta.Ident)

			case roster.NsReadyingError:
				// TODO: Pass back more information from the node, here. It's
				//       not an RPC error, but there was some failure which we
				//       can log or handle here.l
				log.Printf("error readying %s on %s", p.Range().Meta.Ident, n.Ident())
				b.ks.PlacementToState(p, ranje.PsGiveUp)

			case roster.NsReady:
				b.ks.PlacementToState(p, ranje.PsReady)
				return

			// TODO: roster.NsReadyError?

			default:
				log.Printf("very unexpected remote state: %s (placement state=%s)", ri.State, p.State)
				b.ks.PlacementToState(p, ranje.PsGiveUp)
				return
			}
		}

		// We are ready to move from Prepared to Ready, but may have to wait for
		// the placement that this is replacing (maybe) to relinquish it first.
		if !p.Range().MayBecomeReady(p) {
			log.Printf("not moving to Ready")
			return
		}

		b.RPC(func() {
			err := n.Serve(context.Background(), p)
			if err != nil {
				log.Printf("error serving %v to %s: %v", p.LogString(), n.Ident(), err)
			}
		})

	case ranje.PsReady:
		ri, ok := n.Get(p.Range().Meta.Ident)
		if ok {
			switch ri.State {
			case roster.NsReady:
				log.Printf("ready: %s", p.LogString())

			case roster.NsTaking:
				log.Printf("node %s still taking %s", n.Ident(), p.Range().Meta.Ident)

			case roster.NsTakingError:
				// TODO: Pass back more information from the node, here. It's
				//       not an RPC error, but there was some failure which we
				//       can log or handle here.
				log.Printf("error taking %s from %s", p.Range().Meta.Ident, n.Ident())
				b.ks.PlacementToState(p, ranje.PsGiveUp)
				return

			case roster.NsTaken:
				b.ks.PlacementToState(p, ranje.PsTaken)
				return

			// TODO: roster.NsTakeError?

			default:
				log.Printf("very unexpected remote state: %s (placement state=%s)", ri.State, p.State)
				b.ks.PlacementToState(p, ranje.PsGiveUp)
				return
			}
		}

		if !p.Range().MayBeTaken(p) {
			log.Printf("not moving to Taken")
			return
		}

		b.RPC(func() {
			err := n.Take(context.Background(), p)
			if err != nil {
				log.Printf("error taking %v from %s: %v", p.LogString(), n.Ident(), err)
			}
		})

	case ranje.PsTaken:
		ri, ok := n.Get(p.Range().Meta.Ident)
		if !ok {
			log.Printf("will drop %s from %s", p.Range().Meta.Ident, n.Ident())
		}

		switch ri.State {
		case roster.NsTaken:
			if !p.Range().MayBeDropped(p) {
				log.Printf("not moving to Dropped")
				return
			}

		case roster.NsDropping:
			// We have already decided to drop the range, and have probably sent
			// the RPC (below) already, so cannot turn bac now.
			log.Printf("node %s still dropping %s", n.Ident(), p.Range().Meta.Ident)

		case roster.NsDroppingError:
			// TODO: Pass back more information from the node, here. It's
			//       not an RPC error, but there was some failure which we
			//       can log or handle here.
			log.Printf("error dropping %s from %s", p.Range().Meta.Ident, n.Ident())
			b.ks.PlacementToState(p, ranje.PsGiveUp)
			return

		case roster.NsDropped:
			b.ks.PlacementToState(p, ranje.PsDropped)
			return

		default:
			log.Printf("very unexpected remote state: %s (placement state=%s)", ri.State, p.State)
			b.ks.PlacementToState(p, ranje.PsGiveUp)
			return
		}

		b.RPC(func() {
			err := n.Drop(context.Background(), p)
			if err != nil {
				log.Printf("error dropping %v from %s: %v", p.LogString(), n.Ident(), err)
			}
		})

	case ranje.PsDropped:
		log.Printf("will destroy %s", p.LogString())
		*destroy = true
		return

	default:
		panic(fmt.Sprintf("unhandled PlacementState value: %s", p.State))
	}
}

func (b *Balancer) Run(t *time.Ticker) {
	for ; true; <-t.C {
		b.Tick()
	}
}

func (b *Balancer) RPC(f func()) {
	b.rpcWG.Add(1)

	go func() {
		f()
		b.rpcWG.Done()
	}()
}
