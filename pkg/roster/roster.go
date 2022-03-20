package roster

import (
	"context"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	"github.com/adammck/ranger/pkg/config"
	"github.com/adammck/ranger/pkg/discovery"
	pb "github.com/adammck/ranger/pkg/proto/gen"
	"github.com/adammck/ranger/pkg/ranje"
)

const (
	probeTimeout = 3 * time.Second
)

type Roster struct {
	cfg config.Config

	// Public so Balancer can read the Nodes
	// node ident (not ranje.Ident!!) -> Node
	Nodes map[string]*Node
	sync.RWMutex

	disc discovery.Discoverable

	// Callbacks
	add    func(rem *discovery.Remote)
	remove func(rem *discovery.Remote)

	// info receives NodeInfo updated whenever we receive a probe response from
	// a node, or when we expire a node.
	info chan NodeInfo
}

func New(cfg config.Config, disc discovery.Discoverable, add, remove func(rem *discovery.Remote), info chan NodeInfo) *Roster {
	return &Roster{
		cfg:    cfg,
		Nodes:  make(map[string]*Node),
		disc:   disc,
		add:    add,
		remove: remove,
		info:   info, // currently never closed
	}
}

//func (ros *Roster) NodeBy(opts... NodeByOpts)
// TODO: Return an error from this func, to avoid duplicating it in callers.
func (ros *Roster) NodeByIdent(nodeIdent string) *Node {
	ros.RLock()
	defer ros.RUnlock()

	for nid, n := range ros.Nodes {
		if nid == nodeIdent {
			return n
		}
	}

	return nil
}

// NodeExists returns true if a node with the given ident exists. This is just
// to satisfy the NodeChecker interface.
func (ros *Roster) NodeExists(nodeIdent string) bool {
	return ros.NodeByIdent(nodeIdent) != nil
}

// Locate returns the list of node IDs that the given key can be found on, in any state.
// TODO: Allow the Map to be filtered by state.
func (ros *Roster) Locate(k ranje.Key) []string {
	nodes := []string{}

	ros.RLock()
	defer ros.RUnlock()

	// look i'm in a hurry here okay
	for nid, n := range ros.Nodes {
		func() {
			n.muRanges.RLock()
			defer n.muRanges.RUnlock()

			for _, info := range n.ranges {
				if info.Meta.Contains(k) {
					nodes = append(nodes, nid)
				}
			}
		}()
	}

	return nodes
}

// Caller must hold ros.RWMutex
func (ros *Roster) discover() {
	res, err := ros.disc.Get("node")
	if err != nil {
		panic(err)
	}

	for _, r := range res {
		n, ok := ros.Nodes[r.Ident]

		// New Node?
		if !ok {
			n = NewNode(r)
			ros.Nodes[r.Ident] = n
			log.Printf("added node: %v", n.Ident())

			// TODO: Should we also send a blank NodeInfo to introduce the node?
			//       We haven't probed it yet, so don't know what's assigned.

			// TODO: Do this outside of the lock!!
			if ros.add != nil {
				ros.add(&n.Remote)
			}
		}

		n.whenLastSeen = time.Now()
	}
}

// Caller must hold ros.RWMutex
func (ros *Roster) expire() {
	now := time.Now()

	for nID, n := range ros.Nodes {
		if n.IsMissing(ros.cfg, now) {
			// The node hasn't responded to probes in a while.
			log.Printf("expired node: %v", n.Ident())

			// Send a special loadinfo to the reconciler, to tell it that we've
			// lost the node.
			if ros.info != nil {
				ros.info <- NodeInfo{
					Time:    time.Now(),
					NodeID:  n.Ident(),
					Expired: true,
				}
			}

			// TODO: Get rid of these callbacks. Just use the channel.
			if ros.remove != nil {
				ros.remove(&n.Remote)
			}

			if n.IsGoneFromServiceDiscovery(ros.cfg, now) {
				// The node has also been missing from service discovery for a
				// while, so we can forget about it and stop probing.
				log.Printf("removing node: %v", n.Ident())
				delete(ros.Nodes, nID)
				continue
			}
		}
	}
}

func (ros *Roster) probe() {
	var wg sync.WaitGroup

	ctx, cancel := context.WithTimeout(context.Background(), probeTimeout)
	defer cancel()

	// TODO: We already do this in expire, bundle them together.
	for _, node := range ros.Nodes {
		wg.Add(1)

		// Copy node pointer since it changes between iterations.
		// https://golang.org/doc/faq#closures_and_goroutines
		go func(n *Node) {
			defer wg.Done()
			err := ros.probeOne(ctx, n)

			// TODO: Special case? Nodes finished draining can probably just go
			//       away right away rather than logging errors like this.

			if err != nil {
				log.Printf("error probing %v: %v", n.Ident(), err)
				return
			}
		}(node)
	}

	wg.Wait()
}

// probeOne sends an RPC to fetch the current ranges for one node.
// Returns error if the RPC fails or if a probe is already in progess.
func (ros *Roster) probeOne(ctx context.Context, n *Node) error {
	// TODO: Abort if probe in progress.

	ranges := make(map[ranje.Ident]RangeInfo)

	// TODO: This is an InfoRequest now, but we also have RangesRequest which is
	// sufficient for the proxy. Maybe make which one is sent configurable?

	res, err := n.Client.Info(ctx, &pb.InfoRequest{})
	if err != nil {
		return err
	}

	ni := NodeInfo{
		Time:   time.Now(),
		NodeID: n.Ident(),
	}

	for _, r := range res.Ranges {
		if r.Meta == nil {
			log.Printf("malformed probe response from %v: Meta is nil", n.Remote.Ident)
			continue
		}

		m, err := ranje.MetaFromProto(r.Meta)
		if r.Meta == nil {
			log.Printf("malformed probe response from %v: %v", n.Remote.Ident, err)
			continue
		}

		rID := m.Ident

		// TODO: Update the map rather than overwriting it every time.
		info := RangeInfo{
			Meta:  *m,
			State: RemoteStateFromProto(r.State),
			// TODO: LoadInfo
		}

		ni.Ranges = append(ni.Ranges, info)
		ranges[rID] = info
	}

	// TODO: Should this (nil info) even be allowed?
	if ros.info != nil {
		ros.info <- ni
	}

	// TODO: Do we need a range-changed callback?

	n.muRanges.Lock()
	n.wantDrain = res.WantDrain
	n.ranges = ranges
	n.muRanges.Unlock()

	n.whenLastProbed = time.Now()

	return nil
}

func (r *Roster) Tick() {
	// TODO: anything but this
	r.Lock()
	defer r.Unlock()

	// Grab any new nodes from service discovery.
	r.discover()

	r.probe()

	// Expire any nodes that we haven't been able to probe in a while.
	r.expire()
}

// TODO: Need some way to gracefully stop! Have to close the info channel to stop the reconciler.
func (r *Roster) Run(t *time.Ticker) {
	for ; true; <-t.C {
		r.Tick()
	}
}

// Candidate returns the NodeIdent of a node which could accept the given range.
func (r *Roster) Candidate(rng *ranje.Range) (string, error) {
	r.RLock()
	defer r.RUnlock()

	// Build a list of nodes.
	// TODO: Just store them this way!

	nodes := make([]*Node, len(r.Nodes))
	i := 0

	for _, nod := range r.Nodes {
		nodes[i] = nod
		i += 1
	}

	// Build a list of indices of candidate nodes.
	candidates := []int{}

	// Exclude a node if:
	//
	// 1. It's drained, i.e. it doesn't want any more ranges. It's probably
	//    shutting down.
	//
	// 2. It's missing, i.e. hasn't responded to our probes in a while. It might
	//    still come back, but let's avoid it anyway.
	//
	for i := range nodes {
		if nodes[i].WantDrain() || nodes[i].IsMissing(r.cfg, time.Now()) {
			continue
		}

		candidates = append(candidates, i)
	}

	if len(candidates) == 0 {
		return "", fmt.Errorf("no non-excluded nodes available for range: %v", rng)
	}

	// Pick the node with lowest utilization.
	// TODO: This doesn't take into account ranges which are on the way to that
	//       node, and is generally totally insufficient.

	sort.Slice(candidates, func(i, j int) bool {
		return nodes[i].Utilization() < nodes[j].Utilization()
	})

	return nodes[candidates[0]].Ident(), nil
}
