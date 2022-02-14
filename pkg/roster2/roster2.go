package roster2

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/adammck/ranger/pkg/discovery"
	pb "github.com/adammck/ranger/pkg/proto/gen"
	"github.com/adammck/ranger/pkg/ranje"
)

type Roster2 struct {
	// ident -> ranges
	// that's Node ident, not Range Ident!
	Map map[string]*ShortNode
	sync.RWMutex

	disc discovery.Discoverable

	// Callbacks
	add    func(rem *discovery.Remote)
	remove func(rem *discovery.Remote)
}

const (
	probeTimeout = 1 * time.Second
)

func New(disc discovery.Discoverable, add, remove func(rem *discovery.Remote)) *Roster2 {
	return &Roster2{
		Map:    make(map[string]*ShortNode),
		disc:   disc,
		add:    add,
		remove: remove,
	}
}

// Locate returns the list of node IDs that the given key can be found on, in any state.
// TODO: Allow the Map to be filtered by state.
func (ros *Roster2) Locate(k ranje.Key) []string {
	nodes := []string{}

	ros.RLock()
	defer ros.RUnlock()

	// look i'm in a hurry here okay
	for nid, n := range ros.Map {
		func() {
			n.muRanges.RLock()
			defer n.muRanges.RUnlock()

			for m := range n.ranges {
				if m.Contains(k) {
					nodes = append(nodes, nid)
				}
			}
		}()
	}

	return nodes
}

// Caller must hold ros.RWMutex
func (ros *Roster2) discover() {
	res, err := ros.disc.Get("node")
	if err != nil {
		panic(err)
	}

	for _, rem := range res {
		n, ok := ros.Map[rem.Ident]

		// New Node?
		if !ok {
			n = NewShortNode(rem)
			log.Printf("new node: %s", rem.Ident)
			ros.Map[rem.Ident] = n

			// TODO: Do this outside of the lock!!
			if ros.add != nil {
				ros.add(&n.remote)
			}
		}

		n.Seen(time.Now())
	}
}

// Caller must hold ros.RWMutex
func (ros *Roster2) expire() {
	now := time.Now()

	for nid, n := range ros.Map {
		if n.IsStale(now) {
			delete(ros.Map, nid)
			log.Printf("expired node: %v", nid)

			// TODO: Do this outside of the lock!!
			if ros.remove != nil {
				ros.remove(&n.remote)
			}
		}
	}
}

// probeOne sends an RPC to fetch the current ranges for one node.
// Returns error if the RPC fails or if a probe is already in progess.
func probeOne(ctx context.Context, n *ShortNode) error {
	ranges := make(map[ranje.Meta]ranje.StateRemote)

	res, err := n.client.Ranges(ctx, &pb.RangesRequest{})
	if err != nil {
		log.Printf("probe failed: %s", err)
		return err
	}

	for _, r := range res.Ranges {
		if r.Meta == nil {
			log.Printf("malformed probe response from %v: Meta is nil", n.remote.Ident)
			continue
		}

		m, err := ranje.MetaFromProto(r.Meta)
		if r.Meta == nil {
			log.Printf("malformed probe response from %v: %v", n.remote.Ident, err)
			continue
		}

		// TODO: Update the map rather than overwriting it every time.
		ranges[*m] = ranje.RemoteStateFromProto(r.State)
	}

	// TODO: Do we need a range-changed callback?

	n.muRanges.Lock()
	n.ranges = ranges
	n.muRanges.Unlock()

	return nil
}

// TODO: Replace this polling with streaming RPCs.
// Caller must hold ros.RWMutex
func (ros *Roster2) probe() {
	var wg sync.WaitGroup

	ctx, cancel := context.WithTimeout(context.Background(), probeTimeout)
	defer cancel()

	for _, node := range ros.Map {
		wg.Add(1)

		// Copy node since it changes between iterations.
		// https://golang.org/doc/faq#closures_and_goroutines
		go func(n *ShortNode) {
			defer wg.Done()
			err := probeOne(ctx, n)
			if err != nil {
				log.Printf("probe error from %v: %s", n.remote.Ident, err)
				return
			}
		}(node)
	}

	wg.Wait()
}

func (r *Roster2) Tick() {
	// TODO: anything but this
	r.Lock()
	defer r.Unlock()

	// Grab any new nodes from service discovery.
	r.discover()

	// Expire any nodes that have gone missing service discovery.
	r.expire()

	r.probe()
}

func (r *Roster2) Run(t *time.Ticker) {
	for ; true; <-t.C {
		r.Tick()
	}
}
