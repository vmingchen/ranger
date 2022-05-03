package roster

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/adammck/ranger/pkg/config"
	"github.com/adammck/ranger/pkg/discovery"
	"github.com/adammck/ranger/pkg/ranje"
	"github.com/adammck/ranger/pkg/roster/info"
	"github.com/adammck/ranger/pkg/roster/state"
	"github.com/adammck/ranger/pkg/test/fake_nodes"
	"github.com/stretchr/testify/suite"
)

type RosterSuite struct {
	suite.Suite
	ctx   context.Context
	cfg   config.Config
	nodes *fake_nodes.TestNodes
	rost  *Roster

	// Not important
	r *ranje.Range
}

func TestExampleTestSuite(t *testing.T) {
	suite.Run(t, new(RosterSuite))
}

func (ts *RosterSuite) SetupTest() {
	ts.ctx = context.Background()

	// Sensible defaults.
	ts.cfg = config.Config{
		DrainNodesBeforeShutdown: false,
		NodeExpireDuration:       1 * time.Hour, // never
		Replication:              1,
	}

	// Just to avoid constructing this thing everywhere.
	ts.r = &ranje.Range{
		Meta:  ranje.Meta{Ident: 1},
		State: ranje.RsActive,
	}

	// Empty by default.
	ts.nodes = fake_nodes.NewTestNodes()
}

func (ts *RosterSuite) Init() {
	ts.rost = New(ts.cfg, ts.nodes.Discovery(), nil, nil, nil)
	ts.rost.NodeConnFactory = ts.nodes.NodeConnFactory
}

func (ts *RosterSuite) TestNoCandidates() {
	ts.Init()

	nID, err := ts.rost.Candidate(ts.r, *ranje.AnyNode())
	if ts.Error(err) {
		ts.Equal(fmt.Errorf("no candidates available (rID=1, c=any)"), err)
	}
	ts.Equal(nID, "")
}

func (ts *RosterSuite) TestCandidateByNodeID() {
	ts.nodes.Add(ts.ctx, discovery.Remote{
		Ident: "test-aaa",
		Host:  "host-aaa",
		Port:  1,
	}, nil)

	ts.nodes.Add(ts.ctx, discovery.Remote{
		Ident: "test-bbb",
		Host:  "host-bbb",
		Port:  1,
	}, nil)

	ts.nodes.Add(ts.ctx, discovery.Remote{
		Ident: "test-ccc",
		Host:  "host-ccc",
		Port:  1,
	}, nil)

	ts.Init()
	ts.rost.Tick()

	nID, err := ts.rost.Candidate(ts.r, ranje.Constraint{NodeID: "test-bbb"})
	if ts.NoError(err) {
		ts.Equal(nID, "test-bbb")
	}

	nID, err = ts.rost.Candidate(ts.r, ranje.Constraint{NodeID: "test-ccc"})
	if ts.NoError(err) {
		ts.Equal(nID, "test-ccc")
	}
}

func (ts *RosterSuite) TestProbeOne() {

	rem := discovery.Remote{
		Ident: "test-aaa",
		Host:  "host-aaa",
		Port:  1,
	}

	r := &ranje.Range{
		Meta: ranje.Meta{
			Ident: 1,
			Start: ranje.ZeroKey,
			End:   ranje.Key("ggg"),
		},
		State: ranje.RsActive,
		Placements: []*ranje.Placement{{
			NodeID: rem.Ident,
			State:  ranje.PsReady,
		}},
	}

	fakeInfos := map[ranje.Ident]*info.RangeInfo{
		r.Meta.Ident: {
			Meta:  r.Meta,
			State: state.NsReady,
			Info: info.LoadInfo{
				Keys: 123,
			},
		},
	}

	ts.nodes.Add(ts.ctx, rem, fakeInfos)
	ts.Init()

	ts.rost.discover()

	// Far as the roster is concerned, this is a real node.
	rostNode := ts.rost.NodeByIdent("test-aaa")
	ts.Require().NotNil(rostNode)

	err := ts.rost.probeOne(ts.ctx, rostNode)
	if ts.NoError(err) {
		if rostInfo, ok := rostNode.Get(1); ts.True(ok) {

			// The "real" RangeInfo, which we got from the (fake) remote via
			// gRPC (via bufconn) through its rangelet should match the fake
			// RangeInfo above.
			ts.Equal(*fakeInfos[r.Meta.Ident], rostInfo)
		}
	}
}
