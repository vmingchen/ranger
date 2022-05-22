package orchestrator

import (
	"fmt"
	"log"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"context"

	"github.com/adammck/ranger/pkg/config"
	"github.com/adammck/ranger/pkg/discovery"
	"github.com/adammck/ranger/pkg/keyspace"
	pb "github.com/adammck/ranger/pkg/proto/gen"
	"github.com/adammck/ranger/pkg/ranje"
	"github.com/adammck/ranger/pkg/roster"
	"github.com/adammck/ranger/pkg/roster/info"
	"github.com/adammck/ranger/pkg/roster/state"
	"github.com/adammck/ranger/pkg/test/fake_nodes"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/testing/protocmp"
)

type OrchestratorSuite struct {
	suite.Suite
	ctx   context.Context
	cfg   config.Config
	nodes *fake_nodes.TestNodes
	ks    *keyspace.Keyspace
	rost  *roster.Roster
	orch  *Orchestrator
}

// ProtoEqual is a helper to compare two slices of protobufs. It's not great.
func ProtoEqual(t *testing.T, expected, actual interface{}) {
	if diff := cmp.Diff(expected, actual, protocmp.Transform()); diff != "" {
		t.Errorf(fmt.Sprintf("Not equal (-want +got):\n%s\n", diff))
	}
}

// nIDs is a helper to extract the list of nIDs from map like RPCs returns.
func nIDs(obj map[string][]interface{}) []string {
	ret := []string{}

	for k := range obj {
		ret = append(ret, k)
	}

	sort.Strings(ret)
	return ret
}

func RPCs(obj map[string][]interface{}) []interface{} {
	ret := []interface{}{}

	for _, v := range obj {
		ret = append(ret, v...)
	}

	return ret
}

// Init should be called at the top of each test to define the state of the
// world. Nothing will work until this method is called.
func (ts *OrchestratorSuite) Init(ks *keyspace.Keyspace) {
	ts.ks = ks
	srv := grpc.NewServer() // TODO: Allow this to be nil.
	ts.orch = New(ts.cfg, ts.ks, ts.rost, srv)
}

func testConfig() config.Config {
	return config.Config{
		DrainNodesBeforeShutdown: false,
		NodeExpireDuration:       1 * time.Hour, // never
		Replication:              1,
	}
}

func (ts *OrchestratorSuite) SetupTest() {
	ts.ctx = context.Background()

	// Sensible defaults.
	// TODO: Better to set these per test, but that's too late because we've
	//       already created the objects in this method, and config is supposed
	//       to be immutable. Better rethink this.
	ts.cfg = testConfig()

	ts.nodes = fake_nodes.NewTestNodes()
	ts.rost = roster.New(ts.cfg, ts.nodes.Discovery(), nil, nil, nil)
	ts.rost.NodeConnFactory = ts.nodes.NodeConnFactory
}

func requireStable(t *testing.T, orch *Orchestrator) {
	ksLog := orch.ks.LogString()
	rostLog := orch.rost.TestString()
	for i := 0; i < 2; i++ {
		tickWait(orch)
		orch.rost.Tick()
		// Use require (vs assert) since spamming the same error doesn't help.
		require.Equal(t, ksLog, orch.ks.LogString())
		require.Equal(t, rostLog, orch.rost.TestString())
	}
}

// TODO: Return a function to do this from OrchFactory.
func (ts *OrchestratorSuite) TearDownTest() {
	if ts.nodes != nil {
		ts.nodes.Close()
	}
}

type Waiter interface {
	Wait()
}

// tickWait performs a Tick, then waits for any pending RPCs to complete, then
// waits for any give Waiters (which are probably fake_node.Barrier instances)
// to return.
//
// This allows us to pretend that Ticks will never begin while RPCs scheduled
// during the previous tick are still in flight, without sleeping or anything
// like that.
func tickWait(orch *Orchestrator, waiters ...Waiter) {
	orch.Tick()
	orch.WaitRPCs()

	for _, w := range waiters {
		w.Wait()
	}
}

func tickUntilStable(ts *OrchestratorSuite) {
	var ksPrev string // previous value of ks.LogString
	var stable int    // ticks since keyspace changed or rpc sent
	var ticks int     // total ticks waited

	for {
		log.Print("Tick")
		tickWait(ts.orch)
		rpcs := len(ts.nodes.RPCs())

		// Keyspace changed since last tick, or RPCs sent? Keep ticking.
		ksNow := ts.ks.LogString()
		if ksPrev != ksNow || rpcs > 0 {
			ksPrev = ksNow
			stable = 0
		} else {
			stable += 1
		}

		// Stable for a few ticks? We're done.
		if stable >= 2 {
			break
		}

		ticks += 1
		if ticks > 50 {
			ts.FailNow("didn't stablize after 50 ticks")
			return
		}
	}

	// Perform a single probe cycle before returning, to update the remote
	// state of all nodes. (Otherwise we're only observing the remote state as
	// returned from the RPCs. Any state changes which happened outside of those
	// RPCs will be missed.)
	ts.rost.Tick()
}

// TODO: Move to keyspace tests.
func TestJunk(t *testing.T) {
	ksStr := ""
	rosStr := ""
	orch, nodes := orchFactory(t, ksStr, rosStr, testConfig())
	nodes.SetStrictTransitions(false)

	orch.rost.Tick()
	assert.Equal(t, "{1 [-inf, +inf] RsActive}", orch.ks.LogString())
	assert.Equal(t, rosStr, orch.rost.TestString())

	// -------------------------------------------------------------------------

	r := mustGetRange(t, orch.ks, 1)
	assert.NotNil(t, r)
	assert.Equal(t, ranje.ZeroKey, r.Meta.Start, "range should start at ZeroKey")
	assert.Equal(t, ranje.ZeroKey, r.Meta.End, "range should end at ZeroKey")
	assert.Equal(t, ranje.RsActive, r.State, "range should be born active")
	assert.Equal(t, 0, len(r.Placements), "range should be born with no placements")
}

func (ts *OrchestratorSuite) TestPlacementFast() {
	initTestPlacement(ts)
	tickUntilStable(ts)

	ts.Equal("{test-aaa [1:NsReady]}", ts.rost.TestString())
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsReady}", ts.ks.LogString())
}

func (ts *OrchestratorSuite) TestPlacementReadyError() {
	na, nb := remoteFactoryTwo("aaa", "bbb")
	ts.Init(keyspaceFactory(ts.T(), ts.cfg, nil))
	nodeFactory(ts.ctx, ts.nodes, nodeStub{na, nil}, nodeStub{nb, nil})

	ts.rost.Tick()
	ts.Equal("{1 [-inf, +inf] RsActive}", ts.ks.LogString())
	ts.Equal("{test-aaa []} {test-bbb []}", ts.rost.TestString())

	// Node aaa will always fail to become ready.
	ts.nodes.Get("test-aaa").SetReturnValue(ts.T(), 1, state.NsReadying, fmt.Errorf("can't get ready!"))

	// ----

	tickUntilStable(ts)

	// TODO: This is not a good state! The controller has discarded the
	//       placement on aaa, but the node will remember it forever. That's the
	//       only way that it ended up on bbb, which I think is also buggy.
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-bbb:PsReady}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsReadyingError]} {test-bbb [1:NsReady]}", ts.rost.TestString())
}

func (ts *OrchestratorSuite) TestPlacementWithFailures() {
	na, nb := remoteFactoryTwo("aaa", "bbb")
	ts.Init(keyspaceFactory(ts.T(), ts.cfg, nil))
	nodeFactory(ts.ctx, ts.nodes, nodeStub{na, nil}, nodeStub{nb, nil})

	ts.rost.Tick()
	ts.Equal("{1 [-inf, +inf] RsActive}", ts.ks.LogString())
	ts.Equal("{test-aaa []} {test-bbb []}", ts.rost.TestString())

	// ----

	ts.nodes.Get("test-aaa").SetReturnValue(ts.T(), 1, state.NsPreparing, fmt.Errorf("something went wrong"))

	tickUntilStable(ts)
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-bbb:PsReady}", ts.ks.LogString())
	ts.Equal("{test-aaa []} {test-bbb [1:NsReady]}", ts.rost.TestString())
}

func (ts *OrchestratorSuite) TestPlacementMedium() {
	initTestPlacement(ts)

	// First tick: Placement created, Give RPC sent to node and returned
	// successfully. Remote state is updated in roster, but not keyspace.

	tickWait(ts.orch)
	if rpcs := ts.nodes.RPCs(); ts.Equal([]string{"test-aaa"}, nIDs(rpcs)) {
		if aaa := rpcs["test-aaa"]; ts.Len(aaa, 1) {
			ProtoEqual(ts.T(), &pb.GiveRequest{
				Range: &pb.RangeMeta{
					Ident: 1,
					Start: []byte(ranje.ZeroKey),
					End:   []byte(ranje.ZeroKey),
				},
				// TODO: It's weird and kind of useless for this to be in here.
				Parents: []*pb.Parent{
					{
						Range: &pb.RangeMeta{
							Ident: 1,
							Start: []byte(ranje.ZeroKey),
							End:   []byte(ranje.ZeroKey),
						},
						Parent: []uint64{},
						Placements: []*pb.Placement{
							{
								Node:  "host-aaa:1",
								State: pb.PlacementState_PS_PENDING,
							},
						},
					},
				},
			}, aaa[0])
		}
	}

	ts.Equal("{test-aaa [1:NsPrepared]}", ts.rost.TestString())
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsPending}", ts.ks.LogString())

	// Second tick: Keyspace is updated with state from roster. No RPCs sent.

	tickWait(ts.orch)
	ts.Empty(ts.nodes.RPCs())
	ts.Equal("{test-aaa [1:NsPrepared]}", ts.rost.TestString())
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsPrepared}", ts.ks.LogString())

	// Third: Serve RPC is sent, to advance to ready. Returns success, and
	// roster is updated. Keyspace is not.

	tickWait(ts.orch)
	if rpcs := ts.nodes.RPCs(); ts.Equal([]string{"test-aaa"}, nIDs(rpcs)) {
		if aaa := rpcs["test-aaa"]; ts.Len(aaa, 1) {
			ProtoEqual(ts.T(), &pb.ServeRequest{Range: 1}, aaa[0])
		}
	}

	ts.Equal("{test-aaa [1:NsReady]}", ts.rost.TestString())
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsPrepared}", ts.ks.LogString())

	// Forth: Keyspace is updated with ready state from roster. No RPCs sent.

	tickWait(ts.orch)
	ts.Empty(ts.nodes.RPCs())
	ts.Equal("{test-aaa [1:NsReady]}", ts.rost.TestString())
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsReady}", ts.ks.LogString())

	// No more changes. This is steady state.

	requireStable(ts.T(), ts.orch)
}

func (ts *OrchestratorSuite) TestPlacementSlow() {
	initTestPlacement(ts)
	ts.nodes.SetStrictTransitions(true)

	par := ts.nodes.Get("test-aaa").AddBarrier(ts.T(), 1, state.NsPreparing)
	tickWait(ts.orch, par)
	if rpcs := ts.nodes.RPCs(); ts.Equal([]string{"test-aaa"}, nIDs(rpcs)) {
		if aaa := rpcs["test-aaa"]; ts.Len(aaa, 1) {
			ProtoEqual(ts.T(), &pb.GiveRequest{
				Range: &pb.RangeMeta{
					Ident: 1,
					Start: []byte(ranje.ZeroKey),
					End:   []byte(ranje.ZeroKey),
				},
				// TODO: It's weird and kind of useless for this to be in here.
				Parents: []*pb.Parent{
					{
						Range: &pb.RangeMeta{
							Ident: 1,
							Start: []byte(ranje.ZeroKey),
							End:   []byte(ranje.ZeroKey),
						},
						Parent: []uint64{},
						Placements: []*pb.Placement{
							{
								Node:  "host-aaa:1",
								State: pb.PlacementState_PS_PENDING,
							},
						},
					},
				},
			}, aaa[0])
		}
	}

	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsPending}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsPreparing]}", ts.rost.TestString())
	// TODO: Assert that new placement was persisted

	tickWait(ts.orch)
	ts.Len(RPCs(ts.nodes.RPCs()), 1) // redundant Give
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsPending}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsPreparing]}", ts.rost.TestString())

	// The node finished preparing, but we don't know about it.
	par.Release()
	ts.Equal("{test-aaa [1:NsPreparing]}", ts.rost.TestString())

	tickWait(ts.orch)
	ts.Len(RPCs(ts.nodes.RPCs()), 1) // redundant Give
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsPending}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsPrepared]}", ts.rost.TestString())

	// This tick notices that the remote state (which was updated at the end of
	// the previous tick, after the (redundant) Give RPC returned) now indicates
	// that the node has finished preparing.
	//
	// TODO: Maybe the state update should immediately trigger another tick just
	//       for that placement? Would save a tick, but risks infinite loops.
	tickWait(ts.orch)
	ts.Empty(ts.nodes.RPCs())
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsPrepared}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsPrepared]}", ts.rost.TestString())

	ar := ts.nodes.Get("test-aaa").AddBarrier(ts.T(), 1, state.NsReadying)
	tickWait(ts.orch, ar)
	ts.Len(RPCs(ts.nodes.RPCs()), 1) // redundant Serve
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsPrepared}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsReadying]}", ts.rost.TestString())

	// The node became ready, but as above, we don't know about it.
	ar.Release()
	ts.Equal("{test-aaa [1:NsReadying]}", ts.rost.TestString())

	tickWait(ts.orch)
	ts.Len(RPCs(ts.nodes.RPCs()), 1) // redundant Serve
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsPrepared}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsReady]}", ts.rost.TestString())

	tickWait(ts.orch)
	ts.Empty(ts.nodes.RPCs())
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsReady}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsReady]}", ts.rost.TestString())

	requireStable(ts.T(), ts.orch)
}

func TestMissingPlacement(t *testing.T) {
	ksStr := "{1 [-inf, +inf] RsActive p0=test-aaa:PsReady}"
	rosStr := "{test-aaa []}"
	orch, nodes := orchFactory(t, ksStr, rosStr, testConfig())
	nodes.SetStrictTransitions(false)

	orch.rost.Tick()
	assert.Equal(t, ksStr, orch.ks.LogString())
	assert.Equal(t, rosStr, orch.rost.TestString())

	// -------------------------------------------------------------------------

	// Orchestrator notices that the node doesn't have the range, so marks the
	// placement as abandoned.

	tickWait(orch)
	assert.Empty(t, nodes.RPCs())
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsGiveUp}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa []}", orch.rost.TestString())

	// Orchestrator advances to drop the placement, but (unlike when moving)
	// doesn't bother to notify the node via RPC. It has already told us that it
	// doesn't have the range.

	tickWait(orch)
	assert.Empty(t, nodes.RPCs())
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsDropped}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa []}", orch.rost.TestString())

	// The placement is destroyed.

	tickWait(orch)
	assert.Empty(t, nodes.RPCs())
	assert.Equal(t, "{1 [-inf, +inf] RsActive}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa []}", orch.rost.TestString())

	// From here we continue as usual. No need to repeat TestPlacement.

	tickWait(orch)
	if rpcs := nodes.RPCs(); assert.Equal(t, []string{"test-aaa"}, nIDs(rpcs)) {
		if aaa := rpcs["test-aaa"]; assert.Len(t, aaa, 1) {
			ProtoEqual(t, &pb.GiveRequest{
				Range: &pb.RangeMeta{
					Ident: 1,
				},
				Parents: []*pb.Parent{
					{
						Range: &pb.RangeMeta{
							Ident: 1,
						},
						Placements: []*pb.Placement{
							{
								Node:  "host-test-aaa:1",
								State: pb.PlacementState_PS_PENDING,
							},
						},
					},
				},
			}, aaa[0])
		}
	}

	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsPending}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsPrepared]}", orch.rost.TestString())
}

func (ts *OrchestratorSuite) TestMoveFast() {
	r1 := initTestMove(ts, false)
	requireStable(ts.T(), ts.orch)

	func() {
		ts.orch.opMovesMu.Lock()
		defer ts.orch.opMovesMu.Unlock()
		// TODO: Probably add a method to do this.
		ts.orch.opMoves = append(ts.orch.opMoves, OpMove{
			Range: r1.Meta.Ident,
			Dest:  "test-bbb",
		})
	}()

	tickUntilStable(ts)

	// Range moved from aaa to bbb.
	ts.Equal("{test-aaa []} {test-bbb [1:NsReady]}", ts.rost.TestString())
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-bbb:PsReady}", ts.ks.LogString())
}

func (ts *OrchestratorSuite) TestMoveSlow() {
	r1 := initTestMove(ts, true)
	requireStable(ts.T(), ts.orch)

	func() {
		ts.orch.opMovesMu.Lock()
		defer ts.orch.opMovesMu.Unlock()
		// TODO: Probably add a method to do this.
		ts.orch.opMoves = append(ts.orch.opMoves, OpMove{
			Range: r1.Meta.Ident,
			Dest:  "test-bbb",
		})
	}()

	bPAR := ts.nodes.Get("test-bbb").AddBarrier(ts.T(), 1, state.NsPreparing)
	tickWait(ts.orch, bPAR)
	if rpcs := ts.nodes.RPCs(); ts.Equal([]string{"test-bbb"}, nIDs(rpcs)) {
		if bbb := rpcs["test-bbb"]; ts.Len(bbb, 1) {
			ProtoEqual(ts.T(), &pb.GiveRequest{
				Range: &pb.RangeMeta{
					Ident: 1,
					Start: []byte(ranje.ZeroKey),
					End:   []byte(ranje.ZeroKey),
				},
				Parents: []*pb.Parent{
					{
						Range: &pb.RangeMeta{
							Ident: 1,
							Start: []byte(ranje.ZeroKey),
							End:   []byte(ranje.ZeroKey),
						},
						Placements: []*pb.Placement{
							{
								Node:  "host-aaa:1",
								State: pb.PlacementState_PS_READY,
							},
							{
								Node:  "host-bbb:1",
								State: pb.PlacementState_PS_PENDING,
							},
						},
					},
				},
			}, bbb[0])
		}
	}

	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsReady p1=test-bbb:PsPending:replacing(test-aaa)}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsReady]} {test-bbb [1:NsPreparing]}", ts.rost.TestString())

	// Node B finished preparing.
	bPAR.Release()
	ts.Equal("{test-aaa [1:NsReady]} {test-bbb [1:NsPreparing]}", ts.rost.TestString())

	ts.rost.Tick()
	ts.Equal("{test-aaa [1:NsReady]} {test-bbb [1:NsPrepared]}", ts.rost.TestString())

	// Just updates state from roster.
	// TODO: As above, should maybe trigger the next tick automatically.
	tickWait(ts.orch)
	ts.Empty(ts.nodes.RPCs())
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsReady p1=test-bbb:PsPrepared:replacing(test-aaa)}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsReady]} {test-bbb [1:NsPrepared]}", ts.rost.TestString())

	aPDR := ts.nodes.Get("test-aaa").AddBarrier(ts.T(), 1, state.NsTaking)
	tickWait(ts.orch, aPDR)
	if rpcs := ts.nodes.RPCs(); ts.Equal([]string{"test-aaa"}, nIDs(rpcs)) {
		if aaa := rpcs["test-aaa"]; ts.Len(aaa, 1) {
			ProtoEqual(ts.T(), &pb.TakeRequest{Range: 1}, aaa[0])
		}
	}

	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsReady p1=test-bbb:PsPrepared:replacing(test-aaa)}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsTaking]} {test-bbb [1:NsPrepared]}", ts.rost.TestString())

	tickWait(ts.orch)
	ts.Len(RPCs(ts.nodes.RPCs()), 1) // redundant Take
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsReady p1=test-bbb:PsPrepared:replacing(test-aaa)}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsTaking]} {test-bbb [1:NsPrepared]}", ts.rost.TestString())

	aPDR.Release() // Node A finished taking.
	ts.Equal("{test-aaa [1:NsTaking]} {test-bbb [1:NsPrepared]}", ts.rost.TestString())

	ts.rost.Tick()
	ts.Equal("{test-aaa [1:NsTaken]} {test-bbb [1:NsPrepared]}", ts.rost.TestString())

	bAR := ts.nodes.Get("test-bbb").AddBarrier(ts.T(), 1, state.NsReadying)
	tickWait(ts.orch, bAR)
	if rpcs := ts.nodes.RPCs(); ts.Equal([]string{"test-bbb"}, nIDs(rpcs)) {
		if bbb := rpcs["test-bbb"]; ts.Len(bbb, 1) {
			ProtoEqual(ts.T(), &pb.ServeRequest{Range: 1}, bbb[0])
		}
	}

	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsTaken p1=test-bbb:PsPrepared:replacing(test-aaa)}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsTaken]} {test-bbb [1:NsReadying]}", ts.rost.TestString())

	tickWait(ts.orch)
	ts.Len(RPCs(ts.nodes.RPCs()), 1) // redundant Serve
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsTaken p1=test-bbb:PsPrepared:replacing(test-aaa)}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsTaken]} {test-bbb [1:NsReadying]}", ts.rost.TestString())

	bAR.Release() // Node B finished becoming ready.
	ts.Equal("{test-aaa [1:NsTaken]} {test-bbb [1:NsReadying]}", ts.rost.TestString())

	ts.rost.Tick()
	ts.Equal("{test-aaa [1:NsTaken]} {test-bbb [1:NsReady]}", ts.rost.TestString())

	tickWait(ts.orch)
	ts.Empty(ts.nodes.RPCs())
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsTaken p1=test-bbb:PsReady:replacing(test-aaa)}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsTaken]} {test-bbb [1:NsReady]}", ts.rost.TestString())

	aDR := ts.nodes.Get("test-aaa").AddBarrier(ts.T(), 1, state.NsDropping)
	tickWait(ts.orch, aDR)
	if rpcs := ts.nodes.RPCs(); ts.Equal([]string{"test-aaa"}, nIDs(rpcs)) {
		if aaa := rpcs["test-aaa"]; ts.Len(aaa, 1) {
			ProtoEqual(ts.T(), &pb.DropRequest{Range: 1}, aaa[0])
		}
	}

	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsTaken p1=test-bbb:PsReady:replacing(test-aaa)}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsDropping]} {test-bbb [1:NsReady]}", ts.rost.TestString())

	aDR.Release() // Node A finished dropping.
	ts.Equal("{test-aaa [1:NsDropping]} {test-bbb [1:NsReady]}", ts.rost.TestString())

	tickWait(ts.orch)
	ts.Len(RPCs(ts.nodes.RPCs()), 1) // redundant Drop
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsTaken p1=test-bbb:PsReady:replacing(test-aaa)}", ts.ks.LogString())
	ts.Equal("{test-aaa []} {test-bbb [1:NsReady]}", ts.rost.TestString())

	tickWait(ts.orch)
	ts.Empty(ts.nodes.RPCs())
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsDropped p1=test-bbb:PsReady:replacing(test-aaa)}", ts.ks.LogString())
	ts.Equal("{test-aaa []} {test-bbb [1:NsReady]}", ts.rost.TestString())

	// test-aaa is gone!
	tickWait(ts.orch)
	ts.Empty(ts.nodes.RPCs())
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-bbb:PsReady:replacing(test-aaa)}", ts.ks.LogString())
	ts.Equal("{test-aaa []} {test-bbb [1:NsReady]}", ts.rost.TestString())

	// IsReplacing annotation is gone.
	tickWait(ts.orch)
	ts.Empty(ts.nodes.RPCs())
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-bbb:PsReady}", ts.ks.LogString())
	ts.Equal("{test-aaa []} {test-bbb [1:NsReady]}", ts.rost.TestString())

	requireStable(ts.T(), ts.orch)
}

func (ts *OrchestratorSuite) TestSplitFast() {
	r1 := initTestSplit(ts, false)
	requireStable(ts.T(), ts.orch)

	op := OpSplit{
		Range: r1.Meta.Ident,
		Key:   "ccc",
		Err:   make(chan error),
	}

	ts.orch.opSplitsMu.Lock()
	ts.orch.opSplits[r1.Meta.Ident] = op
	ts.orch.opSplitsMu.Unlock()

	tickUntilStable(ts)

	// Range 1 was split into ranges 2 and 3 at ccc.
	ts.Equal("{test-aaa [2:NsReady, 3:NsReady]}", ts.rost.TestString())
	ts.Equal("{1 [-inf, +inf] RsObsolete} {2 [-inf, ccc] RsActive p0=test-aaa:PsReady} {3 (ccc, +inf] RsActive p0=test-aaa:PsReady}", ts.ks.LogString())
}

func (ts *OrchestratorSuite) TestSplitSlow() {
	r1 := initTestSplit(ts, true)

	op := OpSplit{
		Range: r1.Meta.Ident,
		Key:   "ccc",
		Err:   make(chan error),
	}

	ts.orch.opSplitsMu.Lock()
	ts.orch.opSplits[r1.Meta.Ident] = op
	ts.orch.opSplitsMu.Unlock()

	// 1. Split initiated by controller. Node hasn't heard about it yet.

	tickWait(ts.orch)
	ts.Empty(ts.nodes.RPCs())
	ts.Equal("{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsReady} {2 [-inf, ccc] RsActive p0=test-aaa:PsPending} {3 (ccc, +inf] RsActive p0=test-aaa:PsPending}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsReady]}", ts.rost.TestString())

	// 2. Controller places new ranges on nodes.

	a2PAR := ts.nodes.Get("test-aaa").AddBarrier(ts.T(), 2, state.NsPreparing)
	a3PAR := ts.nodes.Get("test-aaa").AddBarrier(ts.T(), 3, state.NsPreparing)
	tickWait(ts.orch, a2PAR, a3PAR)
	if rpcs := ts.nodes.RPCs(); ts.Equal([]string{"test-aaa"}, nIDs(rpcs)) {
		if aaa := rpcs["test-aaa"]; ts.Len(aaa, 2) {
			ProtoEqual(ts.T(), &pb.GiveRequest{
				Range: &pb.RangeMeta{
					Ident: 2,
					Start: []byte(ranje.ZeroKey),
					End:   []byte("ccc"),
				},
				Parents: []*pb.Parent{
					{
						Range: &pb.RangeMeta{
							Ident: 2,
							Start: []byte(ranje.ZeroKey),
							End:   []byte("ccc"),
						},
						Placements: []*pb.Placement{
							{
								Node:  "host-aaa:1",
								State: pb.PlacementState_PS_PENDING,
							},
						},
					},
					{
						Range: &pb.RangeMeta{
							Ident: 1,
						},
						Placements: []*pb.Placement{
							{
								Node:  "host-aaa:1",
								State: pb.PlacementState_PS_READY,
							},
						},
					},
				},
			}, aaa[0])
			ProtoEqual(ts.T(), &pb.GiveRequest{
				Range: &pb.RangeMeta{
					Ident: 3,
					Start: []byte("ccc"),
					End:   []byte(ranje.ZeroKey),
				},
				Parents: []*pb.Parent{
					{
						Range: &pb.RangeMeta{
							Ident: 3,
							Start: []byte("ccc"),
							End:   []byte(ranje.ZeroKey),
						},
						Placements: []*pb.Placement{
							{
								Node:  "host-aaa:1",
								State: pb.PlacementState_PS_PENDING,
							},
						},
					},
					{
						Range: &pb.RangeMeta{
							Ident: 1,
							Start: []byte(ranje.ZeroKey),
							End:   []byte(ranje.ZeroKey),
						},
						Placements: []*pb.Placement{
							{
								Node:  "host-aaa:1",
								State: pb.PlacementState_PS_READY,
							},
						},
					},
				},
			}, aaa[1])
		}
	}

	ts.Equal("{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsReady} {2 [-inf, ccc] RsActive p0=test-aaa:PsPending} {3 (ccc, +inf] RsActive p0=test-aaa:PsPending}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsReady, 2:NsPreparing, 3:NsPreparing]}", ts.rost.TestString())

	// 3. Wait for placements to become Prepared.

	a2PAR.Release() // R2 finished preparing, but R3 has not yet.
	ts.Equal("{test-aaa [1:NsReady, 2:NsPreparing, 3:NsPreparing]}", ts.rost.TestString())

	tickWait(ts.orch)
	ts.Len(RPCs(ts.nodes.RPCs()), 2) // redundant Gives
	ts.Equal("{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsReady} {2 [-inf, ccc] RsActive p0=test-aaa:PsPending} {3 (ccc, +inf] RsActive p0=test-aaa:PsPending}", ts.ks.LogString())

	a3PAR.Release() // R3 becomes Prepared, too.
	ts.Equal("{test-aaa [1:NsReady, 2:NsPrepared, 3:NsPreparing]}", ts.rost.TestString())

	tickWait(ts.orch)
	// Note that we're not sending (redundant) Give RPCs to R2 any more.
	ts.Len(RPCs(ts.nodes.RPCs()), 1) // redundant Give
	ts.Equal("{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsReady} {2 [-inf, ccc] RsActive p0=test-aaa:PsPrepared} {3 (ccc, +inf] RsActive p0=test-aaa:PsPending}", ts.ks.LogString())

	tickWait(ts.orch)
	ts.Empty(ts.nodes.RPCs())
	ts.Equal("{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsReady} {2 [-inf, ccc] RsActive p0=test-aaa:PsPrepared} {3 (ccc, +inf] RsActive p0=test-aaa:PsPrepared}", ts.ks.LogString())

	// 4. Controller takes placements in parent range.

	a1PDR := ts.nodes.Get("test-aaa").AddBarrier(ts.T(), 1, state.NsTaking)
	tickWait(ts.orch, a1PDR)
	if rpcs := ts.nodes.RPCs(); ts.Equal([]string{"test-aaa"}, nIDs(rpcs)) {
		if aaa := rpcs["test-aaa"]; ts.Len(aaa, 1) {
			ProtoEqual(ts.T(), &pb.TakeRequest{Range: 1}, aaa[0])
		}
	}

	ts.Equal("{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsReady} {2 [-inf, ccc] RsActive p0=test-aaa:PsPrepared} {3 (ccc, +inf] RsActive p0=test-aaa:PsPrepared}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsTaking, 2:NsPrepared, 3:NsPrepared]}", ts.rost.TestString())

	a1PDR.Release() // r1p0 finishes taking.
	ts.Equal("{test-aaa [1:NsTaking, 2:NsPrepared, 3:NsPrepared]}", ts.rost.TestString())

	tickWait(ts.orch)
	ts.Len(RPCs(ts.nodes.RPCs()), 1) // redundant Take
	ts.Equal("{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsReady} {2 [-inf, ccc] RsActive p0=test-aaa:PsPrepared} {3 (ccc, +inf] RsActive p0=test-aaa:PsPrepared}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsTaken, 2:NsPrepared, 3:NsPrepared]}", ts.rost.TestString())

	// 5. Controller instructs both child ranges to become Ready.
	a2AR := ts.nodes.Get("test-aaa").AddBarrier(ts.T(), 2, state.NsReadying)
	a3AR := ts.nodes.Get("test-aaa").AddBarrier(ts.T(), 3, state.NsReadying)
	tickWait(ts.orch, a2AR, a3AR)
	if rpcs := ts.nodes.RPCs(); ts.Equal([]string{"test-aaa"}, nIDs(rpcs)) {
		if aaa := rpcs["test-aaa"]; ts.Len(aaa, 2) {
			ProtoEqual(ts.T(), &pb.ServeRequest{Range: 2}, aaa[0])
			ProtoEqual(ts.T(), &pb.ServeRequest{Range: 3}, aaa[1])
		}
	}

	ts.Equal("{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsTaken} {2 [-inf, ccc] RsActive p0=test-aaa:PsPrepared} {3 (ccc, +inf] RsActive p0=test-aaa:PsPrepared}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsTaken, 2:NsReadying, 3:NsReadying]}", ts.rost.TestString())

	a3AR.Release() // r3p0 becomes ready.
	ts.Equal("{test-aaa [1:NsTaken, 2:NsReadying, 3:NsReadying]}", ts.rost.TestString())

	// Orchestrator notices on next tick.
	tickWait(ts.orch)
	ts.Len(RPCs(ts.nodes.RPCs()), 2) // redundant Serves
	ts.Equal("{test-aaa [1:NsTaken, 2:NsReadying, 3:NsReady]}", ts.rost.TestString())
	ts.Equal("{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsTaken} {2 [-inf, ccc] RsActive p0=test-aaa:PsPrepared} {3 (ccc, +inf] RsActive p0=test-aaa:PsPrepared}", ts.ks.LogString())

	a2AR.Release() // r2p0 becomes ready.
	ts.Equal("{test-aaa [1:NsTaken, 2:NsReadying, 3:NsReady]}", ts.rost.TestString())

	// Orchestrator notices on next tick.
	tickWait(ts.orch)
	ts.Len(RPCs(ts.nodes.RPCs()), 1) // redundant Serve
	ts.Equal("{test-aaa [1:NsTaken, 2:NsReady, 3:NsReady]}", ts.rost.TestString())
	ts.Equal("{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsTaken} {2 [-inf, ccc] RsActive p0=test-aaa:PsPrepared} {3 (ccc, +inf] RsActive p0=test-aaa:PsReady}", ts.ks.LogString())

	tickWait(ts.orch)
	ts.Empty(ts.nodes.RPCs())
	ts.Equal("{test-aaa [1:NsTaken, 2:NsReady, 3:NsReady]}", ts.rost.TestString())
	ts.Equal("{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsTaken} {2 [-inf, ccc] RsActive p0=test-aaa:PsReady} {3 (ccc, +inf] RsActive p0=test-aaa:PsReady}", ts.ks.LogString())

	// 6. Orchestrator instructs parent range to drop placements.

	a1DR := ts.nodes.Get("test-aaa").AddBarrier(ts.T(), 1, state.NsDropping)
	tickWait(ts.orch, a1DR)
	if rpcs := ts.nodes.RPCs(); ts.Equal([]string{"test-aaa"}, nIDs(rpcs)) {
		if aaa := rpcs["test-aaa"]; ts.Len(aaa, 1) {
			ProtoEqual(ts.T(), &pb.DropRequest{Range: 1}, aaa[0])
		}
	}

	ts.Equal("{test-aaa [1:NsDropping, 2:NsReady, 3:NsReady]}", ts.rost.TestString())
	ts.Equal("{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsTaken} {2 [-inf, ccc] RsActive p0=test-aaa:PsReady} {3 (ccc, +inf] RsActive p0=test-aaa:PsReady}", ts.ks.LogString())

	tickWait(ts.orch)
	ts.Len(RPCs(ts.nodes.RPCs()), 1) // redundant Drop
	ts.Equal("{test-aaa [1:NsDropping, 2:NsReady, 3:NsReady]}", ts.rost.TestString())
	ts.Equal("{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsTaken} {2 [-inf, ccc] RsActive p0=test-aaa:PsReady} {3 (ccc, +inf] RsActive p0=test-aaa:PsReady}", ts.ks.LogString())

	a1DR.Release() // r1p0 finishes dropping.

	tickWait(ts.orch)
	ts.Len(RPCs(ts.nodes.RPCs()), 1) // redundant Drop
	ts.Equal("{test-aaa [2:NsReady, 3:NsReady]}", ts.rost.TestString())
	ts.Equal("{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsTaken} {2 [-inf, ccc] RsActive p0=test-aaa:PsReady} {3 (ccc, +inf] RsActive p0=test-aaa:PsReady}", ts.ks.LogString())

	tickWait(ts.orch)
	ts.Empty(ts.nodes.RPCs())
	ts.Equal("{test-aaa [2:NsReady, 3:NsReady]}", ts.rost.TestString())
	ts.Equal("{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsDropped} {2 [-inf, ccc] RsActive p0=test-aaa:PsReady} {3 (ccc, +inf] RsActive p0=test-aaa:PsReady}", ts.ks.LogString())

	tickWait(ts.orch)
	ts.Empty(ts.nodes.RPCs())
	ts.Equal("{test-aaa [2:NsReady, 3:NsReady]}", ts.rost.TestString())
	ts.Equal("{1 [-inf, +inf] RsSubsuming} {2 [-inf, ccc] RsActive p0=test-aaa:PsReady} {3 (ccc, +inf] RsActive p0=test-aaa:PsReady}", ts.ks.LogString())

	tickWait(ts.orch)
	ts.Empty(ts.nodes.RPCs())
	ts.Equal("{test-aaa [2:NsReady, 3:NsReady]}", ts.rost.TestString())
	ts.Equal("{1 [-inf, +inf] RsObsolete} {2 [-inf, ccc] RsActive p0=test-aaa:PsReady} {3 (ccc, +inf] RsActive p0=test-aaa:PsReady}", ts.ks.LogString())

	// Whenever the next probe cycle happens, we notice that the range is gone
	// from the node, because it was dropped. Orchestrator doesn't notice this, but
	// maybe should, after sending the redundant Drop RPC?
	ts.rost.Tick()
	ts.Equal("{test-aaa [2:NsReady, 3:NsReady]}", ts.rost.TestString())

	requireStable(ts.T(), ts.orch)

	// Assert that the error chan was closed, to indicate op is complete.
	select {
	case err, ok := <-op.Err:
		if ok {
			ts.NoError(err)
		}
	default:
		ts.Fail("expected op.Err to be closed")
	}
}

func (ts *OrchestratorSuite) TestJoinFast() {
	r1, r2 := initTestJoin(ts, false)
	requireStable(ts.T(), ts.orch)

	op := OpJoin{
		Left:  r1.Meta.Ident,
		Right: r2.Meta.Ident,
		Dest:  "test-ccc",
		Err:   make(chan error),
	}

	ts.orch.opJoinsMu.Lock()
	ts.orch.opJoins = append(ts.orch.opJoins, op)
	ts.orch.opJoinsMu.Unlock()

	tickUntilStable(ts)

	// Ranges 1 and 2 were joined into range 3, which holds the entire keyspace.
	ts.Equal("{1 [-inf, ggg] RsObsolete} {2 (ggg, +inf] RsObsolete} {3 [-inf, +inf] RsActive p0=test-ccc:PsReady}", ts.ks.LogString())
	ts.Equal("{test-aaa []} {test-bbb []} {test-ccc [3:NsReady]}", ts.rost.TestString())
}

func (ts *OrchestratorSuite) TestJoinSlow() {
	r1, r2 := initTestJoin(ts, true)

	// Inject a join to get us started.
	// TODO: Do this via the operator interface instead.
	// TODO: Inject the target node for r3. It currently defaults to the empty
	//       node

	op := OpJoin{
		Left:  r1.Meta.Ident,
		Right: r2.Meta.Ident,
		Dest:  "test-ccc",
		Err:   make(chan error),
	}

	ts.orch.opJoinsMu.Lock()
	ts.orch.opJoins = append(ts.orch.opJoins, op)
	ts.orch.opJoinsMu.Unlock()

	// 1. Controller initiates join.

	tickWait(ts.orch)
	ts.Empty(ts.nodes.RPCs())
	ts.Equal("{1 [-inf, ggg] RsSubsuming p0=test-aaa:PsReady} {2 (ggg, +inf] RsSubsuming p0=test-bbb:PsReady} {3 [-inf, +inf] RsActive p0=test-ccc:PsPending}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsReady]} {test-bbb [2:NsReady]} {test-ccc []}", ts.rost.TestString())

	c3PAR := ts.nodes.Get("test-ccc").AddBarrier(ts.T(), 3, state.NsPreparing)
	tickWait(ts.orch, c3PAR)
	if rpcs := ts.nodes.RPCs(); ts.Equal([]string{"test-ccc"}, nIDs(rpcs)) {
		if ccc := rpcs["test-ccc"]; ts.Len(ccc, 1) {
			ProtoEqual(ts.T(), &pb.GiveRequest{
				Range: &pb.RangeMeta{
					Ident: 3,
				},
				Parents: []*pb.Parent{
					{
						Range: &pb.RangeMeta{
							Ident: 3,
						},
						Placements: []*pb.Placement{
							{
								Node:  "host-ccc:1",
								State: pb.PlacementState_PS_PENDING,
							},
						},
					},
					{
						Range: &pb.RangeMeta{
							Ident: 1,
							End:   []byte("ggg"),
						},
						Placements: []*pb.Placement{
							{
								Node:  "host-aaa:1",
								State: pb.PlacementState_PS_READY,
							},
						},
					},
					{
						Range: &pb.RangeMeta{
							Ident: 2,
							Start: []byte("ggg"),
						},
						Placements: []*pb.Placement{
							{
								Node:  "host-bbb:1",
								State: pb.PlacementState_PS_READY,
							},
						},
					},
				},
			}, ccc[0])
		}
	}

	ts.Equal("{1 [-inf, ggg] RsSubsuming p0=test-aaa:PsReady} {2 (ggg, +inf] RsSubsuming p0=test-bbb:PsReady} {3 [-inf, +inf] RsActive p0=test-ccc:PsPending}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsReady]} {test-bbb [2:NsReady]} {test-ccc [3:NsPreparing]}", ts.rost.TestString())

	// 2. New range finishes preparing.

	c3PAR.Release()
	ts.rost.Tick()
	ts.Equal("{test-aaa [1:NsReady]} {test-bbb [2:NsReady]} {test-ccc [3:NsPrepared]}", ts.rost.TestString())

	// 3. Controller takes the ranges from the source nodes.

	tickWait(ts.orch)
	ts.Empty(ts.nodes.RPCs())
	ts.Equal("{1 [-inf, ggg] RsSubsuming p0=test-aaa:PsReady} {2 (ggg, +inf] RsSubsuming p0=test-bbb:PsReady} {3 [-inf, +inf] RsActive p0=test-ccc:PsPrepared}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsReady]} {test-bbb [2:NsReady]} {test-ccc [3:NsPrepared]}", ts.rost.TestString())

	a1PDR := ts.nodes.Get("test-aaa").AddBarrier(ts.T(), 1, state.NsTaking)
	b2PDR := ts.nodes.Get("test-bbb").AddBarrier(ts.T(), 2, state.NsTaking)
	tickWait(ts.orch, a1PDR, b2PDR)
	if rpcs := ts.nodes.RPCs(); ts.Equal([]string{"test-aaa", "test-bbb"}, nIDs(rpcs)) {
		if aaa := rpcs["test-aaa"]; ts.Len(aaa, 1) {
			ProtoEqual(ts.T(), &pb.TakeRequest{Range: 1}, aaa[0])
		}
		if bbb := rpcs["test-bbb"]; ts.Len(bbb, 1) {
			ProtoEqual(ts.T(), &pb.TakeRequest{Range: 2}, bbb[0])
		}
	}

	ts.Equal("{1 [-inf, ggg] RsSubsuming p0=test-aaa:PsReady} {2 (ggg, +inf] RsSubsuming p0=test-bbb:PsReady} {3 [-inf, +inf] RsActive p0=test-ccc:PsPrepared}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsTaking]} {test-bbb [2:NsTaking]} {test-ccc [3:NsPrepared]}", ts.rost.TestString())

	// 4. Old ranges becomes taken.

	a1PDR.Release()
	b2PDR.Release()
	ts.rost.Tick()
	ts.Equal("{test-aaa [1:NsTaken]} {test-bbb [2:NsTaken]} {test-ccc [3:NsPrepared]}", ts.rost.TestString())

	c3AR := ts.nodes.Get("test-ccc").AddBarrier(ts.T(), 3, state.NsReadying)
	tickWait(ts.orch)
	c3AR.Wait()
	if rpcs := ts.nodes.RPCs(); ts.Equal([]string{"test-ccc"}, nIDs(rpcs)) {
		if ccc := rpcs["test-ccc"]; ts.Len(ccc, 1) {
			ProtoEqual(ts.T(), &pb.ServeRequest{Range: 3}, ccc[0])
		}
	}

	ts.Equal("{1 [-inf, ggg] RsSubsuming p0=test-aaa:PsTaken} {2 (ggg, +inf] RsSubsuming p0=test-bbb:PsTaken} {3 [-inf, +inf] RsActive p0=test-ccc:PsPrepared}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsTaken]} {test-bbb [2:NsTaken]} {test-ccc [3:NsReadying]}", ts.rost.TestString())

	// 5. New range becomes ready.

	c3AR.Release()
	ts.rost.Tick()
	ts.Equal("{test-aaa [1:NsTaken]} {test-bbb [2:NsTaken]} {test-ccc [3:NsReady]}", ts.rost.TestString())

	tickWait(ts.orch)
	ts.Empty(ts.nodes.RPCs())
	ts.Equal("{1 [-inf, ggg] RsSubsuming p0=test-aaa:PsTaken} {2 (ggg, +inf] RsSubsuming p0=test-bbb:PsTaken} {3 [-inf, +inf] RsActive p0=test-ccc:PsReady}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsTaken]} {test-bbb [2:NsTaken]} {test-ccc [3:NsReady]}", ts.rost.TestString())

	a1DR := ts.nodes.Get("test-aaa").AddBarrier(ts.T(), 1, state.NsDropping)
	b2DR := ts.nodes.Get("test-bbb").AddBarrier(ts.T(), 2, state.NsDropping)
	tickWait(ts.orch, a1DR, b2DR)
	if rpcs := ts.nodes.RPCs(); ts.Equal([]string{"test-aaa", "test-bbb"}, nIDs(rpcs)) {
		if aaa := rpcs["test-aaa"]; ts.Len(aaa, 1) {
			ProtoEqual(ts.T(), &pb.DropRequest{Range: 1}, aaa[0])
		}
		if bbb := rpcs["test-bbb"]; ts.Len(bbb, 1) {
			ProtoEqual(ts.T(), &pb.DropRequest{Range: 2}, bbb[0])
		}
	}

	ts.Equal("{1 [-inf, ggg] RsSubsuming p0=test-aaa:PsTaken} {2 (ggg, +inf] RsSubsuming p0=test-bbb:PsTaken} {3 [-inf, +inf] RsActive p0=test-ccc:PsReady}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsDropping]} {test-bbb [2:NsDropping]} {test-ccc [3:NsReady]}", ts.rost.TestString())

	// Drops finish.
	a1DR.Release()
	b2DR.Release()
	ts.rost.Tick()
	ts.Equal("{test-aaa []} {test-bbb []} {test-ccc [3:NsReady]}", ts.rost.TestString())

	tickWait(ts.orch)
	ts.Empty(ts.nodes.RPCs())
	ts.Equal("{1 [-inf, ggg] RsSubsuming p0=test-aaa:PsDropped} {2 (ggg, +inf] RsSubsuming p0=test-bbb:PsDropped} {3 [-inf, +inf] RsActive p0=test-ccc:PsReady}", ts.ks.LogString())
	ts.Equal("{test-aaa []} {test-bbb []} {test-ccc [3:NsReady]}", ts.rost.TestString())

	tickWait(ts.orch)
	ts.Empty(ts.nodes.RPCs())
	ts.Equal("{1 [-inf, ggg] RsSubsuming} {2 (ggg, +inf] RsSubsuming} {3 [-inf, +inf] RsActive p0=test-ccc:PsReady}", ts.ks.LogString())
	ts.Equal("{test-aaa []} {test-bbb []} {test-ccc [3:NsReady]}", ts.rost.TestString())

	tickWait(ts.orch)
	ts.Empty(ts.nodes.RPCs())
	ts.Equal("{1 [-inf, ggg] RsObsolete} {2 (ggg, +inf] RsObsolete} {3 [-inf, +inf] RsActive p0=test-ccc:PsReady}", ts.ks.LogString())
	ts.Equal("{test-aaa []} {test-bbb []} {test-ccc [3:NsReady]}", ts.rost.TestString())

	requireStable(ts.T(), ts.orch)

	// Assert that the error chan was closed, to indicate op is complete.
	select {
	case err, ok := <-op.Err:
		if ok {
			ts.NoError(err)
		}
	default:
		ts.Fail("expected op.Err to be closed")
	}
}

func (ts *OrchestratorSuite) TestSlowRPC() {

	// One node, one range, unplaced.

	na := remoteFactory("aaa")
	ts.Init(keyspaceFactory(ts.T(), ts.cfg, nil))
	nodeFactory(ts.ctx, ts.nodes, nodeStub{na, nil})
	ts.nodes.Get(na.Ident).SetGracePeriod(3 * time.Second)

	ts.rost.Tick()
	ts.Equal("{1 [-inf, +inf] RsActive}", ts.ks.LogString())
	ts.Equal("{test-aaa []}", ts.rost.TestString())

	// -------------------------------------------------------------------------

	// No WaitRPCs() this time! But we do stil have to wait until our one
	// expected RPC reaches the barrier (via PrepareAddRange). Otherwise, Tick
	// without WaitRPCs will likely return before the RPC has even started, let
	// alone gotten to the barrier.
	par := ts.nodes.Get("test-aaa").AddBarrier(ts.T(), 1, state.NsPreparing)
	ts.orch.Tick()
	par.Wait()

	if rpcs := ts.nodes.RPCs(); ts.Equal([]string{"test-aaa"}, nIDs(rpcs)) {
		if aaa := rpcs["test-aaa"]; ts.Len(aaa, 1) {
			if ts.IsType(&pb.GiveRequest{}, aaa[0]) {
				ProtoEqual(ts.T(), &pb.RangeMeta{
					Ident: 1,
					Start: []byte(ranje.ZeroKey),
					End:   []byte(ranje.ZeroKey),
				}, aaa[0].(*pb.GiveRequest).Range)
			}
		}
	}

	// Placement has been moved into PsPending, because we ticked past it...
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsPending}", ts.ks.LogString())

	// But the roster hasn't been updated yet, because the RPC hasn't completed.
	ts.Equal("{test-aaa []}", ts.rost.TestString())

	// RPC above is still in flight when the next Tick starts!
	// (This is quite unusual for tests.)
	ts.orch.Tick()

	// No redundant RPC this time.
	ts.Empty(ts.nodes.RPCs())

	// No state change; nothing happened.
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsPending}", ts.ks.LogString())
	ts.Equal("{test-aaa []}", ts.rost.TestString())

	// Give RPC finally completes! Roster is updated.
	par.Release()
	ts.orch.WaitRPCs()
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsPending}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsPrepared]}", ts.rost.TestString())

	// Subsequent ticks continue the placement as usual. No need to verify the
	// details in this test.
	tickUntilStable(ts)
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsReady}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsReady]}", ts.rost.TestString())
}

func TestOrchestratorSuite(t *testing.T) {
	suite.Run(t, new(OrchestratorSuite))
}

// -----------------------------------------------------------------------------

func initTestPlacement(ts *OrchestratorSuite) {
	na := remoteFactory("aaa")
	ts.Init(keyspaceFactory(ts.T(), ts.cfg, nil))
	nodeFactory(ts.ctx, ts.nodes, nodeStub{na, nil})

	ts.rost.Tick()
	ts.Equal("{1 [-inf, +inf] RsActive}", ts.ks.LogString())
	ts.Equal("{test-aaa []}", ts.rost.TestString())
}

// initTestMove spawns two hosts (aaa, bbb), one range (1), and one placement
// (range 1 is on aaa in PsReady), and returns the range.
func initTestMove(ts *OrchestratorSuite, strict bool) *ranje.Range {
	na, nb := remoteFactoryTwo("aaa", "bbb")
	//ks := keyspaceFactory(ts.T(), ts.cfg, []rangeStub{{"", na.Ident}})
	ks := keyspaceFactory(ts.T(), ts.cfg, nil)
	r1 := mustGetRange(ts.T(), ks, 1)
	ts.Init(ks)

	nodeFactory(ts.ctx, ts.nodes, nodeStub{na, []ranje.Meta{r1.Meta}}, nodeStub{nb, nil})
	ts.nodes.SetStrictTransitions(strict)

	ts.rost.Tick()
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsReady}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsReady]} {test-bbb []}", ts.rost.TestString())

	return r1
}

func initTestSplit(ts *OrchestratorSuite, strict bool) *ranje.Range {

	// Nodes
	na := remoteFactory("aaa")

	// Controller-side
	// TODO: Test that controller-side placements are repaired when a node
	//       shows up claiming to have a (valid) placement.
	//ks := keyspaceFactory(ts.T(), ts.cfg, []rangeStub{{"", na.Ident}})
	ks := keyspaceFactory(ts.T(), ts.cfg, nil)
	r1 := mustGetRange(ts.T(), ks, 1)
	ts.Init(ks)

	// Nodes-side
	nodeFactory(ts.ctx, ts.nodes, nodeStub{na, []ranje.Meta{r1.Meta}})
	ts.nodes.SetStrictTransitions(strict)

	// Probe the fake nodes.
	ts.rost.Tick()
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsReady}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsReady]}", ts.rost.TestString())

	tickWait(ts.orch)
	ts.Empty(ts.nodes.RPCs())
	ts.Require().Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsReady}", ts.ks.LogString())

	return r1
}

// initTestJoin sets up three hosts (aaa, bbb, ccc), two ranges (1, 2) split at
// ggg, and two placements (r1 on aaa, r2 on bbb; both in PsReady), and returns
// the two ranges.
func initTestJoin(ts *OrchestratorSuite, strict bool) (*ranje.Range, *ranje.Range) {

	// Start with two ranges (which together cover the whole keyspace) assigned
	// to two of three nodes. The ranges will be joined onto the third node.

	na, nb, nc := remoteFactoryThree("aaa", "bbb", "ccc")
	//ks := keyspaceFactory(ts.T(), ts.cfg, []rangeStub{{"", na.Ident}, {"ggg", nb.Ident}})
	ks := keyspaceFactory(ts.T(), ts.cfg, nil)
	r1 := mustGetRange(ts.T(), ks, 1)
	r2 := mustGetRange(ts.T(), ks, 2)
	ts.Init(ks)

	nodeFactory(ts.ctx, ts.nodes, nodeStub{na, []ranje.Meta{r1.Meta}}, nodeStub{nb, []ranje.Meta{r2.Meta}}, nodeStub{nc, nil})
	ts.nodes.SetStrictTransitions(strict)

	// Probe the fake nodes to verify the setup.

	ts.rost.Tick()
	ts.Equal("{1 [-inf, ggg] RsActive p0=test-aaa:PsReady} {2 (ggg, +inf] RsActive p0=test-bbb:PsReady}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsReady]} {test-bbb [2:NsReady]} {test-ccc []}", ts.rost.TestString())

	return r1, r2
}

type nodeStub struct {
	node discovery.Remote
	m    []ranje.Meta
}

type rangeStub struct {
	rID        string
	startKey   string
	endKey     string
	rState     string
	placements []placementStub
}

type placementStub struct {
	nodeID string
	pState string
}

func parseKeyspace(t *testing.T, keyspace string) []rangeStub {

	// keyspace = "{1 [-inf, ggg] RsActive p0=test-aaa:PsReady} {2 (ggg, +inf] RsActive p0=test-bbb:PsReady}"
	// roster = "{test-aaa [1:NsReady]} {test-bbb [2:NsReady]} {test-ccc []}"

	r := regexp.MustCompile(`{[^{}]*}`)
	x := r.FindAllString(keyspace, -1)

	sr := make([]rangeStub, len(x))
	for i := range x {
		fmt.Printf("x[%d]: %s\n", i, x[i])

		// {1 [-inf, ggg] RsActive p0=test-aaa:PsReady p1=test-bbb:PsReady} {2 (ggg, +inf] RsActive}
		//                       {    1                [          -inf            ,      ggg             ]                RsActive    (placements)   }
		r = regexp.MustCompile(`^{` + `(\d+)` + ` ` + `[\[\(]` + `([\+\-\w]+)` + `, ` + `([\+\-\w]+)` + `[\]\)]` + ` ` + `(Rs\w+)` + `(?: (.+))?` + `}$`)
		y := r.FindStringSubmatch(x[i])
		if y == nil {
			t.Fatalf("invalid range string: %v", x[i])
		}

		sr[i] = rangeStub{
			rID:      y[1],
			startKey: y[2],
			endKey:   y[3],
			rState:   y[4],
		}

		placements := y[5]
		if placements != "" {
			pl := strings.Split(placements, ` `)
			sp := make([]placementStub, len(pl))
			for ii := range pl {
				// p0=test-aaa:PsReady
				//                            p0         =     test-aaa :     PsReady
				r = regexp.MustCompile(`^` + `p(\d+)` + `=` + `(.+)` + `:` + `(Ps\w+)` + `$`)
				z := r.FindStringSubmatch(pl[ii])
				if z == nil {
					t.Fatalf("invalid placement string: %v", pl[ii])
				}

				// TODO: Check that indices are contiguous?

				sp[ii] = placementStub{nodeID: z[2], pState: z[3]}
			}

			sr[i].placements = sp
		}
	}

	return sr
}

func TestParseKeyspace(t *testing.T) {
	ksStr := "{1 [-inf, ggg] RsActive p0=test-aaa:PsReady p1=test-bbb:PsReady} {2 (ggg, +inf] RsActive}"
	ks := keyspaceFactory(t, config.Config{}, parseKeyspace(t, ksStr))
	assert.Equal(t, ksStr, ks.LogString())
}

type nodePlacementStub struct {
	rID    int
	nState string
}

type nodeStub2 struct {
	nodeID     string
	placements []nodePlacementStub
}

func parseRoster(t *testing.T, s string) []nodeStub2 {

	// {aa} {bb}
	r1 := regexp.MustCompile(`{[^{}]*}`)

	// {test-aaa [1:NsReady 2:NsReady]}
	//                         {     test-aaa            [1:NsReady 2:NsReady]}
	r2 := regexp.MustCompile(`^{` + `([\w\-]+)` + ` ` + `\[(.*)\]}$`)

	// 1:NsReady
	r3 := regexp.MustCompile(`^` + `(\d+)` + `:` + `(Ns\w+)` + `$`)

	x := r1.FindAllString(s, -1)

	ns := make([]nodeStub2, len(x))
	for i := range x {
		fmt.Printf("x[%d]: %s\n", i, x[i])

		y := r2.FindStringSubmatch(x[i])
		if y == nil {
			t.Fatalf("invalid node string: %v", x[i])
		}

		ns[i] = nodeStub2{
			nodeID:     y[1],
			placements: []nodePlacementStub{},
		}

		placements := y[2]
		if placements != "" {
			pl := strings.Split(placements, ` `)
			nps := make([]nodePlacementStub, len(pl))
			for ii := range pl {
				z := r3.FindStringSubmatch(pl[ii])
				if z == nil {
					t.Fatalf("invalid placement string: %v", pl[ii])
				}
				rID, err := strconv.Atoi(z[1])
				if err != nil {
					t.Fatalf("invalid parsing range ID: %v", err)
				}
				nps[ii] = nodePlacementStub{
					rID:    rID,
					nState: z[2],
				}
			}
			ns[i].placements = nps
		}
	}

	return ns
}

func nodesFactory2(t *testing.T, cfg config.Config, ctx context.Context, ks *keyspace.Keyspace, stubs []nodeStub2) (*fake_nodes.TestNodes, *roster.Roster) {
	nodes := fake_nodes.NewTestNodes()
	rost := roster.New(cfg, nodes.Discovery(), nil, nil, nil)

	for i := range stubs {

		rem := discovery.Remote{
			Ident: stubs[i].nodeID,
			Host:  fmt.Sprintf("host-%s", stubs[i].nodeID),
			Port:  1,
		}

		ri := map[ranje.Ident]*info.RangeInfo{}
		for _, pStub := range stubs[i].placements {

			rID := ranje.Ident(pStub.rID)
			r, err := ks.Get(rID)
			if err != nil {
				t.Fatalf("invalid node placement stub: %v", err)
			}

			ri[rID] = &info.RangeInfo{
				Meta:  r.Meta,
				State: RemoteStateFromString(t, pStub.nState),
			}
		}

		nodes.Add(ctx, rem, ri)
	}

	rost.NodeConnFactory = nodes.NodeConnFactory
	return nodes, rost
}

func orchFactory(t *testing.T, sKS, sRos string, cfg config.Config) (*Orchestrator, *fake_nodes.TestNodes) {
	ks := keyspaceFactory(t, cfg, parseKeyspace(t, sKS))
	nodes, ros := nodesFactory2(t, cfg, context.TODO(), ks, parseRoster(t, sRos))
	srv := grpc.NewServer() // TODO: Allow this to be nil.
	orch := New(cfg, ks, ros, srv)
	return orch, nodes
}

func TestParseRoster(t *testing.T) {
	ksStr := "{1 [-inf, ggg] RsActive p0=test-aaa:PsReady p1=test-bbb:PsReady} {2 (ggg, +inf] RsActive}"
	ks := keyspaceFactory(t, config.Config{}, parseKeyspace(t, ksStr))
	assert.Equal(t, ksStr, ks.LogString())

	rosStr := "{test-aaa [1:NsReady 2:NsReady]} {test-bbb []} {test-ccc []}"
	fmt.Printf("%v\n", parseRoster(t, rosStr))
	_, ros := nodesFactory2(t, testConfig(), context.TODO(), ks, parseRoster(t, rosStr))

	ros.Tick()
	assert.Equal(t, rosStr, ros.TestString())
}

func TestOrchFactory(t *testing.T) {
	ksStr := "{1 [-inf, ggg] RsActive p0=test-aaa:PsReady p1=test-bbb:PsReady} {2 (ggg, +inf] RsActive}"
	rosStr := "{test-aaa [1:NsReady 2:NsReady]} {test-bbb []} {test-ccc []}"
	orch, _ := orchFactory(t, ksStr, rosStr, testConfig())
	orch.rost.Tick()

	assert.Equal(t, ksStr, orch.ks.LogString())
	assert.Equal(t, rosStr, orch.rost.TestString())
}

func nodeFactory(ctx context.Context, nodes *fake_nodes.TestNodes, stubs ...nodeStub) {
	for i := range stubs {

		ri := map[ranje.Ident]*info.RangeInfo{}
		for _, m := range stubs[i].m {
			ri[m.Ident] = &info.RangeInfo{
				Meta:  m,
				State: state.NsReady,
			}
		}

		nodes.Add(ctx, stubs[i].node, ri)
	}
}

func remoteFactory(suffix string) discovery.Remote {
	return discovery.Remote{
		Ident: fmt.Sprintf("test-%s", suffix),
		Host:  fmt.Sprintf("host-%s", suffix),
		Port:  1,
	}
}

func remoteFactoryTwo(s1, s2 string) (discovery.Remote, discovery.Remote) {
	return remoteFactory(s1), remoteFactory(s2)
}

func remoteFactoryThree(s1, s2, s3 string) (discovery.Remote, discovery.Remote, discovery.Remote) {
	return remoteFactory(s1), remoteFactory(s2), remoteFactory(s3)
}

func PlacementStateFromString(t *testing.T, s string) ranje.PlacementState {
	switch s {
	case ranje.PsUnknown.String():
		return ranje.PsUnknown

	case ranje.PsPending.String():
		return ranje.PsPending

	case ranje.PsPrepared.String():
		return ranje.PsPrepared

	case ranje.PsReady.String():
		return ranje.PsReady

	case ranje.PsTaken.String():
		return ranje.PsTaken

	case ranje.PsGiveUp.String():
		return ranje.PsGiveUp

	case ranje.PsDropped.String():
		return ranje.PsDropped
	}

	t.Fatalf("invalid PlacementState string: %s", s)
	return ranje.PsUnknown // unreachable
}

func RemoteStateFromString(t *testing.T, s string) state.RemoteState {
	switch s {
	case "NsUnknown":
		return state.NsUnknown
	case "NsPreparing":
		return state.NsPreparing
	case "NsPreparingError":
		return state.NsPreparingError
	case "NsPrepared":
		return state.NsPrepared
	case "NsReadying":
		return state.NsReadying
	case "NsReadyingError":
		return state.NsReadyingError
	case "NsReady":
		return state.NsReady
	case "NsTaking":
		return state.NsTaking
	case "NsTakingError":
		return state.NsTakingError
	case "NsTaken":
		return state.NsTaken
	case "NsDropping":
		return state.NsDropping
	case "NsDroppingError":
		return state.NsDroppingError
	case "NsNotFound":
		return state.NsNotFound
	}

	t.Fatalf("invalid PlacementState string: %s", s)
	return state.NsUnknown // unreachable
}

// TODO: Remove config param. Config was a mistake.
func keyspaceFactory(t *testing.T, cfg config.Config, stubs []rangeStub) *keyspace.Keyspace {
	ranges := make([]*ranje.Range, len(stubs))
	for i := range stubs {
		r := ranje.NewRange(ranje.Ident(i + 1))
		r.State = ranje.RsActive

		if i > 0 {
			r.Meta.Start = ranje.Key(stubs[i].startKey)
			ranges[i-1].Meta.End = ranje.Key(stubs[i].startKey)
		}

		r.Placements = make([]*ranje.Placement, len(stubs[i].placements))

		for ii := range stubs[i].placements {
			pstub := stubs[i].placements[ii]
			r.Placements[ii] = &ranje.Placement{
				NodeID: pstub.nodeID,
				State:  PlacementStateFromString(t, pstub.pState),
			}
		}

		ranges[i] = r
	}

	pers := &FakePersister{ranges: ranges}

	var err error
	ks, err := keyspace.New(cfg, pers)
	if err != nil {
		t.Fatalf("keyspace.New: %s", err)
	}

	return ks
}

// mustGetRange returns a range from the given keyspace or fails the test.
func mustGetRange(t *testing.T, ks *keyspace.Keyspace, rID int) *ranje.Range {
	r, err := ks.Get(ranje.Ident(rID))
	if err != nil {
		t.Fatalf("ks.Get(%d): %v", rID, err)
	}
	return r
}

// -----------------------------------------------------------------------------

type FakePersister struct {
	ranges []*ranje.Range
	called bool
}

func (fp *FakePersister) GetRanges() ([]*ranje.Range, error) {
	if fp.called {
		// PutRanges is not implemented, so this doesn't make sense.
		panic("FakePersister.GetRanges called more than once")
	}
	fp.called = true

	if fp.ranges != nil {
		return fp.ranges, nil
	}

	return []*ranje.Range{}, nil
}

func (fp *FakePersister) PutRanges([]*ranje.Range) error {
	return nil
}
