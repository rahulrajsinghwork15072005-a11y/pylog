# Devlog

Notes from a focused correctness audit of the Raft layer. Theme: the paper
is six pages; the devil is in six lines.

## PreVote wanted unanimous consent

Found while writing an election-liveness test: crash one of three nodes
*before* any election has ever happened, and the two survivors should still
elect a leader. They did - but only after burning a full extra election
timeout every single time.

The PreVote response handler counted votes like this:

    if len(self.votes) > (len(self.peers) + 1) // 2:

which looks right until you notice `votes` never contained the candidate
itself. So on a healthy 3-node cluster, "majority" required *both* peers to
grant - unanimity among peers, not majority of the cluster. With any node
down, that bar is unreachable; every election degraded to the fallback path
(prevote times out -> start real election anyway), doubling election latency
exactly when the cluster is already degraded.

Fix: seed `votes = {self.id}` when starting a prevote, matching how real
elections count themselves. Regression test drives a node with stubbed
transport: exactly one grant out of two peers must flip it straight into
CANDIDATE state.

## transfer_leadership could wedge writes forever

`transfer_leadership(target)` sets `transfer_target` and stops accepting
proposals (correct - don't take new writes mid-handover). It also stopped
sending AppendEntries to the target (incorrect - the target can only take
over once its log catches up). And nothing ever cleared the target.

So: call `transfer_leadership("n1")` while n1 is crashed, and the leader
refuses all client writes indefinitely. Verified in the simulator: 2000ms
of virtual time later, `transfer_target` still set, proposals still dead.

Two-part fix:

1. keep replicating to the target during the handover (it needs the entries)
2. arm a deadline of 2x election timeout; `tick()` aborts the transfer when
   it passes, clearing the target so the old leader resumes proposing

Regression test runs the exact scenario in the simulator: elect, crash a
follower, transfer to it, advance past the deadline, assert the leader
resumes committing.

## What the audit checked and found sound

- commit-index advancement respects the current-term-only rule (paper 5.4.2),
  with the leader's NOOP trick to make it kick in immediately after election
- AppendEntries conflict back-off returns the first index of the conflicting
  term, not just index-1 (the optimization from the Raft dissertation)
- vote/append handlers persist term changes before acting on them
- snapshot install handles the already-have case and resyncs disk truncation
- CheckQuorum steps down at <half-alive, matching the lease read heuristic

## Meta-lesson

Every bug here was found by pointing a simulator at a specific "what if the
world is degraded?" question, not by reading code. The virtual-clock harness
made each experiment deterministic and cheap. Next time: write the hostile-
scenario probes first, then read the code with their answers in hand.
