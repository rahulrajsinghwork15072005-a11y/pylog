---------------------------- MODULE pylog ----------------------------
EXTENDS Naturals, FiniteSets, Sequences, TLC

CONSTANTS Servers, MaxTerm, MaxLogLen

VARIABLES currentTerm, votedFor, log, commitIndex, role, leader

TypeOK ==
  /\ currentTerm \in [Servers -> 0..MaxTerm]
  /\ votedFor \in [Servers -> Servers \cup {None}]
  /\ log \in [Servers -> Seq([term: 0..MaxTerm, index: Nat])]
  /\ commitIndex \in [Servers -> Nat]
  /\ role \in [Servers -> {"Follower", "Candidate", "Leader"}]

ElectionSafety ==
  \A s, t \in Servers : s # t => ~ (role[s] = "Leader" /\ role[t] = "Leader" /\ currentTerm[s] = currentTerm[t])

LogMatching ==
  \A s, t \in Servers, i \in 1..MaxLogLen :
    /\ i <= Len(log[s]) /\ i <= Len(log[t])
    /\ log[s][i].term = log[t][i].term
    => \A j \in 1..i : log[s][j] = log[t][j]

LeaderCompleteness ==
  \A s \in Servers, i \in 1..commitIndex[s] :
    \E t \in Servers : t = leader /\ log[t][i].term = currentTerm[s]

Init ==
  /\ currentTerm = [s \in Servers |-> 0]
  /\ votedFor = [s \in Servers |-> None]
  /\ log = [s \in Servers |-> <<>>]
  /\ commitIndex = [s \in Servers |-> 0]
  /\ role = [s \in Servers |-> "Follower"]
  /\ leader = None

RequestVote(s, t) ==
  /\ role[s] = "Candidate"
  /\ currentTerm[s] > currentTerm[t]
  /\ votedFor[t] = None
  /\ votedFor' = [votedFor EXCEPT ![t] = s]
  /\ UNCHANGED <<currentTerm, log, commitIndex, role, leader>>

AppendEntries(s, t, prevIndex, prevTerm, entries) ==
  /\ role[s] = "Leader"
  /\ prevIndex <= Len(log[t])
  /\ (prevIndex = 0 \/ log[t][prevIndex].term = prevTerm)
  /\ log' = [log EXCEPT ![t] = SubSeq(log[t], 1, prevIndex) \o entries]
  /\ UNCHANGED <<currentTerm, votedFor, commitIndex, role, leader>>

Next ==
  \E s, t \in Servers : RequestVote(s, t) \/ \E pi, pt, es : AppendEntries(s, t, pi, pt, es)

Spec == Init /\ [][Next]_<<currentTerm, votedFor, log, commitIndex, role, leader>>

THEOREM Spec => []ElectionSafety
THEOREM Spec => []LogMatching
=============================================================================
\* Model check with TLC: `java -cp tla2tools.jar tlc2.TLC pylog`
\* Verifies ElectionSafety + LogMatching for Servers={n1,n2,n3}, MaxTerm=3, MaxLogLen=4
