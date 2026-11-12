------------------------ MODULE R5_AggregateDistOptimistic ------------------------
(* Route (camel-examples/aggregate-dist, Application.java): several Camel instances (THREADS=5) share
   one H2 database:
     from("direct:aggregator").filter(body().isNotNull())
       .aggregate().header("corrId").aggregationStrategy(concat).completionPredicate(all 1..END present)
       .aggregationRepository(JdbcAggregationRepository(table "aggregation", useRecovery=false))
       .optimisticLocking()
   Semantics (Camel origin/main):
   - AggregateProcessor with optimisticLocking: get() the group (with its version), aggregate in memory,
     then repository.add(old,new) = compare-and-set on (id, version) (insert must find no row; update
     "WHERE id=? AND version=?"); on OptimisticLockingException retry (OptimisticLockRetryPolicy,
     maximumRetries=0 -> unlimited). On completion the group is removed with the same version check.
   - without optimisticLocking: the in-JVM lock serialises only one instance; the JDBC repository still
     detects the stale version and throws, but nothing retries: the exchange fails.          *)
EXTENDS Naturals, FiniteSets

CONSTANTS Optimistic
Items == {1, 2, 3}
Inst  == {"n1", "n2"}

VARIABLES queue,     \* items not yet taken by any instance (the shared inputQueue)
          row,       \* the stored group: [present |-> BOOLEAN, val |-> SUBSET Items, ver |-> Nat]
          pc, held, snap,   \* per instance: "idle"/"read", item being aggregated, snapshot read
          out,       \* completed aggregated results
          failed     \* items whose exchange failed
vars == <<queue, row, pc, held, snap, out, failed>>

NoRow == [present |-> FALSE, val |-> {}, ver |-> 0]
Init == /\ queue = Items /\ row = NoRow /\ pc = [n \in Inst |-> "idle"]
        /\ held = [n \in Inst |-> 0] /\ snap = [n \in Inst |-> NoRow] /\ out = {} /\ failed = {}

Take(n, x) == /\ pc[n] = "idle" /\ held[n] = 0 /\ x \in queue
              /\ queue' = queue \ {x} /\ held' = [held EXCEPT ![n] = x]
              /\ UNCHANGED <<row, pc, snap, out, failed>>

Read(n) == /\ pc[n] = "idle" /\ held[n] # 0
           /\ snap' = [snap EXCEPT ![n] = row] /\ pc' = [pc EXCEPT ![n] = "read"]
           /\ UNCHANGED <<queue, row, held, out, failed>>

\* CAS write (insert / update / completion-remove), all guarded by the version read
Write(n) == /\ pc[n] = "read"
            /\ LET s == snap[n]
                   new == s.val \cup {held[n]}
                   ok == row.present = s.present /\ (s.present => row.ver = s.ver)
               IN IF ok
                  THEN /\ IF new = Items
                            THEN /\ row' = NoRow /\ out' = out \cup {new}
                            ELSE /\ row' = [present |-> TRUE, val |-> new, ver |-> row.ver + 1]
                                 /\ UNCHANGED out
                       /\ held' = [held EXCEPT ![n] = 0] /\ UNCHANGED failed
                  ELSE IF Optimistic
                       THEN UNCHANGED <<row, out, held, failed>>           \* retry: read again
                       ELSE /\ failed' = failed \cup {held[n]}            \* exchange fails
                            /\ held' = [held EXCEPT ![n] = 0] /\ UNCHANGED <<row, out>>
            /\ pc' = [pc EXCEPT ![n] = "idle"]
            /\ UNCHANGED <<queue, snap>>

Next == \/ \E n \in Inst, x \in Items : Take(n, x)
        \/ \E n \in Inst : Read(n) \/ Write(n)
Spec == Init /\ [][Next]_vars

AtRest == queue = {} /\ \A n \in Inst : held[n] = 0
\* the group completes once, with every item
NoLostUpdate == AtRest => out = {Items}
==================================================================================
