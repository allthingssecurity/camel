---------------------------- MODULE R1_IdempotentFileHttp ----------------------------
(* Route (camel-quarkus-examples/jpa-idempotent-repository, JpaIdempotentRoute.java):
     from("file:target/input-files")
       .idempotentConsumer(simple("${body}")).idempotentRepository("jpaIdempotentRepository")
       .toF("http://localhost:%s/costly-api-call", port);
   Component semantics modelled (Camel origin/main):
   - file consumer: one poll thread; on exchange success the file is moved to .camel (commit);
     on failure the file is left in place and picked up again (rollback).
   - IdempotentConsumer: eager=true (default) adds the key BEFORE processing; removeOnFailure=true
     removes it when the exchange fails; a duplicate is skipped and the exchange completes OK.
     eager=false checks contains() before and adds the key on completion.
   - JpaMessageIdRepository.add runs in its own transaction (persisted immediately).
   - crash = JVM dies: no onCompletion/onFailure runs; in-memory state lost.          *)
EXTENDS Naturals, FiniteSets

CONSTANTS Eager, PersistentRepo, MaxCrashes, MaxFailures

\* two input files with the same body (a duplicate) and one with a different body
Files == {"f1", "f2", "f3"}
Body == [f \in Files |-> IF f = "f3" THEN "B" ELSE "A"]

VARIABLES inbox,      \* files still in the input folder
          repo,       \* keys in the idempotent repository
          cur,        \* file being processed, or "none"
          st,         \* "idle" | "keyed" (passed the idempotent check) | "called" (HTTP done)
          calls,      \* number of HTTP calls per body
          up, crashes, failures

vars == <<inbox, repo, cur, st, calls, up, crashes, failures>>
Bodies == {Body[f] : f \in Files}

Init == /\ inbox = Files /\ repo = {} /\ cur = "none" /\ st = "idle"
        /\ calls = [b \in Bodies |-> 0] /\ up = TRUE /\ crashes = 0 /\ failures = 0

\* file consumer polls f and the idempotent consumer checks the key
Poll(f) == /\ up /\ st = "idle" /\ f \in inbox
           /\ IF Body[f] \in repo
                THEN \* duplicate: skipped, exchange completes OK, file is moved away
                     /\ inbox' = inbox \ {f} /\ UNCHANGED <<repo, cur, st>>
                ELSE /\ repo' = IF Eager THEN repo \cup {Body[f]} ELSE repo
                     /\ cur' = f /\ st' = "keyed" /\ UNCHANGED inbox
           /\ UNCHANGED <<calls, up, crashes, failures>>

CallOk == /\ up /\ st = "keyed"
          /\ calls' = [calls EXCEPT ![Body[cur]] = @ + 1] /\ st' = "called"
          /\ UNCHANGED <<inbox, repo, cur, up, crashes, failures>>

\* HTTP call fails -> exchange fails: removeOnFailure, file rollback (stays in folder)
CallFails == /\ up /\ st = "keyed" /\ failures < MaxFailures
             /\ repo' = IF Eager THEN repo \ {Body[cur]} ELSE repo
             /\ cur' = "none" /\ st' = "idle" /\ failures' = failures + 1
             /\ UNCHANGED <<inbox, calls, up, crashes>>

\* exchange completes: non-eager adds the key now, file consumer moves the file to .camel
Complete == /\ up /\ st = "called"
            /\ repo' = repo \cup {Body[cur]}
            /\ inbox' = inbox \ {cur} /\ cur' = "none" /\ st' = "idle"
            /\ UNCHANGED <<calls, up, crashes, failures>>

Crash == /\ up /\ crashes < MaxCrashes
         /\ up' = FALSE /\ crashes' = crashes + 1 /\ cur' = "none" /\ st' = "idle"
         /\ repo' = IF PersistentRepo THEN repo ELSE {}
         /\ UNCHANGED <<inbox, calls, failures>>

Restart == /\ ~up /\ up' = TRUE /\ UNCHANGED <<inbox, repo, cur, st, calls, crashes, failures>>

Next == \/ \E f \in Files : Poll(f)
        \/ CallOk \/ CallFails \/ Complete \/ Crash \/ Restart

Spec == Init /\ [][Next]_vars

\* ---- properties ----
AtRest == up /\ inbox = {} /\ st = "idle"
\* every distinct body that arrived was sent to the API at least once
NoLoss == AtRest => \A b \in Bodies : calls[b] >= 1
\* the costly API is never called twice for the same body
NoDuplicateEffect == \A b \in Bodies : calls[b] <= 1
=======================================================================================
