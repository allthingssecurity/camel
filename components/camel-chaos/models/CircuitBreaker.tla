\* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
\* file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
\* to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
\* License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
\* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
\* an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
\* specific language governing permissions and limitations under the License.
\* Copy of tla/circuitbreaker/CircuitBreaker.tla (camel-lean workspace), checked there with TLC; see PROPOSAL.md section 8.
--------------------------- MODULE CircuitBreaker ---------------------------
(***************************************************************************)
(* camel-resilience4j ResilienceProcessor (main @ bd4d2ba20): the state    *)
(* Camel adds around resilience4j for a call with a TimeLimiter (timeout)  *)
(* and a fallback: the exchangeWriteGuard shared by the worker task and    *)
(* the fallback task.                                                      *)
(*                                                                         *)
(* Java anchors (components/camel-resilience4j/.../ResilienceProcessor):   *)
(*   processSync: guard shared by task and fallbackTask :584-586;          *)
(*     timeLimiter -> supplyAsync(task, executorService) :590-593;         *)
(*     callable.call() / on exception fallbackTask.apply(e) :606-610       *)
(*   processAsync: executorService.submit(processTaskAsync) :648-652,      *)
(*     TimeLimiter.decorateCompletionStage :661-663, whenComplete ->       *)
(*     fallback / successState :671-687                                    *)
(*   processTask (worker): processor.process(copy) :741, then              *)
(*     if guard.compareAndSet(false,true) write results to the ORIGINAL    *)
(*     exchange :744-752 (setException / copyResults / properties)         *)
(*   processTaskAsync: same CAS + write in the callback :805-813, then     *)
(*     future.complete :824-829                                            *)
(*   CircuitBreakerFallbackTask.apply: guard.set(true) (NOT a CAS) :932-935 *)
(*     then writes properties, clears the exception and runs the fallback  *)
(*     processor on the ORIGINAL exchange :936-1034                        *)
(* The TimeLimiter completes the caller's future with a TimeoutException   *)
(* at any time before the worker's future completes; the worker keeps      *)
(* running (a CompletableFuture is not interrupted).                       *)
(***************************************************************************)
EXTENDS Naturals, TLC

CONSTANTS
    FIX   \* candidate fix: the fallback also does guard.compareAndSet(false, true); if it loses, the worker has
          \* taken over the exchange and the caller waits for the worker's future instead of running the fallback

VARIABLES
    guard,      \* exchangeWriteGuard
    wpc,        \* worker: "run" | "cas" | "write" | "complete" | "done"
    cpc,        \* caller: "wait" | "fallback" | "fbwrite" | "joinWorker" | "success" | "done"
    fut,        \* the worker's future completed
    timedOut,   \* the TimeLimiter fired
    writes,     \* ghost: who wrote the original exchange: subset of {"worker", "fallback"}
    overlap     \* ghost: worker and fallback wrote the exchange at the same time

vars == <<guard, wpc, cpc, fut, timedOut, writes, overlap>>

Init ==
    /\ guard = FALSE /\ wpc = "run" /\ cpc = "wait" /\ fut = FALSE /\ timedOut = FALSE
    /\ writes = {} /\ overlap = FALSE

(* worker thread (executorService) *)
WRun ==
    /\ wpc = "run"
    /\ wpc' = "cas"     \* processor.process(copy) finished
    /\ UNCHANGED <<guard, cpc, fut, timedOut, writes, overlap>>

WCas ==
    /\ wpc = "cas"
    /\ IF ~guard THEN /\ guard' = TRUE /\ wpc' = "write"
                 ELSE /\ UNCHANGED guard /\ wpc' = "complete"
    /\ UNCHANGED <<cpc, fut, timedOut, writes, overlap>>

\* ExchangeHelper.copyResults(exchange, copy) + properties on the ORIGINAL exchange
WWrite ==
    /\ wpc = "write"
    /\ writes' = writes \cup {"worker"}
    /\ overlap' = (overlap \/ cpc = "fbwrite")
    /\ wpc' = "complete"
    /\ UNCHANGED <<guard, cpc, fut, timedOut>>

WComplete ==
    /\ wpc = "complete"
    /\ fut' = TRUE /\ wpc' = "done"
    /\ UNCHANGED <<guard, cpc, timedOut, writes, overlap>>

(* caller side: TimeLimiter + CircuitBreaker decorators *)
CSuccess ==
    /\ cpc = "wait" /\ fut /\ ~timedOut
    /\ cpc' = "done"
    /\ UNCHANGED <<guard, wpc, fut, timedOut, writes, overlap>>

CTimeout ==
    /\ cpc = "wait" /\ ~fut
    /\ timedOut' = TRUE /\ cpc' = "fallback"
    /\ UNCHANGED <<guard, wpc, fut, writes, overlap>>

\* CircuitBreakerFallbackTask.apply
CFallback ==
    /\ cpc = "fallback"
    /\ IF FIX
       THEN IF ~guard THEN /\ guard' = TRUE /\ cpc' = "fbwrite"
                      ELSE /\ UNCHANGED guard /\ cpc' = "joinWorker"
       ELSE /\ guard' = TRUE /\ cpc' = "fbwrite"
    /\ UNCHANGED <<wpc, fut, timedOut, writes, overlap>>

\* the fallback writes the exchange (properties, exception cleared, fallback processor)
CFbWrite ==
    /\ cpc = "fbwrite"
    /\ writes' = writes \cup {"fallback"}
    /\ overlap' = (overlap \/ wpc = "write")
    /\ cpc' = "done"
    /\ UNCHANGED <<guard, wpc, fut, timedOut>>

\* FIX: the worker won the guard: use its result
CJoin ==
    /\ cpc = "joinWorker" /\ fut
    /\ cpc' = "done"
    /\ UNCHANGED <<guard, wpc, fut, timedOut, writes, overlap>>

Next == WRun \/ WCas \/ WWrite \/ WComplete \/ CSuccess \/ CTimeout \/ CFallback \/ CFbWrite \/ CJoin
Done == cpc = "done" /\ wpc = "done"
Spec == Init /\ [][Next \/ (Done /\ UNCHANGED vars)]_vars /\ WF_vars(Next)

\* the original exchange is written by the worker OR by the fallback, never both
SingleWriter == ~({"worker", "fallback"} \subseteq writes)
NoConcurrentWrite == ~overlap
\* a timed-out call is answered by the fallback (or, with the fix, by the worker that won the race)
TimeoutAnswered == (Done /\ timedOut) => writes # {}
Termination == <>Done
W_Fallback == ~("fallback" \in writes)
=============================================================================
