-- Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
-- file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
-- to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
-- License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
-- Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
-- an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
-- specific language governing permissions and limitations under the License.
-- Copy of proofs/CamelLean/ChaosVerdict.lean (camel-lean workspace), where it is built with lake (Lean 4.34).
/-!
# camel-chaos prototype: the verdict logic and the rule counters

Transcription (by hand) of the parts of the camel-chaos prototype that decide PASS / FAIL and bound the faults a
rule injects. Java on branch `camel-chaos-prototype`, `components/camel-chaos/src/main/java/org/apache/camel/chaos/`:

* `ChaosManager.RouteStats.verdict` and `toJson` (the per-route verdict and result),
* `ChaosManager.Expectation.toJson` (lost messages and result),
* `ChaosManager.report` (the overall result: FAIL if any route, expectation or invariant fails, else RUNNING while a
  rule is active, else PASS),
* `ChaosRule.roll` (a fail/latency rule: `calls` bounds the matching calls it sees) and `ChaosRule.triggerStall`
  (a stall on a call fires once).

What is proved is a property of this transcription, not of the Java (no extraction); the Java tests
(`ChaosVerdictTest`, `ChaosSafetyTest`, `ChaosInvariantTest`) exercise the same cases. Counters are `Nat` (Java
`AtomicLong`, never negative here). Everything below is proved; no `sorry`.
-/

namespace Camel.ChaosVerdict

inductive Res where
  | pass | fail | running
deriving DecidableEq, Repr

/-- The inputs of `RouteStats.verdict` / `toJson`. -/
structure Route where
  faults : Nat             -- faultsInjected
  ruleActive : Bool        -- a rule that can affect this route is still active
  started : Bool           -- route status isStarted()
  completedAfter : Nat     -- completedAfterLastFault
  failed : Nat             -- failed unhandled, fire and forget (lost)
  failedToCaller : Nat     -- request/reply failures returned to the caller
deriving Repr

inductive Verdict where
  | noFaults | chaosActive | notRecovered | recoveredButLost | recoveredCaller | recovered
deriving DecidableEq, Repr

/-- `RouteStats.verdict` (the order of the tests is the order of the Java `if`s). -/
def verdict (r : Route) : Verdict :=
  if r.faults = 0 then .noFaults
  else if r.ruleActive then .chaosActive
  else if !r.started then .notRecovered
  else if r.completedAfter = 0 then .notRecovered
  else if r.failed > 0 then .recoveredButLost
  else if r.failedToCaller > 0 then .recoveredCaller
  else .recovered

/-- `RouteStats.toJson`: `result`. -/
def routeResult (r : Route) : Res :=
  match verdict r with
  | .chaosActive => .running
  | v => if r.failed > 0 || v == .notRecovered then .fail else .pass

/-- A route PASSes only if it lost nothing, and, when faults were injected, it is started and completed exchanges
after the last fault. -/
theorem route_pass_sound (r : Route) (h : routeResult r = .pass) :
    r.failed = 0 ∧ (r.faults = 0 ∨ (r.started = true ∧ r.completedAfter > 0 ∧ r.ruleActive = false)) := by
  unfold routeResult verdict at h
  by_cases hf : r.faults = 0 <;> by_cases ha : r.ruleActive = true <;> by_cases hs : r.started = true <;>
    by_cases hc : r.completedAfter = 0 <;> by_cases hl : r.failed > 0 <;>
    simp_all <;> omega

/-- A lost message makes the route FAIL unless a rule is still active (then RUNNING, never PASS). -/
theorem lost_never_passes (r : Route) (h : r.failed > 0) : routeResult r ≠ .pass := by
  intro hp
  have := (route_pass_sound r hp).1
  omega

/-- Once no rule is active, a route is never RUNNING. -/
theorem settled_route (r : Route) (h : r.ruleActive = false) : routeResult r ≠ .running := by
  unfold routeResult verdict
  by_cases hf : r.faults = 0 <;> by_cases hs : r.started = true <;> by_cases hc : r.completedAfter = 0 <;>
    by_cases hl : r.failed > 0 <;> by_cases ht : r.failedToCaller > 0 <;> simp_all

/-- `Expectation.toJson`. -/
def lost (count observed : Nat) : Nat := count - observed   -- Math.max(0, count - obs) on Nat

def expectResult (count observed : Nat) (anyRuleActive : Bool) : Res :=
  if observed ≥ count then .pass else if anyRuleActive then .running else .fail

theorem expect_pass_iff_nothing_lost (c o : Nat) (a : Bool) :
    expectResult c o a = .pass ↔ lost c o = 0 := by
  unfold expectResult lost
  by_cases h : o ≥ c <;> cases a <;> simp_all <;> omega

/-- `ChaosManager.report`: `result`. Every component (route, expectation, invariant) contributes its result. -/
def reportResult (parts : List Res) (anyRuleActive : Bool) : Res :=
  if parts.any (· == .fail) then .fail else if anyRuleActive then .running else .pass

/-- The report PASSes only if no component fails and no rule is active. -/
theorem report_pass_sound (parts : List Res) (a : Bool) (h : reportResult parts a = .pass) :
    (∀ p ∈ parts, p ≠ .fail) ∧ a = false := by
  unfold reportResult at h
  by_cases hf : parts.any (· == .fail) = true
  · simp [hf] at h
  · refine ⟨?_, ?_⟩
    · intro p hp hpf
      apply hf
      simp only [List.any_eq_true]
      exact ⟨p, hp, by simp [hpf]⟩
    · cases a <;> simp_all

/-- Adding a component (a new expectation or invariant) can never turn a FAIL into a PASS. -/
theorem report_fail_monotone (parts : List Res) (a : Bool) (x : Res)
    (h : reportResult parts a = .fail) : reportResult (x :: parts) a = .fail := by
  unfold reportResult at *
  by_cases hf : parts.any (· == .fail) = true
  · simp [List.any_cons, hf]
  · cases a <;> simp_all

/-! ## Rule counters (`ChaosRule.roll`, `ChaosRule.triggerStall`) -/

structure Counters where
  matched : Nat
  injected : Nat
deriving Repr

/-- One matching call: `roll(hit)`; `calls = 0` means no limit. -/
def roll (calls : Nat) (c : Counters) (hit : Bool) : Counters :=
  if calls > 0 ∧ c.matched ≥ calls then c
  else { matched := c.matched + 1, injected := c.injected + (if hit then 1 else 0) }

def run (calls : Nat) (c : Counters) : List Bool → Counters
  | [] => c
  | h :: t => run calls (roll calls c h) t

/-- For any traffic and any draws of the seeded random source: a rule never injects more faults than the calls it
matched, and a rule with `calls = N` sees at most N calls (so it injects at most N faults). -/
theorem roll_bounds (calls : Nat) (hits : List Bool) (c : Counters)
    (h1 : c.injected ≤ c.matched) (h2 : calls > 0 → c.matched ≤ calls) :
    (run calls c hits).injected ≤ (run calls c hits).matched ∧
    (calls > 0 → (run calls c hits).matched ≤ calls) := by
  induction hits generalizing c with
  | nil => exact ⟨h1, h2⟩
  | cons h t ih =>
    apply ih
    · unfold roll; split <;> (try split) <;> simp_all <;> omega
    · intro hc; unfold roll; split
      · exact h2 hc
      · simp_all; omega

theorem rule_injects_at_most_calls (calls : Nat) (hc : calls > 0) (hits : List Bool) :
    (run calls ⟨0, 0⟩ hits).injected ≤ calls := by
  have := roll_bounds calls hits ⟨0, 0⟩ (by simp) (by intro; simp)
  exact Nat.le_trans this.1 (this.2 hc)

/-- A rule with probability 1 (every draw a hit) and `calls = N` injects exactly `min N (number of calls)`:
the "5-call rule fails exactly 5 of 25" of the safety test, for any N and any traffic. -/
theorem certain_rule_exact (calls : Nat) (hc : calls > 0) (n : Nat) :
    (run calls ⟨0, 0⟩ (List.replicate n true)).injected = min calls n := by
  suffices ∀ m k, k ≤ calls → (run calls ⟨k, k⟩ (List.replicate m true)).injected = k + min (calls - k) m by
    simpa using this n 0 (Nat.zero_le _)
  intro m
  induction m with
  | zero => intro k _; simp [run]
  | succ m ih =>
    intro k hk
    simp only [List.replicate_succ, run]
    unfold roll
    by_cases hkc : k ≥ calls
    · have : k = calls := by omega
      subst this
      simp [hc]
      clear ih hk
      induction m with
      | zero => simp [run]
      | succ m ih2 => simp [List.replicate_succ, run, roll, hc, ih2]
    · have hnot : ¬ (calls > 0 ∧ k ≥ calls) := by omega
      simp only [hnot, ite_false, ite_true]
      rw [ih (k + 1) (by omega)]
      omega

/-- `triggerStall`: a stall rule on a call suspends the route once at most. -/
structure Stall where
  matched : Nat
  triggered : Bool
  injected : Nat
deriving Repr

def trigger (atCall : Nat) (s : Stall) : Stall :=
  if s.triggered then s
  else if s.matched + 1 < atCall then { s with matched := s.matched + 1 }
  else { matched := s.matched + 1, triggered := true, injected := s.injected + 1 }

def runStall (atCall : Nat) (s : Stall) : Nat → Stall
  | 0 => s
  | n + 1 => runStall atCall (trigger atCall s) n

theorem stall_fires_once (atCall n : Nat) :
    (runStall atCall ⟨0, false, 0⟩ n).injected ≤ 1 := by
  suffices ∀ n (s : Stall), (s.triggered = false → s.injected = 0) → (s.triggered = true → s.injected = 1) →
      (runStall atCall s n).injected ≤ 1 by
    exact this n _ (by simp) (by simp)
  intro n
  induction n with
  | zero => intro s h0 h1; cases ht : s.triggered <;> simp_all [runStall]
  | succ n ih =>
    intro s h0 h1
    apply ih
    · unfold trigger; split <;> (try split) <;> simp_all
    · unfold trigger; split <;> (try split) <;> simp_all

end Camel.ChaosVerdict
