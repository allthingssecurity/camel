# An exchange sanitizer for Camel developers: catch the hang, the double completion and the lost commit when they happen

[![Example: exchange #7, a normal run, a step that throws and a callback called twice, with what the sanitizer would report (preview, click for the full video)](docs/sanitizer-example-fast.gif)](https://github.com/allthingssecurity/camel/raw/camel-sanitizer-proposal/sanitizer/docs/sanitizer-explainer.mp4)

*Preview above. [Download the 3.5-minute whiteboard explainer (MP4, captions, no sound)](https://github.com/allthingssecurity/camel/raw/camel-sanitizer-proposal/sanitizer/docs/sanitizer-explainer.mp4). Everything in it is design only, like this document.*

A proposal for a dev-mode checker that watches every exchange of a running Camel application and reports, at the
moment it happens, when one of Camel's own promises is broken: an exchange that never completes, a callback called
twice, a unit of work that commits twice or never. The idea is borrowed from the sanitizers of other runtimes
(AddressSanitizer and ThreadSanitizer in LLVM, KASAN and KCSAN in the Linux kernel): cheap bookkeeping at a few
well-chosen points, a check of one simple rule at each of them, and a precise report when a rule breaks.

Every statement carries one of three labels:

- **design**: written down, not built;
- **prototype**: built and tested (none yet: this proposal comes before the prototype);
- **measured**: a number from a run, with its log.

Facts about Camel and about merged fixes were checked on apache/camel main (2026-11-05) *(checked)*.

## 1. The problem

Most of the concurrency fixes merged into camel-core in the last two weeks share one property: **when the bug hits,
nothing is logged and nothing fails**. The route says `Started`, the exchange simply never completes, or completes
twice, or the consumer never commits. A developer sees a hung HTTP request, a file that never moves, a message that
comes back, and has no clue where to look.

| Fix | What broke, silently | Rule that would have caught it |
|---|---|---|
| CAMEL-26430 (#28785), CAMEL-26445 (#28808), CAMEL-26446 (#29040) | An `AsyncProcessor` that throws instead of calling its callback: the exchange never completes (redelivery error handler) | R1 |
| CAMEL-26461 (#29041), CAMEL-26464 (#29127) | The same, under `noErrorHandler` and in steps without an error handler (`doTry`, `onException`, multicast outputs) | R1 |
| CAMEL-26575 (#29200, open) | A Multicast/Split `AggregationStrategy` whose `onCompletion` throws: the exchange hangs, only a DEBUG line | R1 |
| CAMEL-26578 (#29194, open) | `ProducerTemplate` stopped while async requests are queued: their futures never complete | R1, R4 |
| CAMEL-26569 / CAMEL-26576 (#29203, open) | Parallel `shareUnitOfWork`: an on completion handed over twice or never; a seda part takes the consumer's commit | R2, R3 |
| CAMEL-26579 (#29195, open) | Pooled exchanges reset the parent's unit of work: the consumer never commits, the file is picked up again | R2 |

*(checked: PR states on 2026-11-05)* Each of these was found by a TLA+ or SPIN model, a deterministic test or Fray,
and each was confirmed with a test on the real classes. A user hitting one of them in their own application had no
way to know which one it was.

## 2. What Camel already has

- `InflightRepository` knows every exchange in flight and how long it has been there (`browse(limit,
  sortByLongestDuration)`, `getDuration()`, `getElapsed()`), and the `inflight` dev console and `camel get inflight`
  show it *(checked)*. That answers "is something stuck?", not "why", and it says nothing about an exchange completed
  twice.
- The shutdown strategy logs the in-flight exchanges it waits for at stop *(checked)*.
- `EventNotifier` publishes exchange created, completed and failed events *(checked)*.

The sanitizer builds on these; it does not replace them.

## 3. The rules (design)

| Rule | Promise | Reported when |
|---|---|---|
| **R1** | Every exchange completes exactly once | still in flight after a timeout with its last step known to have thrown, or completed a second time |
| **R2** | Every unit of work runs its `done` exactly once | `done` a second time, or an exchange completes while its unit of work never ran `done` |
| **R3** | An exchange is used by one thread at a time (outside the EIPs that copy it) | two threads inside the same step of the same exchange at once (sampled, as KCSAN does) |
| **R4** | A task handed to a Camel executor with a callback or a future attached gets it completed | the executor is shut down with such a task still queued |

R1 and R2 are counters per exchange. R3 is sampled, so it costs almost nothing when switched on for long test runs.
R4 needs one small hook in the executor shutdown path.

## 4. How it runs (design)

A side table, one small record per exchange in flight, updated by hooks at the points every exchange already passes
through:

| Event | Hook (existing extension point unless noted) |
|---|---|
| exchange created / completed / failed | `EventNotifier` |
| each step called, its callback called, the step threw | an advice in `CamelInternalProcessor`, the same place the CAMEL-26464 fix lives (needs a small SPI addition: **design**) |
| unit of work `done` | a `UnitOfWorkFactory` that wraps the default (pluggable today) |
| executor shut down with queued tasks | a listener on `ExecutorServiceManager` shutdown (small SPI addition: **design**) |

A tiny example. Route: `from("file:inbox").process(new CheckStock()).to("seda:shipping")`.

- Normal run: exchange #7 is created (record: started, done 0, commit 0); step 1 calls its callback; the exchange
  completes (done 1); the file moves (commit 1); the record is dropped. Nothing is printed.
- `CheckStock` throws instead of calling the callback: after the timeout, the report says *exchange #7 (route
  `orders`, file `order.txt`) never completed; its last step `process1` (`CheckStock`) threw
  `IllegalStateException` before calling its callback*. Today: a file that never moves, and nothing in the log.
- The callback is called twice: on the second call, the report says *exchange #7 completed twice; second call from
  thread `pool-3`* with the stack of the culprit.

Switched on like chaos: off by default, only with profile `dev` or `camel.sanitizer.enabled=true`, never with profile
`prod`; results in a `sanitizer` dev console, `camel get sanitizer`, and optionally a failed test (a JUnit extension
that fails the test on the first violation).

## 5. Where the formal models come in (design)

TLA+ is not used at run time. It is used before:

1. **To pick the rules.** Each rule is a property from the models of this year's fixes ("completes once" from the
   `SimpleTask` and `NoErrorHandler` models, "commits once" from the shared unit-of-work models, "a future is always
   completed" from the `ProducerTemplate` one). TLC showed each one breaks on real Camel code, and the JIRAs above are
   the proof that they matter.
2. **To check the sanitizer itself.** The side table is concurrent code too: callbacks arrive on many threads. A small
   TLA+ model of it must show no false report and no missed violation in any interleaving, before it ships. A false
   report would make users switch it off.
3. **To place the hooks.** The models show which internal points every exchange passes through (the channel, the unit
   of work, the executor shutdown), so nothing slips around the hooks.

## 6. Cost (design; to be measured with the prototype)

- Off: no cost (the hooks are not installed).
- On, counters only: one map entry per exchange in flight and a few counter updates per step.
- On, with stacks: a stack capture per step boundary, only for development.
- R3 sampled: a watch on a small fraction of steps.

The prototype will measure the cost with the existing camel-core JMH benchmarks before any number is claimed.

## 7. Non-goals

- Not for production (like chaos).
- Not log analysis: it checks the rules inside Camel, at the moment of the event, so it sees the event that never
  happened (a missing callback) and the stack of a second call.
- Not a general data-race detector for user code; R3 only watches the exchange itself.
- No change to how Camel behaves: it only reports.

## 8. Plan

1. **Prototype** (camel-sanitizer module, R1 and R2 only), local.
2. **Show it on real bugs**: run it on main from before the fixes in section 1 and show that it reports each one with
   the right step; run it on today's main and show it stays quiet on the camel-core test suite.
3. **Measure** the cost (section 6).
4. **Model** the side table in TLA+ (section 5.2).
5. Then a JIRA and a design document in `design/`, as for chaos.

Questions for the community: is R3 worth it, or should it stay with R1, R2 and R4? Should the JUnit extension be part
of camel-test from the start?

_Drafted with Claude Code on behalf of allthingssecurity_
