# camel-chaos: a fault injector for Camel developers (prototype)

**Status: prototype for discussion, not a pull request.** Branch `camel-chaos-prototype` of
github.com/allthingssecurity/camel, on top of apache/camel `main`. Dev and test only: off unless enabled at startup,
refused with the `prod` profile, and nothing changes unless you add the jar.

## The problem

An application has an error handler, redeliveries, a dead letter channel, health checks. Whether they work is
usually found out in production. Recent Camel fixes show how quiet the failures are: the slack consumer skipped the
rest of a poll when the route was suspended in the middle of it (CAMEL-25684); the mongodb tailable consumer's thread
ended while the server was down and the route kept saying `Started` (CAMEL-25679). Nothing was logged in either case.

Claus asked on Zulip for a chaos injector that a developer can drive from the CLI, the TUI, or an AI assistant through
MCP, to check that an application is robust.

## The idea

1. **Faults at seams Camel already has**, no core change: calls to endpoints (`to`) are wrapped through
   `InterceptStrategy` and can fail or slow down; a route's consumer can be stalled through the `RouteController`,
   right away or on the Nth call, in the middle of a batch.
2. **Small rules at runtime**: "30 % of calls to `http*` fail for the next 20 calls", "add 2 s to `kafka:*` for a
   minute", "suspend route `orders` on the 3rd call for 5 s". Rules end by themselves.
3. **A seed decides which calls fail**: same seed, same traffic, same faults, same report (SHA-256).
4. **The report is the point**: per rule, faults injected and health before/during/after; per route, completed,
   failed (lost), returned to the caller, handled by the error handler, redelivered, and PASS/FAIL. Expectations
   (`expect orders 30`) catch messages lost without becoming an exchange and consumers that died while `Started`.

```
$ camel cmd chaos my-app --command=add --type=fail --pattern='http*' --probability=30% --calls=20
$ camel cmd chaos my-app --command=add --type=stall --route-id=orders --duration=3000
$ camel cmd chaos my-app --command=expect --route-id=orders --count=30
$ camel cmd chaos my-app
  rule backend-30pct  fail    log:backend*         ended  matched=20 injected=4 health UP/UP/UP
  rule stall-orders   stall   orders               ended  matched=1 injected=1 health UP/DOWN/UP
  rule backend-down   fail    log:backend*         ended  matched=3 injected=3 health UP/UP/UP
  route orders         completed=116 failed=0 errorHandled=1 redeliveries=6 faults=8 -> RECOVERED [PASS]
  expect orders        >=30 completed=68 lost=0 [PASS]
Result: PASS
```

## Proof points

Each is a test or a script with a log (the logs are in `camel-lean/logs-chaos/` of the author's workspace; the
commands to produce them are in [`DEMO.md`](DEMO.md)).

| # | Claim | Test | Result |
|---|---|---|---|
| a1 | finds a real bug in real code | [`SlackConsumerChaosTest`](../camel-slack/src/test/java/org/apache/camel/component/slack/SlackConsumerChaosTest.java), run on main and on `SlackConsumer` from before CAMEL-25684 | before the fix: 5/5 runs FAIL, 2 of 4 messages lost; main: 5/5 PASS |
| a2 | flags a consumer dead while `Started` | [`ChaosConsumerDeadWhileStartedTest`](src/test/java/org/apache/camel/chaos/ChaosConsumerDeadWhileStartedTest.java), a **minimal reproduction** of the CAMEL-25679 mechanism, not the mongodb component | old shape: health UP, route Started, expectation 0/20 FAIL; fixed shape: PASS |
| b | reproducible | [`ChaosReproducibilityTest`](src/test/java/org/apache/camel/chaos/ChaosReproducibilityTest.java) | seed 11: 3/3 identical report SHA-256; seed 12: other schedule, other SHA-256 |
| c | no measurable cost when off | [`ChaosOverheadTest`](src/test/java/org/apache/camel/chaos/ChaosOverheadTest.java) (`-Dchaos.bench=true`), timing loop with rotated order and an A/A control | off: +0.4 % and +2.8 % over two runs (A/A +0.0 % and +0.2 %) on a 640-670 ns no-op route; enabled with no rule: +6 to +9 % |
| d | safe | [`ChaosSafetyTest`](src/test/java/org/apache/camel/chaos/ChaosSafetyTest.java) | off by default, hooks removed at startup, cannot be switched on at runtime, refused with prod, rules expire, removing a rule restores normal behaviour |
| e | verdicts are right | [`ChaosVerdictTest`](src/test/java/org/apache/camel/chaos/ChaosVerdictTest.java) | same 30 % faults: with redelivery PASS (200/200), with a dead letter channel PASS, without error handling FAIL with exactly 56 lost |
| f | control works end to end | [`LocalCliConnectorChaosActionTest`](../../dsl/camel-cli-connector/src/test/java/org/apache/camel/cli/connector/LocalCliConnectorChaosActionTest.java), [`ChaosDemoTest`](src/test/java/org/apache/camel/chaos/ChaosDemoTest.java), `demo/` (live app driven by `camel cmd chaos` and by the MCP tool method, [`McpChaosE2E`](demo/McpChaosE2E.java)) | the fault applies in the application and the report comes back through each surface |

Also: [`ChaosManagerTest`](src/test/java/org/apache/camel/chaos/ChaosManagerTest.java) (rules, latency, stall) and
[`ChaosMidFlightStallTest`](src/test/java/org/apache/camel/chaos/ChaosMidFlightStallTest.java) (a file consumer
suspended in the middle of a batch of 6 still delivers all 6).

## How it hooks in

| Class | Role |
|---|---|
| `ChaosContextServicePlugin` | `ContextServicePlugin`: installs the manager when the jar is on the classpath |
| `ChaosManager` | rules, seed, `InterceptStrategy`, event notifier, health sampling, expectations, report; removes its hooks at startup when not enabled |
| `ChaosProcessor` | wraps each `EndpointAware` processor; with no active rule it reads one volatile list |
| `ChaosRule` | `fail`, `latency`, `stall` (route, or on the Nth call); probability, calls, duration, exception |
| `ChaosDevConsole` | the `chaos` console: `action=add/remove/clear/reset/expect/report` |

| Surface | Wired | Tested |
|---|---|---|
| dev console `chaos` (`/q/dev/chaos`) | yes | `ChaosDemoTest`, `ChaosSafetyTest` |
| CLI connector action `chaos` | yes | `LocalCliConnectorChaosActionTest` with the real console |
| `camel cmd chaos` (`CamelChaosAction`) | yes | live application, `demo/` |
| MCP `camel_runtime_chaos` | yes | argument checks in `RuntimeToolsTest`; tool method called against the live application (not through an MCP client) |
| TUI | no | |

Enable with `camel.chaos.enabled=true` (property, system property or `CAMEL_CHAOS_ENABLED`) **before the application
starts**; `camel.chaos.seed` sets the seed (default 42).

## Limits

- Only `to` (`EndpointAware`) sends are wrapped: not `toD`, `enrich`, dynamic `wireTap`, nor calls made inside a bean
  or a component's client. Faults inside a component (a repository store, a driver reconnect) are out of reach.
- Latency is a `Thread.sleep` in the calling thread. A stall suspends the route; it does not take the broker away.
- No thread-pool faults yet.
- The verdict is a heuristic over counters: fire-and-forget exchanges that fail unhandled count as lost,
  request/reply failures as returned to the caller. Loss downstream of a successful call is not seen; expectations
  help, given a count.
- Not in the catalog, BOM or website docs yet (the generated CLI and MCP doc pages are updated).

## Next steps

`camel run --chaos` and rules from a file; docs; dynamic sends, thread-pool and poll faults; a TUI tab; optional
adapters (Toxiproxy, Testcontainers pause, Chaos Mesh). The full proposal, with the debate and the effort, is kept
with the author's community notes (`chaos-proposal.md`).
