# A chaos injector for Camel developers: break the calls, stall the consumers, read the report

[![Animated explainer: how camel-chaos works (silent preview, click for the narrated video)](docs/chaos-explainer.gif)](https://github.com/allthingssecurity/camel/raw/camel-chaos-prototype/components/camel-chaos/docs/chaos-explainer.mp4)

*Silent preview above (recorded on 10-17). [Download the narrated 4-minute explainer (MP4, 9 MB)](https://github.com/allthingssecurity/camel/raw/camel-chaos-prototype/components/camel-chaos/docs/chaos-explainer.mp4).*

A proposal for a chaos injector that Camel application developers can drive from the CLI, the TUI or an AI through
MCP, following the Zulip discussion. It was parked on 2026-10-17 until camel-test-dst had its time source; that is
now on main (CAMEL-25911, c536c99caaaa), and the prototype has been rebased onto it.

Facts marked *(checked)* were verified again on apache/camel main **72a1bff606a3 (2026-10-25)** or in the logs
cited. Every other statement carries one of three labels:

- **design**: written down, not built;
- **prototype**: built on the branch and covered by its tests, no numbers claimed;
- **measured**: a number from a run, with its log.

Nothing of the prototype is in Camel.

Branch: `camel-chaos-prototype` on github.com/allthingssecurity/camel (rebased onto main
72a1bff606a3), module `components/camel-chaos`. The logs of every number below are kept in the author's workspace,
not in this repository (`logs-chaos2/` for the runs of 2026-10-25 on the rebased branch, `logs-chaos/` for numbers
marked *(10-17)* that were not run again); the tests and scripts that produce them are on the branch.

### What changed since 10-17

- The branch is rebased onto current main: five commits instead of six, one conflict (a generated file), all chaos
  tests pass again (section 9).
- camel-chaos now reads the clock and sleeps through Camel's internal `TimeSource`, so a chaos run inside a
  camel-test-dst world runs on the virtual clock and replays from its seed (section 8.4, **measured**).
- New motivating examples: seven bugs where removing or stopping one route breaks a resource that other routes
  still use (section 1.1), with the rule that would have exposed each (**design**: a `remove` rule, section 4.2).
- Three facts changed on main: the mongodb, zookeeper and pg-replication-slot fixes of section 1 are now merged, and
  the MCP server has 42 runtime tools.

## 1. The problem

A Camel application has an error handler, redeliveries, maybe a dead letter channel, a circuit breaker, health checks.
Whether they work is usually found out in production, the first time a backend is slow or a broker goes away. To check
it today a developer stops a container by hand or writes a test that mocks the failure, and then reads logs to guess
what happened.

This month's fixes show what goes wrong when nobody tried. Each was silent: nothing logged at the moment of the damage,
and the route kept saying `Started`.

| Bug | What happened | What a chaos run shows |
|---|---|---|
| CAMEL-25684 slack (#27923, on main 68329f0919bc *(checked)*) | A route suspend that reaches the consumer in the middle of a poll ends the batch; the timestamp had already moved past the rest of the poll, so those messages were never read again. | Stall the route on the 2nd call of a poll, expect 4 messages: **2 lost, FAIL** before the fix *(10-17)*, PASS on main (5 of 5 again on 10-25) (section 9, proof a1, on the real consumer; walked through step by step in section 5). Main now also has a DST world for it (`SlackSuspendDstTest`). |
| CAMEL-25679 mongodb (#27918, now on main 3af2c90d63ad *(checked)*) | While the server was down, opening the new cursor timed out outside the try of the consumer loop; the exception ended the consumer thread. Route `Started`, health `UP`, nothing consumed until a restart. | Fail the server for 3 calls: expectation **0 of 20, FAIL**, health still UP (proof a2, minimal reproduction of the mechanism, not the real component; **measured** again on 10-25). |
| CAMEL-25681 file idempotent repository (on main 7b12fca82cf4 *(checked)*) | A failed store left the key in the cache; the retried message was taken for a duplicate and skipped. | Not reachable by this prototype: the fault is inside the repository, not at an endpoint call (section 11). |
| CAMEL-25682 resilience4j (on main 24cd63f10fb2 *(checked)*) | A timed-out call and the task's write-back raced on the same exchange. | A latency rule that ends the task at the timeout, and the TLA+ model's invariant as a chaos invariant: **17 to 28 of 20,000 calls violate it, FAIL** in 3 of 3 runs before the fix *(10-17)*, 0 and PASS on main (3 of 3 on 10-17, 1 of 1 again on 10-25) (proof g1, section 8). Sampled, not replayed: a deterministic replay is DST's job. |
| CAMEL-25655 zookeeper (#27858, on main 2a30744c443a), CAMEL-25654 pg-replication-slot (#27857, on main 71d8ee06eceb) *(checked)* | Consumers that never reconnect after a session expiry or a server restart. | Same shape as mongodb (consumer alive check, expectation); needs the external service (Testcontainers) or an adapter (section 7). **Design**, not run. |

What they share: the damage is invisible in logs and in the route status, and visible only when you count what came
out against what went in, after a fault at the right moment. That count is what this proposal adds.

### 1.1 Since 10-17: removing or stopping one route breaks what other routes share

Seven bugs found since the proposal was parked have one shape. Several routes use the same endpoint, or the same
cache, channel or session behind it. One route is removed (a route reload, a route template removed, JMX or
controlbus) or stopped. Camel stops and shuts down the endpoint of that route's consumer (`RouteService.doShutdown`),
also when other routes still use it, and the endpoint releases something the other routes still hold. The route that
was removed is fine. The damage lands on the routes that were left alone.

They were found by a seeded lifecycle harness (random stop, start, suspend, resume, remove and add over several routes,
with a check after every step) and by a sweep of the endpoints' stop methods, not by chaos. Each has a JIRA and a fix
branch with a test that fails on main; none of the fixes is on main yet *(checked: no commit for these keys on
72a1bff606a3)*.

| Bug | What breaks when one route goes | Rule that would have exposed it | What the report would show |
|---|---|---|---|
| CAMEL-26286 disruptor | Removing one of three routes on `disruptor:orders?multipleConsumers=true` shuts the shared disruptor down: the other two routes receive nothing, every send fails with "Disruptor was not started". | `remove` on one consumer route, then add it back | the producing route: exchanges failed unhandled, FAIL; the two other consumer routes: an expectation, FAIL (they get nothing, so their own counters see nothing) |
| CAMEL-26287 jcache | Removing the only route of `jcache://sessions` closes the JVM-wide caching provider: every other jcache endpoint fails with "The cache closed", for good. | `remove` on the `sessions` route | the `orders` route: failed unhandled, then NOT-RECOVERED (nothing completes after the fault, also after the re-add) |
| CAMEL-26288 quickfix | Removing one of two routes on one FIX session clears the endpoint's consumer list: the other route silently receives nothing. | `remove` on one route (a plain stop does not trigger it) | only an expectation on the other route, FAIL, as for slack: the counters see nothing wrong |
| CAMEL-26302 ehcache | Removing the route that consumes cache events closes the cache that the producer routes hold: "State is UNINITIALIZED", also after the route is added again. | `remove` on the event route, then add it back | the producer routes: failed unhandled, NOT-RECOVERED after the re-add |
| CAMEL-26303 browse | A stopped consuming route breaks every send to the endpoint (RouteStoppingException); a removed one gives an NPE on every send. | the existing `stall`, for the stop half: a browse consumer cannot suspend, so `suspendRoute` stops the route instead, and the chaos resume starts it again; `remove` for the NPE half | the producing routes: failed unhandled while the stall lasts, FAIL |
| CAMEL-26304 jgroups | Removing the consumer route closes the endpoint's own channel, which the producer still uses: "channel is closed". | `remove` on the consumer route | the producer route: failed unhandled, FAIL |
| CAMEL-26305 jgroups-raft | The same for the endpoint's own RaftHandle: the channel and the Raft log are closed and the node leaves the cluster. | `remove` on the consumer route | the producer route: failed unhandled, FAIL |

How sure is this column? It is **design analysis**, not a chaos run: the `remove` rule does not exist yet (section
4.2), and the browse row relies on Camel's documented fallback from suspend to stop for a consumer that cannot suspend
(`AbstractCamelContext.suspendRoute`, *(checked)* on main) and on the chaos resume path that starts a stopped route,
which is in the prototype. What the class teaches is independent of that: the fault is put on one route and the loss
shows on another, so a chaos report must judge every route, not only the target of the rule. The prototype already
counts every route; the expectation is needed where the victim route goes silent (disruptor, quickfix).

## 2. The pitch

`camel-chaos` is a dev and test only module that injects faults into a running Camel application at the seams Camel
already has: it fails or slows down calls to endpoints matching a pattern, and it stalls a route's consumer, now or in
the middle of a batch. Rules are added and removed at runtime, end by themselves after N calls or a duration, and draw
from a seed, so the same seed fails the same calls. It is driven from Camel CLI (`camel cmd chaos`), from an AI
assistant through the MCP tool `camel_runtime_chaos`, or over HTTP through the `chaos` dev console. What it gives back
is the report: per rule, how many faults were injected and the health before, during and after; per route, exchanges
completed, failed, handled by the error handler, redelivered, lost; and a PASS or FAIL. It is off unless enabled at
startup, refused with profile `prod`, and costs nothing measurable when off.

## 3. User experience

### Camel CLI (works in the prototype)

```
$ camel cmd chaos my-app --command=add --type=fail --pattern='http*' --probability=30% --calls=20
$ camel cmd chaos my-app --command=add --type=stall --route-id=orders --duration=3000
$ camel cmd chaos my-app --command=expect --route-id=orders --count=30
$ camel cmd chaos my-app
Chaos enabled=true seed=7
  rule backend-30pct  fail    log:backend*         ended  matched=20 injected=4 health UP/UP/UP
  rule stall-orders   stall   orders               ended  matched=1 injected=1 health UP/DOWN/UP
  rule backend-down   fail    log:backend*         ended  matched=3 injected=3 health UP/UP/UP
  route orders         completed=190 failed=0 errorHandled=1 redeliveries=6 faults=8 -> RECOVERED [PASS]
  expect orders        >=30 completed=114 lost=0 [PASS]
Result: PASS
```

(real output of the rebased branch on 10-25, `logs-chaos2/f-cli-live-run1.log`; the application is the demo of
section 10 with the rule ids of `DEMO.md`. The rules, faults and verdicts are the same as on 10-17; the completed
counts depend on how long each step waited.) `--json` gives everything, including a timeline of rule-added,
route-stalled, route-resumed, rule-ended with the health at each step.

### MCP (works in the prototype, tool method called directly)

The assistant gets one tool, `camel_runtime_chaos`, next to the other `camel_runtime_*` tools:

```json
{ "nameOrPid": "chaos-demo", "action": "add", "type": "fail", "pattern": "log:backend*",
  "probability": "100%", "calls": "3", "exception": "java.io.IOException" }
{ "nameOrPid": "chaos-demo", "action": "expect", "routeId": "orders", "count": "20" }
{ "nameOrPid": "chaos-demo" }      -> report ... "result": "PASS"
```

### AI prompts this enables

- "Make 30 % of the calls to the payment API fail for a minute and tell me if any order is lost."
- "Stall the Kafka consumer in the middle of a batch and check that every message still arrives."
- "Which of my routes has no error handling? Prove it with a fault, then propose the fix."
- "After you changed the error handler, run the same seed again and compare the reports."

The last one matters: the seed makes a before/after comparison of two reports meaningful.

### TUI (not in the prototype)

A Chaos tab in camel-jbang-plugin-tui: rules with live counters, the timeline, and the verdict per route. The TUI's AI
panel would get the same tool through the shared `ToolRegistry`.

## 4. Design

### 4.1 Hooks (no change to Camel core)

| Seam | Used for |
|---|---|
| `ContextServicePlugin` (service loader) | installs the manager when the jar is on the classpath |
| `InterceptStrategy` | wraps every processor that knows its endpoint (`EndpointAware`: `to`) in a `ChaosProcessor` |
| `RouteController` | suspends and resumes a route for a stall; for a `BatchConsumer` the stall waits until the suspend has reached the consumer, so it stops in the middle of its batch |
| `EventNotifier` | completed, failed, failure handled, redelivery events for the counters |
| `HealthCheckHelper` | health before, during and after each rule |
| `DevConsole` (`chaos`) | the one control surface (section 5 follows a rule from the CLI to the processor); CLI connector action `chaos`, `camel cmd chaos` and the MCP tool all end there |

### 4.2 Fault types

| Type | Effect | Options |
|---|---|---|
| `fail` | the call throws (default `ChaosException`, or any exception class with a String constructor); the route's error handler sees a real failure | `pattern`, `probability`, `calls`, `duration`, `exception` |
| `latency` | the call waits first | `pattern`, `delay`, `probability`, `calls`, `duration` |
| `stall` (route) | the route is suspended for `duration`, then resumed | `routeId`, `duration` |
| `stall` (on call) | the route of the exchange is suspended on the `atCall`-th matching call, while that exchange is routed | `pattern`, `atCall`, `duration` |
| `remove` (route, **design**) | the route is stopped and removed for `duration`, then added again from its definition (the `Model` context plugin), as a reload or a removed route template does | `routeId`, `duration`, `readd` (default true) |

All four built types are **prototype**. A stall on a route whose consumer cannot suspend becomes a stop: that is
Camel's own fallback in `suspendRoute`, and the chaos resume starts the route again. The `remove` type is what the
bugs of section 1.1 need; it is about 100 lines on the existing route controller and model APIs (estimate, not
built).

### 4.3 Rules and seeds

A rule is active from `add` until it has seen `calls` matching calls, its `duration` has passed, or it is removed.
Each rule draws from its own `java.util.Random(seed * 1000003 + ruleOrder)`, so two rules do not disturb each other
and the same seed with the same traffic fails the same calls. The seed is `camel.chaos.seed` (default 42).

### 4.4 The report and its invariants

Per route: `completed`, `failed` (unhandled, fire and forget: **lost**), `failedReturnedToCaller` (request/reply: the
caller got the exception), `failureHandled` (dead letter channel, `onException` handled), `redeliveries`,
`faultsInjected`, `completedAfterLastFault`, and a verdict:

- `RECOVERED`: the route is started and completed exchanges after the last fault;
- `RECOVERED, but N exchange(s) failed unhandled`: FAIL, those messages are lost;
- `NOT-RECOVERED`: the route is not started, or nothing completed after the last fault (FAIL);
- `chaos-active`: a rule is still running (RUNNING).

Expectations (`expect routeId count`) cover what the counters cannot see: messages lost without ever becoming an
exchange (the slack case) and consumers that died while the route stays `Started` (the mongodb case). Invariants
(`invariant routeId predicate`) check a property of every exchange of a route when it completes, for example one taken
from a TLA+ model (section 8). The report's `result` is FAIL if any route, expectation or invariant fails.

The canonical report (no wall-clock times, sorted keys) has a SHA-256, so two runs can be compared byte for byte.

### 4.5 Safety

Off unless `camel.chaos.enabled=true` when the context starts. When off, the hooks are taken out at startup, so the
jar costs nothing and nobody can switch chaos on later (not the CLI, not MCP, not the dev console, not code). Profile
`prod` refuses it even when enabled. Rules end by themselves.

### 4.6 Clock and threads (prototype)

Since the rebase every clock read and every sleep of camel-chaos goes through `org.apache.camel.util.TimeSource`, the
internal time source that DST stage 2 put into camel-util (CAMEL-25911):

| Where | Before | Now |
|---|---|---|
| rule start and end by duration (`activate`, `isExpiredAt`, the expiry check every 100 ms) | `System.currentTimeMillis()` | `TimeSource.currentTimeMillis()` |
| stall duration (counted from the moment the suspend is done) | `System.currentTimeMillis()` | `TimeSource.currentTimeMillis()` |
| latency rule | `Thread.sleep(delay)` | `TimeSource.sleep(delay)` |
| the wait until a suspend has reached a batch consumer | `Thread.sleep(5)` and a 10 s deadline | `TimeSource.sleep(5)` and the deadline on `TimeSource` |
| timeline, health samples, report times | `Instant.now()` | `TimeSource.instant()` |
| last fault per route | `System.currentTimeMillis()` | `TimeSource.currentTimeMillis()` |

Two thread changes go with it. The expiry check already ran on a scheduled executor of the `ExecutorServiceManager`.
The route suspend of a stall on a call used its own `new Thread`; it now runs on a cached pool of the
`ExecutorServiceManager` too. With the default provider `TimeSource` makes the same JDK calls as before, so nothing
changes outside a DST run: the 20 tests of 10-17 pass unchanged on the rebased branch (section 9). What it gives inside
a DST run is in section 8.4.

The CLI side (`camel cmd chaos` waiting for the reply file) runs in the developer's shell, not in the application, and
is unchanged.

## 5. How it works, step by step

One real scenario, followed through every class: the slack consumer from before CAMEL-25684, a route suspend that
reaches the consumer in the middle of a poll, 2 of 4 messages lost, report FAIL (proof a1). The route under test is

```java
fromF("slack://general?token=...&serverUrl=http://localhost:%d&maxResults=10&delay=20", port)
    .routeId("slack")
    .process(e -> routed.add(...))
    .to("mock:sink");
```

and a local HTTP server answers `conversations.history` as Slack does (newest first). Code excerpts below are from
the module on the branch (`components/camel-chaos/src/main/java/org/apache/camel/chaos/`), shortened only by
leaving out lines.

### 5.1 The sequence

```
 CLI / MCP          LocalCliConnector     ChaosDevConsole +      ChaosProcessor          SlackConsumer        RouteController
 (camel cmd chaos)  (in the app)          ChaosManager           [to mock:sink]          (BatchConsumer)
     |                    |                     |                       |                       |                     |
 0   |   startup: ContextServicePlugin.load -> ChaosManager.install (InterceptStrategy, EventNotifier, service)      |
     |                    |   routes built: wrapProcessorInInterceptors(to mock:sink) -> ChaosProcessor               |
     |                    |   doStart: enabled and not prod? yes -> armed, expiry check every 100 ms                 |
 1   | add stall --pattern=mock:sink --at-call=2 --duration=500                     |                       |                     |
     |--{pid}-action-{id}.json->|               |                       |                       |                     |
     |                    |--call(JSON,params)->|                       |                       |                     |
     |                    |                     | addRule: checks, id, healthBefore, activate(seq, seed)      |                     |
     |<-{pid}-output-{id}.json--|<-- result + report                    |                       |                     |
 2   | expect --route-id=slack --count=4 (same path) -> Expectation(baseline = completed so far)                    |
 3   |                    |                     |                       |      poll: m5 m4 m3 m2 (newest first)       |
     |                    |                     |<-beforeSend(mock:sink)-|<---------- m5 ----------|                     |
     |                    |                     |  matched=1 < atCall: nothing                     |                     |
     |                    |                     |<-beforeSend(mock:sink)-|<---------- m4 ----------|                     |
     |                    |                     |  matched=2: triggerStall                         |                     |
     |                    |                     |--- thread ChaosSuspendRoute-slack ---------------------------------> suspendRoute
     |                    |                     |  wait until !isBatchAllowed() ------------------>|                     |
     |                    |                     |  timeline route-stalled, healthDuring            |                     |
     |                    |                     |                       |-- m4 -> mock:sink     |                     |
     |                    |                     |                       |   batch loop sees isBatchAllowed()=false:   |
     |                    |                     |                       |   m3, m2 not routed (before the fix the     |
     |                    |                     |                       |   poll's timestamp had moved past them)     |
     |                    |                     |  completed events -> RouteStats (m5, m4)          |                     |
 4   |                    |                     | expireRules: suspended + 500 ms -> endRule -> resumeRoute ------------> resumeRoute
     |                    |                     |  healthAfter, timeline rule-ended                 |                     |
 5   |                    |                     |                       |   next polls: before the fix nothing new;   |
     |                    |                     |                       |   on main m3, m2         |                     |
 6   | camel cmd chaos -> report(): rules, routes + verdict, expectations -> Result FAIL (2 lost)                    |
```

### 5.2 Startup: install, wrap, decide

The jar carries a service loader file for `org.apache.camel.spi.ContextServicePlugin`, so Camel calls the plugin
for every context it creates (`ChaosContextServicePlugin.java`):

```java
public void load(CamelContext camelContext) {
    ChaosManager.install(camelContext);
}
```

`install` registers the manager three times, as a context plugin (so the console finds it), as an intercept
strategy and as an event notifier (`ChaosManager.java`, `install`):

```java
context.getCamelContextExtension().addContextPlugin(ChaosManager.class, manager);
context.getCamelContextExtension().addInterceptStrategy(manager);
context.getManagementStrategy().addEventNotifier(manager.notifier);
context.addService(manager, true, true);
```

When the routes are built, Camel passes each processor through the intercept strategies. Only processors that know
their endpoint are wrapped; in the slack route that is `to("mock:sink")` (`wrapProcessorInInterceptors`):

```java
if (armed && !isEnabled()) {
    disarm();                       // routes are built before services start: decide on the first processor
}
if (armed && target instanceof EndpointAware) {
    return new ChaosProcessor(this, target);
}
return target;
```

`isEnabled()` is false with profile `prod` whatever the property says, and otherwise reads `camel.chaos.enabled`.
When it is false, `disarm()` removes the event notifier and `doStart()` removes the intercept strategy, so the
application runs as if the jar were absent and `addRule` refuses for the rest of its life. In the slack test it is
true (`camel.chaos.enabled=true`, `camel.chaos.seed=25684`), so `doStart()` reads the seed and schedules
`expireRules` every 100 ms.

### 5.3 The rule travels from the CLI to the manager

The developer types

```
camel cmd chaos slack-app --command=add --type=stall --pattern=mock:sink --at-call=2 --duration=500 --id=suspend-mid-poll
camel cmd chaos slack-app --command=expect --route-id=slack --count=4
```

or, from an assistant, calls the MCP tool `camel_runtime_chaos` with
`{"nameOrPid":"slack-app","action":"add","type":"stall","pattern":"mock:sink","atCall":"2","duration":"500","id":"suspend-mid-poll"}`.
(The proof run a1 adds the same rule through the Java API, `chaos.addRule(ChaosRule.stall(null, 500).onCall("mock:sink").atCall(2))`,
because it is a unit test; the CLI and MCP path is tested separately in proof f, on another application.)

`CamelChaosAction` writes `~/.camel/{pid}-action-{requestId}.json` with `"action":"chaos"` and the chaos action in
`"command"` (the connector's own protocol already uses `action`). The MCP tool builds the same map. In the
application, `LocalCliConnector` polls the file and hands it to the dev console (`doActionChaosTask`):

```java
DevConsole dc = ...getContextPlugin(DevConsoleRegistry.class).resolveById("chaos");
params.put("action", command != null ? command : "report");
// type, pattern, routeId, probability, calls, delay, duration, exception, message, id, atCall, count
JsonObject json = (JsonObject) dc.call(DevConsole.MediaType.JSON, params);
writeOutput(json);                  // -> ~/.camel/{pid}-output-{requestId}.json
```

`ChaosDevConsole.createRule` turns the strings into a `ChaosRule` (`"30%"` and `0.3` both mean 0.3), and
`ChaosManager.addRule` checks it (a stall needs a duration, and a route id or a pattern; a fail or latency rule needs
a pattern; probability in 0..1; no active rule with the same id), samples `healthBefore`, and activates it
(`ChaosRule.activate`):

```java
this.random = new Random(seed * 1_000_003L + seq);   // seq = order in which the rule was added
this.expiresAt = durationMs > 0 && type != Type.STALL ? now + durationMs : 0;
```

The active rules are kept in a `volatile List` that is replaced, never changed in place, so the hot path reads it
without a lock. The expectation records how many exchanges the route had completed so far (its baseline).

### 5.4 Anatomy of a rule

The stall rule of this scenario, as the report shows it:

| Field | Value here | Meaning |
|---|---|---|
| `id` | `suspend-mid-poll` | given, or `chaos-<n>` |
| `type` | `stall` | `fail`, `latency` or `stall` |
| `pattern` | `mock:sink` | endpoint pattern, as in `intercept` (wildcard or regex); for a stall it selects the call that triggers it |
| `routeId` | `slack` (filled in when it fires) | for a route stall given up front; for a stall on a call, the route of the exchange |
| `atCall` | `2` | stall on the 2nd matching call |
| `durationMs` | `500` | how long the route stays suspended, counted from the moment the suspend is done |
| `probability` | 1.0 | fail and latency rules: chance per matching call, drawn from the rule's own `Random` |
| `calls` | 0 | fail and latency rules: the rule ends after N matching calls (0 = no limit) |
| `delayMs` | (not used) | latency rules |
| `exception`, `message` | (not used) | fail rules: exception class with a String constructor |
| `state`, `endReason` | `ended`, `expired` | `active` until `calls-reached`, `expired` or `removed` |
| `matched`, `injected` | 2, 1 | matching calls seen, faults injected |
| `injectedPerRoute`, `injectedCallNumbers` | `{slack: 1}`, `[2]` | where and on which call numbers the faults fell; with the seed this is the schedule |
| `healthBefore`, `healthDuring`, `healthAfter` | UNKNOWN (no camel-health in that test) | `HealthCheckHelper` snapshots; in the live demo the stall shows `UP/DOWN/UP` |

### 5.5 A matching call reaches the ChaosProcessor

Every call through `to("mock:sink")` goes through `ChaosProcessor.process`. With no active rule it reads one
volatile list and calls the real processor:

```java
if (manager.hasActiveRules() && processor instanceof EndpointAware ea) {
    Exception fault = manager.beforeSend(exchange, ea.getEndpoint().getEndpointUri());
    if (fault != null) {
        exchange.setException(fault);     // the error handler sees a real failure
        callback.done(true);
        return true;
    }
}
return super.process(exchange, callback);
```

`beforeSend` walks the active rules. The pattern match is cached per endpoint URI in the rule. Then, per type:

- **fail**: `roll` counts the call and draws from the rule's `Random`; on a hit it returns the exception, which the
  processor sets on the exchange:

  ```java
  long n = matched.incrementAndGet();
  boolean hit = probability >= 1.0d || random.nextDouble() < probability;
  ```

- **latency**: the same draw; on a hit `Thread.sleep(delayMs)` in the calling thread, then the real call.
- **stall on a call** (this scenario): `triggerStall` counts the matching call and fires once, on call `atCall`. For
  m5 `matched=1`, nothing happens. For m4 `matched=2`, and `stallRouteMidFlight` runs in the consumer's thread. A route
  cannot suspend itself while it routes, so a separate thread asks the `RouteController`, and the consumer's thread
  waits until the suspend has reached a `BatchConsumer`:

  ```java
  Thread suspender = new Thread(() -> camelContext.getRouteController().suspendRoute(routeId), ...);
  suspender.start();
  Consumer consumer = camelContext.getRoute(routeId).getConsumer();
  while (consumer instanceof BatchConsumer bc && System.currentTimeMillis() < deadline) {
      if (!bc.isBatchAllowed()) {
          break;                         // the suspend has reached the consumer
      }
      Thread.sleep(5);
  }
  ```

  Then m4 continues to `mock:sink`. Back in its batch loop the slack consumer asks `isBatchAllowed()`, gets false,
  and stops: m3 and m2 are not routed. Before CAMEL-25684, `SlackConsumer.createExchanges` had already moved the
  timestamp to the most recent message of the poll (m5) before `processBatch` routed anything, so after the resume the
  consumer asks Slack only for messages newer than m5, and m3, m2 are never read again. On main the consumer keeps the
  messages of the poll it did not route, and routes m3 and m2 after the resume.

A plain route stall (`--route-id=orders`, no pattern) skips the wait: `addRule` suspends the route at once.

### 5.6 Counters and health

`ChaosEventNotifier` listens to exchange events only (`setIgnoreCamelContextEvents(true)` and so on) and counts per
route of origin:

```java
if (event instanceof CamelEvent.ExchangeRedeliveryEvent)      s.redeliveries.incrementAndGet();
else if (event instanceof CamelEvent.ExchangeFailureHandledEvent) s.failureHandled.incrementAndGet();
else if (event instanceof CamelEvent.ExchangeFailedEvent)
    if (exchange.getPattern().isOutCapable()) s.failedToCaller.incrementAndGet();   // the caller got it
    else s.failed.incrementAndGet();                                                // fire and forget: lost
else if (event instanceof CamelEvent.ExchangeCompletedEvent)  { s.completed...; s.completedAfterLastFault... }
```

Correlated copies made by an EIP (the task of a circuit breaker, a multicast or split part) are skipped, so a message
counts once. Each injected fault calls `markFault()`, which resets `completedAfterLastFault`. Invariants (section 8)
are checked here too, on each completed or failed exchange of their route. Health is sampled with
`HealthCheckHelper.invoke` when a rule is added, at its first fault, and when it ends; with camel-health present a
stalled route shows as DOWN in `healthDuring`. In the slack run the consumer routed m5 and m4: `completed` went up by 2
after the expectation was set.

### 5.7 Rule expiry

`expireRules` runs every 100 ms on a single scheduler thread, and also at the start of every report:

```java
if (r.getType() == ChaosRule.Type.STALL && (suspender != null && suspender.isAlive()
        || r.getRouteId() != null && routeStatus(r.getRouteId()) == ServiceStatus.Suspending)) {
    continue;                    // still finishing in-flight exchanges: resume it once it is suspended
}
if (r.isExpiredAt(now)) {
    endRule(r, r.getCalls() > 0 && r.getMatched() >= r.getCalls() ? "calls-reached" : "expired");
}
```

A stall is expired when `durationMs` has passed since the suspend was done; a fail or latency rule when its duration
has passed or it has seen `calls` matching calls (`roll` also refuses call N+1 itself, so a 5-call rule never injects
a 6th fault even between two scheduler runs). `endRule` takes the rule out of the active list, resumes the route for a
stall (or starts it if it was stopped), samples `healthAfter` and adds `rule-ended` to the timeline. In the slack run
the stall ends 500 ms after the suspend; the route resumes and polls again.

### 5.8 The report, the verdict and the hash

`report()` puts together the rules (with their counters and health), one entry per route, the expectations, the
invariants, the current health and the timeline. The verdict per route (`RouteStats.verdict`), in this order:

| Condition | Verdict | Result |
|---|---|---|
| no fault was injected on the route | `no-faults` | PASS (FAIL if an exchange failed unhandled anyway) |
| a rule that can affect the route is active | `chaos-active` | RUNNING |
| the route is not started | `NOT-RECOVERED (route ...)` | FAIL |
| nothing completed after the last fault | `NOT-RECOVERED (no exchange completed ...)` | FAIL |
| exchanges failed unhandled (fire and forget) | `RECOVERED, but N exchange(s) failed unhandled` | FAIL |
| request/reply failures went back to the caller | `RECOVERED (N request/reply failure(s) returned to the caller)` | PASS |
| otherwise | `RECOVERED` | PASS |

An expectation passes when `completed - baseline >= count` and reports `lostMessages = count - observed`; while a
rule is active it is RUNNING. An invariant fails as soon as one exchange violated it. The report's `result` is FAIL
if any route, expectation or invariant fails, else RUNNING while a rule is active, else PASS. These rules are short
enough to state as theorems; section 8.5 proves them for a transcription.

In the slack run the route counters see nothing wrong: two exchanges completed after the fault, none failed, so the
route says `RECOVERED`. Only the expectation sees the loss. The real text report of one run before the fix
(*(10-17)*, `logs-chaos/a1-slack-CAMEL-25684.txt`):

```
Chaos report (enabled=true, seed=25684)

Rules:
  suspend-mid-poll stall   mock:sink            state=ended  matched=2 injected=1 health before=UNKNOWN during=UNKNOWN after=UNKNOWN

Routes:
  slack        completed=3 failed=0 handledByErrorHandler=0 redeliveries=0 faults=1 completedAfterLastFault=1 lost=0 verdict=RECOVERED result=PASS

Expectations:
  slack        expected>=4 completed=2 lost=2 result=FAIL

Result: FAIL
```

On main the same run gives `completed=4 lost=0 result=PASS` for the expectation and `Result: PASS`. (`completed=3`
for the route counts the m1 exchange routed before the rule; the expectation counts from its baseline.) In JSON
(`--json`, or the console) each rule carries the fields of 5.4; an excerpt of a real report from the live demo
(`logs-chaos2/f-cli-live-report-run1.json`, the same rule figures as on 10-17):

```json
{ "id": "backend-30pct", "type": "fail", "pattern": "log:backend*", "probability": 0.3, "calls": 20,
  "state": "ended", "endReason": "calls-reached", "matched": 20, "injected": 4,
  "injectedPerRoute": { "orders": 4 }, "injectedCallNumbers": [6, 11, 14, 18],
  "healthBefore": { "state": "UP" }, "healthDuring": { "state": "UP" }, "healthAfter": { "state": "UP" } }
```

Last, `canonicalReport()` removes everything that depends on the wall clock (every `time` key and the current health),
sorts the keys at every level, and `canonicalReportSha256()` hashes the UTF-8 bytes:

```java
JsonObject r = report();
r.remove("healthNow");
return Jsoner.serialize(canonical(r));     // canonical(): TreeMap per object, drops "time"
```

Same seed and same sequence of calls give the same text and so the same SHA-256 (proof b: three fresh contexts,
seed 11, the same `338cb29a...bcd94`; **measured** again on 10-25 on the rebased branch: the same hash, so the
rebase and the switch to `TimeSource` did not change a byte of the canonical report). Inside a DST world even the
times are reproducible, so there the whole report can be compared (section 8.4).

## 6. What exists today *(checked on 72a1bff606a3)*

- `camel-test` (CamelTestSupport, `AdviceWith`, `interceptSendToEndpoint`, `NotifyBuilder`, mocks) lets a test author
  simulate a failure inside a test. Nothing lets a developer inject one into a running application from the CLI.
- `camel-test-dst` is on main now (stage 1, CAMEL-25738, 6f36673e85cf; stage 2, CAMEL-25911, c536c99caaaa). Its
  fakes inject seeded faults into the external system of a component under test; it is a test tool for Camel's own
  components, not something an application developer points at a running application.
- camel-jbang-mcp has 42 tools in `RuntimeTools` (grep of `@Tool(` on main; 41 on 10-17); `camel_runtime_route_control`
  can suspend a route, nothing fails or slows a call.
- The dev consoles and the CLI connector's file protocol (`~/.camel/{pid}-action-{id}.json`) already carry actions
  such as `trace`, `send`, `route-controller`; the chaos action is one more.

## 7. How it relates to DST, TLA+, runtime checks and external tools

In one line: a model (TLA+) says which invariant to check and which fault to inject, Z3 or an exhaustive model says
with which numbers, chaos checks the real route, DST replays what chaos can only sample, and Lean is used only on the
chaos tool's own verdict logic. Section 8 has the pipeline and the evidence.

| Tool | Finds | Relation |
|---|---|---|
| DST (`camel-test-dst`, on main since 10-25 with its time source) | timing bugs inside a consumer, by exploring interleavings of the real code under a seeded scheduler, in tests | DST is for Camel's own components and needs fakes; chaos is for an application developer's routes and needs nothing. Both use seeds and invariants; the chaos expectation ("N more exchanges complete") is DST's at-least-once invariant at runtime. Where chaos can only sample a window inside Camel's code (proof g1), DST replays it from a seed. And a chaos rule can now run inside a DST world, on the virtual clock (section 8.4, **measured**). |
| TLA+ models | protocol bugs, before code | The model's invariant becomes a chaos invariant and its counterexample a rule schedule; chaos then checks the real code on the real route. Done for `CircuitBreaker.tla` on a `circuitBreaker()` route: FAIL before CAMEL-25682, PASS on main (section 8.1, proof g1). |
| Z3 and exhaustive models | the parameter values where a fault starts to matter | They choose the rule's numbers: the failing-call count that opens a breaker (proof g2), the lock expiry a renewer misses (servicebus, model only) (section 8.2). |
| Lean | proofs about small pure logic | Not about the experiments; it proves the chaos tool's own verdict and rule counters, for a transcription (section 8.5, proof h). |
| Runtime invariant checks (consumer started but silent, checkpoint lag) | the same silent failures, in production | Complementary: chaos provokes the fault in dev, runtime checks detect it in prod. The chaos report's verdict logic is a candidate for such a check. |
| Toxiproxy | network faults (latency, cut connections) on a TCP proxy | Below Camel: real driver behaviour, reconnects. Optional adapter later: a rule `type=network` that drives a Toxiproxy proxy, with the same report. |
| Chaos Mesh | pod, network, IO faults in Kubernetes | For deployed systems; the report could read the same counters after a Chaos Mesh experiment. Adapter later, not a dependency. |
| Chaos Monkey for Spring Boot | latency, exceptions, app kill on Spring beans through AOP, controlled by Actuator endpoints | The closest in spirit (in-process, runtime control). It does not know Camel routes, endpoints, consumers or the error handler, and it gives no report of what happened to the messages. |

(External tools as described in their own documentation; not re-checked for this draft.) Section 8 shows how the
TLA+, Z3, DST and Lean work in camel-lean connects to chaos runs, with one pipeline run end to end.

## 8. From model to experiment: TLA+, Z3, DST and chaos together

The camel-lean workspace already has TLA+ models of several EIPs (`tla/circuitbreaker`, `tla/oncompletion`,
`tla/servicepool`, `tla/multicast`), exhaustive timing models, a DST prototype and 86 Lean files in `proofs/CamelLean`. This section
says where each one feeds a chaos run, shows one pipeline run end to end on a real Camel EIP, and says where a tool
does not help.

### 8.1 The pipeline

| Step | From | To | Example in this proposal | Status |
|---|---|---|---|---|
| 1 | a TLA+ invariant | a chaos **invariant** (a predicate every exchange must satisfy when it completes) or an **expectation** (a count) | `CircuitBreaker.tla` `SingleWriter` and `TimeoutAnswered` become the invariant "one story per exchange" | run (g1) |
| 2 | a TLC counterexample trace (`*_TTrace_*.tla`) | a rule schedule: which call, which fault, how long | the `SingleWriter` trace becomes a latency rule that ends the task at the timeout | run (g1) |
| 3 | Z3, or an exhaustive model | fault parameters that cross a threshold or a timing window | Z3: 5 failing calls open this breaker, 4 provably never do; servicebus: the lock expiry the renewer misses | breaker run (g2); servicebus model only, chaos **not run** |
| 4 | the same rule under DST | a deterministic replay inside Camel's own components | a ten-minute stall and a latency rule inside a DST world (i); the circuit breaker window | stall and latency: **run** (i); a TLC schedule replayed under DST: **not built** (8.4) |
| 5 | Lean | a proof about the chaos tool itself | verdict logic and rule counters | proved (h), for a transcription |

**Step 1, invariant.** A TLA+ invariant talks about model variables. A chaos invariant can only look at what an
exchange shows when it completes: body, headers, properties, exception. So the step is a projection, and it is worth
writing down what survives. For `CircuitBreaker.tla` (the timeout and fallback of `ResilienceProcessor`):

| Model | What the exchange shows |
|---|---|
| `writes = {"worker"}` | body of the task, `CamelCircuitBreakerResponseFromFallback` not true, `...TimedOut` not true |
| `writes = {"fallback"}` | body of the fallback, `...FromFallback = true`, `...SuccessfulExecution` not true |
| `SingleWriter` violated (`writes = {"worker","fallback"}`) | a mix: fallback body with `FromFallback=false`, or task body with `TimedOut=true`, or a cleared exception |
| `TimeoutAnswered` (`Done /\ timedOut => writes # {}`) | no exception on the exchange (with a fallback) |
| `NoConcurrentWrite` (`overlap`) | not observable as such; only its results are |

The prototype now has `ChaosManager.invariant(routeId, name, predicate)` (and `action=invariant` with a simple
predicate on the console). The notifier checks the predicate on every completed or failed exchange of the route and
the report counts `checked` and `violated`; one violation makes the report FAIL. The test writes the projection as a
small Java predicate (`ResilienceChaosTimeoutTest.oneStory`).

**Step 2, counterexample to schedule.** The TLC trace for `SingleWriter` (`tla/circuitbreaker/CircuitBreaker_TTrace_1790262934.tla`) is

```
1 Init          caller wait,      task run,      guard FALSE
2 CTimeout      caller fallback   (the time limiter fires; the task is still running)
3 WRun          task cas          (the task's processor returns)
4 WCas          task write        guard TRUE   (the task wins the guard)
5 WWrite        writes {worker}
6 CFallback     caller fbwrite    (guard.set(true): no compare-and-set)
7 CFbWrite      writes {worker, fallback}       <- SingleWriter violated
```

Steps 2 and 3 say "the task ends right after the timer fires": a latency rule on the protected endpoint with a delay
`D` such that the task ends at about the timeout `T`. Steps 4 to 6 (who wins the guard, and the fallback writing while
the task copies) happen inside `ResilienceProcessor`, where no endpoint seam reaches: chaos cannot order them, it can
only put the end of the task into the window and let concurrency sample the interleaving. The window is about one
`copyResults`, well below a millisecond, and the delay is in milliseconds, so the run sweeps `D` over 18 to 22 ms for
`T = 20 ms` with 32 callers, 4,000 calls per delay.

This is the general rule I would propose: when every step of a counterexample is an endpoint call or a route
lifecycle event, the trace becomes an exact schedule (the slack case: `stall atCall=2` is the counterexample, and it
fails 5 runs out of 5); when the trace has steps inside Camel's code (a compare-and-set, a copy), chaos can aim at the
window and count how often it hits, and a deterministic replay is DST's job (step 4).

**Result (proof g1).** `ResilienceChaosTimeoutTest` in camel-resilience4j, on a real `circuitBreaker()` route with a
fallback (resilience4j, sync mode, 64-thread timeout pool, breaker kept closed):

| Code | Runs | Report | Violations at completion (of 20,000) | Seen by the callers |
|---|---|---|---|---|
| main (with CAMEL-25682) | 3 | **3/3 PASS** | 0, 0, 0 | 0, 0, 0 |
| before CAMEL-25682 (`ResilienceProcessor.java` of 24cd63f10fb2^) | 3 | **3/3 FAIL** | 17, 21, 28 | 18, 30, 35 |

The route verdict says `RECOVERED, PASS` in all six runs; only the invariant sees the problem, as only the
expectation saw the slack loss. The callers see a few more violations than the completion check because the task
can still write after the exchange has completed, which is the model's point. Before the fix every delay of the sweep
produced some (1 to 10 per 4,000 calls; an 18 ms delay hits too, since thread hand-offs add a millisecond or two). This is a sampling result, not a replay: the count changes from run to run,
and a run with no violation would not prove the code correct. The JIRA draft's own probe (20,000 calls, task of
20 ms +/- 0.3 ms) found 26; the chaos run gets the same order with a declarative rule instead of a hand-written probe.

### 8.2 Choosing fault parameters: Z3 and exhaustive models

Some faults only matter at a threshold. `components/camel-chaos/models/fault_parameters_z3.py` (z3-solver 4.16.0):

- **Breaker threshold (run, proof g2).** For the route's configuration (COUNT_BASED window 10,
  `minimumNumberOfCalls` 10, `failureRateThreshold` 50 %) after 10 successful calls, Z3 minimises the number of
  consecutive failures that opens the breaker: **N = 5**, and proves (unsat) that **4 failures anywhere in 30 calls
  never open it**. The arithmetic is small enough to do by hand; the encoding takes the configuration as input, which
  is the point. `ResilienceChaosBreakerTest` feeds it to a `fail` rule with `calls=5`, plus a probe rule (a `fail`
  rule with probability 0, first on the endpoint, whose `matched` counter is the number of calls that reached it):
  the breaker opens on the 5th failure; while OPEN, 20 calls are answered by the fallback, all with
  `CamelCircuitBreakerResponseRejected=true`, and the probe stays at 15 (**no call reaches the protected endpoint**);
  after `waitDurationInOpenState` (1 s) one half-open call reaches it, succeeds, and the breaker is CLOSED, and the next
  10 calls all reach it (probe 26). With `calls=4` the breaker stays CLOSED and all 34 calls reach the endpoint. Both
  reports PASS, and the invariant "rejected => fallback body, backend not reached" holds on every exchange. These two
  properties come from resilience4j's documented state machine, not from our TLA+ model, which covers only the timeout
  and the fallback.
- **Servicebus lock renewal (model only).** The JIRA draft (`scripts/jira-1017/962.json`) used an exhaustive timing
  model: runs 10 or 11 s apart, every sequence of 6 gaps, lock expiry at every 100 ms. I re-implemented it next to a
  Z3 query. With the current 10 s margin Z3 finds a lock no run renews (gaps 11, 11, 10, 10, 11, 10 s, expiry at
  63.0 s, exactly on a run, which is too late); with a 20 s margin it proves no such expiry exists (unsat). The
  exhaustive count in my re-implementation is **5.7 %** lost (2,304 of 40,320) with 10 s and 0 with 20 s; the
  draft's model said 6.0 %. The difference is how the expiry phases at the edges of the horizon are counted; both say
  "a few percent per lock period, none with two intervals". The chaos rule that would follow is a latency rule that
  keeps an exchange of the Service Bus route in progress past `lockDuration - 10 s`. **Not run**: it needs a Service
  Bus namespace or a fake of the renewal client, and the renewer's timing comes from Camel's period task scheduler,
  which a chaos rule cannot steer. A DST run with a fake clock is the right tool here, which is how the draft's unit
  test reproduces it (Mockito, one renewer run).

### 8.3 What TLA+ adds, what chaos adds

TLA+ explores every interleaving of a small model and finds the bad one in seconds (`CircuitBreaker.tla`: 26 states).
It says nothing about the real code, and the model can be wrong (the JIRA draft records one wrong assumption about
microprofile fault tolerance, found by the test). Chaos runs the real code on the real route and finds whether the bad
interleaving is reachable, how often, and what the route does with it; it cannot prove its absence. Used together, the
model says which invariant to check and which fault to inject, and the chaos run says whether it happens in this
application.

The other models in `tla/` map less directly:

| Model | Invariant that would become a chaos check | Fault | Fit |
|---|---|---|---|
| `circuitbreaker` | one writer per exchange; a timeout is answered | latency at the timeout | **done** (g1) |
| `oncompletion` | every exchange's onCompletion runs exactly once, also when the route is interrupted or terminated | stall or fail on the route | plausible: an invariant on a header set by the onCompletion; not tried |
| `multicast` | every part's result is aggregated once; a timeout or stop does not lose or duplicate one | latency on one recipient, fail on another | plausible for timeouts (latency rule); the stop races are internal, DST's job; not tried |
| `servicepool` | a pooled producer is not used after stop | stall the route while calls are in flight | weak: the states are inside the pool; not tried |

### 8.4 DST: a chaos run inside a DST world

DST (`camel-test-dst`, on main since stage 1 and stage 2, see `dst-proposal.md`) runs a component's real code under a
seeded scheduler with fakes and a virtual clock. Every pool created through the `ExecutorServiceManager` runs on one
event loop, and since stage 2 the core code that waits or reads time does so through `TimeSource`, on which the run
installs its virtual clock. Chaos now uses the same two seams (section 4.6), so a chaos rule added inside a DST world
lives in the world's time.

**What it gives (measured, proof i).** `ChaosStallDstTest` (camel-chaos, `components/camel-chaos/src/test/java/
org/apache/camel/chaos/dst/`) builds a DST world with `DstCamel.newContext(dst)` and strict mode on. A client offers
one message per virtual second to `direct:orders -> mock:backend` and holds them back while the route is not started.
A latency rule makes half of the first 20 calls wait 2 seconds (`probability=0.5`, chaos seed = DST seed). At virtual
t = 5.5 s an operator task adds `stall routeId=orders duration=600000`, ten minutes. Results for seed 7:

| What | Number |
|---|---|
| route stalled at, resumed at (from the chaos timeline) | t = 5.5 s and t = 605.5 s: exactly 600,000 virtual ms |
| real time of the whole run | 43 ms (36 ms in the first build), 6,841 DST steps |
| report | PASS, 20 of 20 completed, expectation met, latency faults on calls [6, 7, 10, 11, 14, 16, 18, 20] |
| same seed again | the same DST trace SHA-256 and the same chaos report, byte for byte, **times included** |
| seed 8 | other latency faults ([1, 6, 9, 10, 11, 12, 13, 15, 16, 18]), another report hash |
| escapes in strict mode | none: no chaos clock read or sleep from a thread outside the harness |

Three things follow, and each is a step beyond what chaos alone could do on 10-17:

1. **Long faults cost nothing.** A stall of ten minutes, a rule that ends after an hour, a latency of 30 s per call
   all run in milliseconds. Outside DST the same test would take ten real minutes.
2. **The seed fixes the traffic too.** Outside DST the report is reproducible only "with the same sequence of calls"
   (debate, objection 7), and the times have to be cut out before hashing. Inside DST the scheduler is seeded as
   well, so the whole report, times included, is a function of the seed. A failing chaos scenario becomes a seed that
   replays exactly, which is what DST was built for.
3. **The suspend is inside the harness.** The suspend of a stall on a call ran on its own thread before the rebase;
   in strict mode that thread's clock read would fail the seed as an escape. It now runs on a pool of the
   `ExecutorServiceManager`, so it is a task of the seeded scheduler.

**What it does not give.**

- The trace hash names call sites, so it changes when the code changes: it changed between the two builds of 10-25,
  where the only code change was a guard added to `ChaosManager`. The report hash did not change.
- Only a plain route stall and a latency rule were run under DST. A stall on a call in the middle of a batch hits DST's
  one-stack limit: the suspend waits for an exchange that sits below it on the same stack (test-dst.adoc, "The
  one-stack model"). It is not tested under DST.
- Chaos keeps its own random stream per rule (`Random(seed * 1000003 + order)`), seeded here from the DST seed
  number. It does not draw from DST's own random source, so a chaos fault and a fault of the world's fake are not
  interleaved by one stream. Sharing the stream, and turning a TLC counterexample into a DST schedule for the circuit
  breaker window (where chaos samples 17 to 28 hits in 20,000 calls), are **not built**.
- The one-test evidence is small: one world, one route, three seeds. It shows the mechanism, not coverage.

For Camel's own components DST remains the deterministic counterpart of steps 2 and 3: on eventhubs it found that the
corner case raised in review of #27922 lost the last event(s) of a partition in **31 of 5000 seeds** (0.6 %), each
replaying with the same trace SHA-256 (dst-proposal, section 6). The `remove` rule of section 4.2 inside a DST world
would be the application-side twin of the lifecycle harness that found the bugs of section 1.1 (**design**).

### 8.5 Lean: only for the chaos tool itself

Lean adds nothing to the circuit breaker or servicebus experiments: those are about interleavings and timing, which
TLC and Z3 handle, and about real code, which only running it checks. Where Lean does fit is the part of the chaos tool
that must be right for the report to be trusted: the verdict and the rule counters. They are small and pure.
`proofs/CamelLean/ChaosVerdict.lean` (copy in `components/camel-chaos/models/ChaosVerdict.lean`) transcribes
`RouteStats.verdict`/`toJson`, `Expectation.toJson`, `report()`'s result, `ChaosRule.roll` and
`ChaosRule.triggerStall`, and proves (Lean 4.34, no `sorry`; axioms only `propext` and `Quot.sound`):

- `route_pass_sound`: a route PASSes only if it lost nothing and, when faults were injected, no rule is active, it is
  started and it completed exchanges after the last fault; `lost_never_passes`; `settled_route` (no RUNNING once no
  rule is active);
- `expect_pass_iff_nothing_lost`;
- `report_pass_sound` (PASS means no component fails and no rule is active) and `report_fail_monotone` (adding an
  expectation or invariant never turns a FAIL into a PASS);
- `rule_injects_at_most_calls` (for any traffic and any random draws, a rule with `calls=N` injects at most N faults),
  `certain_rule_exact` (a 100 % rule with `calls=N` injects exactly `min(N, calls seen)`: the "5 of 25" of proof d,
  for every N), `stall_fires_once`.

The limit is plain: the proofs are about a hand transcription, not the Java (no extraction), and the Java tests
(`ChaosVerdictTest`, `ChaosSafetyTest`, `ChaosInvariantTest`) are what tie the two. Writing them did not find a bug.
If the verdict grows (open question 3: asking the consumer whether it will redeliver), the theorems are the place to
keep it honest; until then they are cheap insurance, not a selling point.

## 9. Proof points

Laptop, JDK 21.0.11, 12 CPUs, Maven with `-Xmx2g`, offline. The column "When" says whether the number was measured
again on 2026-10-25 on the rebased branch (logs in `camel-lean/logs-chaos2/`, proof files in `logs-chaos2/proof/`) or
is the number of 10-17 (logs in `logs-chaos/`, archived). Nothing marked 10-25 changed against 10-17 except where
the row says so.

| # | Claim | Evidence | Numbers | When | Log |
|---|---|---|---|---|---|
| a1 | Finds a real bug, real code | `SlackConsumerChaosTest` (camel-slack), stall on the 2nd call of a poll of 4, expect 4; run on main and on `SlackConsumer.java` of 68329f0919bc^ | before CAMEL-25684: **5/5 runs FAIL, routed [m5, m4], 2 lost**; main: **5/5 PASS, routed [m5, m4, m3, m2]** | main: 10-25; before the fix: 10-17 | `proof/a1-slack-CAMEL-25684.txt`; before: `logs-chaos/a1-slack-*` |
| a2 | Flags a consumer dead while Started | `ChaosConsumerDeadWhileStartedTest`, a minimal reproduction of the CAMEL-25679 mechanism (not the mongodb component), server failed for 3 calls | old shape: thread ended, route Started, health UP, **expectation 0/20, FAIL**; fixed shape: **107/20, PASS** (123 on 10-17; the count depends on timing) | 10-25 | `proof/a2-consumer-dead-while-started.txt` |
| b | Reproducible | `ChaosReproducibilityTest`, 300 messages, 2 rules, 3 fresh contexts per seed | seed 11: **3/3 identical SHA-256** `338cb29a...bcd94`, 84 faults; seed 12: other schedule (103 faults), other SHA-256 `0b616751...07e7`; the same hashes as on 10-17 | 10-25 | `proof/b-reproducibility.txt`, `proof/b-report-seed*.json` |
| b' | Reproducible across JVMs | live demo, seed 7, three separate application runs, each starting with a reset | same failing calls `[6, 11, 14, 18]` in all three, and the same as on 10-17 | 10-25 | `f-cli-live-report-run{1,2,3}.json` |
| c | No cost when off | `ChaosOverheadTest`, `direct:a -> to(direct:b) -> setBody`, 200,000 exchanges per round, rotated order, A/A control | off: **+0.4 %** (40 rounds) and **+2.8 %** (20 rounds) vs baseline, A/A control +0.0 % / +0.2 %, round spread 5.2 % / 1.9 %; enabled, no rule: +6 to +9 %; a rule on another endpoint: +11 to +16 % | 10-17 (not re-run: the benchmark needs a quiet machine) | `logs-chaos/c-overhead-*` |
| d | Safe | `ChaosSafetyTest` (5 tests) | off by default (API, console, no "enable" action, hooks removed, cannot be switched on at runtime); refused with profile prod even when forced; a 200 ms rule expires; a 5-call rule fails exactly 5 of 25; remove/clear gives 10/10 routed with no latency left | 10-25 | `proof/d-safety.txt` |
| e | Verdicts are right | `ChaosVerdictTest`, 200 messages, 30 % of backend calls fail, seed 2026 | with 5 redeliveries: **PASS**, 90 faults, 90 redeliveries, 200/200 delivered; with a dead letter channel: **PASS**, 183 + 17 dead-lettered; without error handling: **FAIL, 56 lost** = 56 faults = 56 caller failures = 200 - 144 delivered | 10-25 | `proof/e-verdicts.txt` |
| g1 | A TLA+ invariant, checked by chaos, catches a real bug in real code | `ResilienceChaosTimeoutTest` (camel-resilience4j): `CircuitBreaker.tla` `SingleWriter`/`TimeoutAnswered` as the invariant "one story per exchange"; latency rule at the 20 ms timeout, swept 18-22 ms, 32 callers, 20,000 calls; run on main and on `ResilienceProcessor.java` of 24cd63f10fb2^ | before CAMEL-25682: **3/3 runs FAIL, 17 / 21 / 28 violations** (route verdict still RECOVERED); main: **PASS, 0 violations** (3/3 on 10-17, 1/1 on 10-25) | main: 10-25; before the fix: 10-17 | `proof/g1-circuitbreaker-tla.txt`; before: `logs-chaos/g1-*` |
| g2 | Z3 picks the fault parameter; state-machine properties checked from the report | `fault_parameters_z3.py` (N = 5 opens the breaker, 4 provably never); `ResilienceChaosBreakerTest`, `fail calls=5` and `calls=4`, a probe rule counting calls that reach the endpoint | calls=5: OPEN, 20 calls, **0 reach the endpoint**; after 1 s one half-open call succeeds, CLOSED, next 10 reach it; calls=4: CLOSED, 34/34 reach it; both PASS | test: 10-25; Z3 script: 10-17 | `proof/g2-breaker-z3.txt`; `logs-chaos/g2-z3-fault-parameters.txt` |
| h | The verdict logic says what it claims | `proofs/CamelLean/ChaosVerdict.lean`, 9 theorems on a transcription of the verdict, expectation, report result and rule counters | proved, no `sorry`, axioms `propext`, `Quot.sound` only; not a proof about the Java | 10-17 (the verdict code did not change) | `logs-chaos/h-lean-verdict.log` |
| f | Control works end to end | connector test with the real console; MCP tool unit test; live app driven by `camel cmd chaos` and by the MCP tool method | connector: 2/2; `RuntimeToolsTest` 15/15; CLI: 7 commands (reset, 3 rules, expect, report, stop), final `Result: PASS` in 3 of 3 runs, rules injected 4, 1 and 3 faults, 1 message dead-lettered by the error handler as it should, the stall shows `UP/DOWN/UP` with `route:orders` and `consumer:orders` down; MCP: process found, rule added, 3 faults, report back with `RESULT=PASS` | 10-25 | `build2-surfaces-and-proofs.mvn.log`, `build3-jbang.mvn.log`, `f-cli-live-run*.log`, `f-mcp-e2e.log`, `run-live-demo.sh` |
| i | A chaos run inside a DST world is on the virtual clock and replays from its seed | `ChaosStallDstTest` (section 8.4): a ten-minute stall and a latency rule in a strict DST world, seeds 7, 7, 8 | stall of **600,000 virtual ms in 43 ms** of real time; PASS 20/20; seed 7 twice: same DST trace and same report, times included; seed 8: other faults; no escape | 10-25 (new) | `proof/dst-stall.txt` |

Test totals on the rebased branch (10-25): camel-chaos 21 tests (the 20 of 10-17 plus `ChaosStallDstTest`; the
overhead benchmark is skipped unless `-Dchaos.bench=true`), camel-cli-connector `LocalCliConnectorChaosActionTest` 2,
camel-jbang-mcp `RuntimeToolsTest` 15, camel-slack `SlackConsumerChaosTest` 1 (5 repetitions), camel-resilience4j 3
(`ResilienceChaosBreakerTest` 2, `ResilienceChaosTimeoutTest` 1). 0 failures. The whole resilience4j module was not
run again (84 run, 0 failures on 10-17).

How the 10-25 builds ran: offline, through the shared Maven lock, `test -DskipITs`, with the core modules in the
reactor because `~/.m2` is older than main. camel-jbang-core and camel-jbang-mcp also needed tooling-model, the YAML DSL
validator and rest-openapi in the reactor, and `-Dmcp-java-sdk-version=2.0.1` (main asks for 2.1.0, which is not in
`~/.m2`). The live demo runs the freshly compiled classes ahead of the `~/.m2` jars; no module was installed.

What the numbers do not say:

- a2 is a reproduction of the mechanism in a 100-line consumer, labelled as such; the mongodb fix itself is now on
  main (3af2c90d63ad), and the chaos test does not run against it.
- g1 is sampling, not a replay: before the fix it hit 17 to 28 times in 20,000 calls, which is enough to fail every
  run so far but is not a guarantee. It needs concurrency and a delay at the timeout; with one caller it may find
  nothing. The breaker was kept closed on purpose (the run is about the timeout, not the state machine).
- The before-the-fix runs of a1 and g1 were not repeated on 10-25; only the main side was.
- g2's properties come from resilience4j's documented state machine, not from our TLA+ model.
- The servicebus step of section 8.2 is a model only; no chaos run.
- i is one small world; it shows the mechanism (virtual time, replay), not coverage of real components.
- The Lean theorems are about a transcription of the verdict code, not the Java.
- The overhead harness is a plain timing loop, not JMH. "Off" is within the noise in one run and 2.8 % (about
  19 ns per exchange on a 670 ns no-op route) in the other; I would call it "not measurable on a route that does I/O",
  not "zero". The `TimeSource` switch does not touch the path that runs when chaos is off (no active rule: one
  volatile read), but that was not measured again.
- The MCP tool was called as a Java method against a live application, not through an MCP client and transport, and
  without the `@McpSecured` interceptor.
- The section 1.1 column "rule that would have exposed it" is analysis; no chaos run was made on those components.
- On the way, the overhead harness found two costs of the first version, both fixed: matching the endpoint pattern on
  every call (+75 %, now cached per URI) and the event notifier left in place when chaos is off (+9 %, now removed
  at startup, together with the context's "events applicable" flag it had switched on).

## 10. The prototype

Branch `camel-chaos-prototype`, rebased onto main 72a1bff606a3 on 2026-10-25 (it was 6 commits on
9fb9bc911f78 and 440 commits behind). Five commits:

1. the module: rules, seeds, expectations, invariants, report (`components/camel-chaos`, parent pom, catalog pom);
2. the control surfaces: the `chaos` action in camel-cli-connector, `camel cmd chaos` (`CamelChaosAction`), the MCP
   tool `camel_runtime_chaos`, their docs and the jbang commands metadata;
3. the proof tests in camel-slack and camel-resilience4j;
4. README, DEMO, the module copy of this proposal, the live demo scripts, the models (TLA+, Z3, Lean) and the
   explainer video;
5. `TimeSource`, the DST test (section 4.6, 8.4) and this version of the proposal.

About 1,725 lines of main code in `components/camel-chaos` (6 classes), 2,250 lines of tests (with the slack and
resilience4j ones). The rebase had one conflict: `camel-jbang-commands-metadata.json`, a generated file. It was
resolved by keeping main's version and adding the `cmd chaos` entry; the camel-jbang-core build regenerated the file
offline and produced no difference. Two small fixes came with the rebase: the license check now runs on the models
directory and did not know `.lean` and `.tla` (they carry the ASF header in their own comment syntax, so the module
excludes them), and the stall on a call refuses cleanly when the manager has not started.

`components/camel-chaos/PROPOSAL.md` on the branch is a copy of this proposal. The module's `README.md` and `DEMO.md`
still describe the state of 10-17 (base 9fb9bc911f78, `install` from the Apache snapshot repository); the offline
commands of 10-25 are in `logs-chaos2/run-live-demo.sh` and section 9.

## 11. Limits and open questions

Limits:

- Only `EndpointAware` sends are wrapped: `to`, not `toD`, `enrich`, `pollEnrich`, a dynamic `wireTap`, nor calls a
  bean or a component's client makes by itself. Faults inside a component (a repository store, a driver reconnect)
  are out of reach; CAMEL-25681 is an example.
- Latency is a sleep in the calling thread (`TimeSource.sleep`: a real sleep outside DST, a virtual wait inside).
- No route lifecycle rule beyond the stall: removing a route and adding it back (`remove`, section 4.2) is design, so
  the bugs of section 1.1 cannot be provoked by the prototype except the stop half of CAMEL-26303.
- Under DST, only a plain route stall and latency were run; a stall on a call in the middle of a batch meets DST's
  one-stack limit (section 8.4).
- A stall suspends the route; it does not take the broker away, so the consumer's own reconnect code is not exercised
  (that is Toxiproxy's or Testcontainers' job).
- No thread-pool faults yet (`ThreadPoolFactory`: rejections, slow pools).
- The verdict is a heuristic over counters. It sees exchanges that fail unhandled; it does not see loss downstream of
  a successful call. Expectations fill part of the gap, and need the developer to say how many messages to expect.
- Enabling is decided at startup, so a long-running dev instance must be started with chaos enabled.
- Invariants are new: Java API and the dev console (`action=invariant`, a simple-language predicate) only; the CLI
  connector, `camel cmd chaos` and the MCP tool do not pass a predicate yet. They are checked when the exchange
  completes, so a write that lands after completion (the circuit breaker race) is seen only by the caller.

Open questions:

1. Home: a `components/camel-chaos` module (as prototyped), part of camel-cli-connector, or a camel-jbang plugin that
   adds the jar to `camel run` when asked (`camel run --chaos`)?
2. Should rules be loadable from a file at startup (`camel.chaos.rules=...`), to make a chaos scenario part of a test
   run?
3. Is "fire and forget failed unhandled = lost" the right default for the verdict? JMS with transactions and the file
   consumer retry by themselves; the report could ask the consumer whether it will redeliver.
4. One MCP tool with an `action` argument, or separate tools (`add`, `report`), which suits some assistants better?
5. Should the canonical report and its SHA-256 be the format of a "chaos baseline" checked into a project?
6. Should a chaos rule be usable as a fault source of a DST world in Camel's own tests (a `remove` rule replayed over
   seeds is close to the lifecycle harness that found section 1.1), or should the two stay separate tools that only
   share seams and vocabulary?

## 12. The debate

The strongest objections a PMC member could raise, and the answers.

| # | Objection | Answer |
|---|---|---|
| 1 | "Fault injection in a runtime jar is a production risk." | It is off unless enabled at startup; when off, the hooks are removed at startup (proof d, and c: no measurable cost); it cannot be switched on at runtime by any surface; profile `prod` refuses it even when enabled. The safest place is still a dev dependency or `camel run --dep`, never the production build. |
| 2 | "Users can already do this with `interceptSendToEndpoint` or `AdviceWith` in tests." | In a test, yes, if they write it. Not in a running application, not from the CLI, not reproducibly, and with no report. The value is the report: proof e shows the same 30 % fault giving PASS with redelivery and FAIL with 56 counted lost messages without it. |
| 3 | "Toxiproxy and Chaos Mesh exist." | They break the network, not Camel. They do not know which route lost what, or whether the error handler fired. The proposal keeps them as optional adapters (section 7) that reuse this report. |
| 4 | "The verdict will be wrong for some consumers and give false confidence." | It can be, and the report says what it counted. The prototype already separates request/reply failures (returned to the caller) from fire and forget ones (lost), and dead-lettered from lost. Expectations exist because counters alone missed the slack bug: in proof a1 the route verdict said RECOVERED and only the expectation caught the 2 lost messages. Open question 3 is about doing better. |
| 5 | "An MCP tool that breaks things is dangerous in the hands of an AI." | The tool is marked destructive; it only works on an application that was started with chaos enabled; rules end by themselves. Camel's security model (`security-model.adoc`, "Developer tools") already treats the CLI, the TUI and MCP clients as developer tools on the developer's own machine. |
| 6 | "Maintenance: another module, another console, another tool." | About 1,725 lines on existing SPIs, no core change; the only core class it uses beyond the SPIs is `TimeSource`, which is already there for DST. The control surfaces are thin (one console, one connector action, one CLI command, one MCP tool). |
| 7 | "Seeds are only reproducible with the same traffic; real traffic is not." | True for a live application: same seed and same sequence of calls give the same faults (proof b, b'), and with live traffic the schedule shifts with the order of calls, so a live run is "the same kind of run". For a test it is exact, and inside a DST world the seed fixes the traffic and the times as well: the whole report replays byte for byte (proof i). |
| 8 | "Why not do this in DST instead?" | DST explores interleavings inside Camel's own components with fakes, as a Camel test tool. Chaos is for application developers, on their real routes, with no fakes. They share no code, but since stage 2 they share the seams (`TimeSource`, the `ExecutorServiceManager`), so a chaos rule can run inside a DST world when a test wants both (section 8.4). |

## 13. Phases

| Phase | Content | Done when |
|---|---|---|
| 0 (done) | Prototype: module, console, connector action, CLI command, MCP tool, proof points; rebased on 10-25 and moved to `TimeSource` | Section 9 |
| 1 | camel-jbang integration: `camel run --chaos` adds the module and enables it; rules from a file; JSON report written at stop | A developer runs a chaos scenario on an existing project with one flag |
| 2 | Docs: a page in the user manual (`chaos.adoc`), the CLI command page, the MCP tool docs, catalog entry, BOM | A user finds it from the CLI docs and the MCP tool list |
| 3 | More seams: the `remove` route rule, `toD`/dynamic sends, thread-pool faults through `ThreadPoolFactory`, consumer poll faults for the main polling components; TUI Chaos tab; tool in the shared `ToolRegistry` | The slack, mongodb and pg-replication-slot shapes, and the shared-resource class of section 1.1, can be provoked without a reproduction |
| 4 (optional) | Adapters: Toxiproxy (network faults, same report), Testcontainers pause/unpause of a service, Chaos Mesh report reader | Decided on dev@ after phase 3 |

## 14. Effort

| Phase | Estimate |
|---|---|
| 1 | 1 to 2 weeks (camel-jbang run options, rules file, report at stop, tests) |
| 2 | 1 week (docs, catalog, generated pages) |
| 3 | 3.5 to 4.5 weeks (`remove` rule 0.5, dynamic sends 1, thread pools 1, poll faults per component 1 to 2, TUI tab 1) |
| 4 | 1 to 2 weeks per adapter |

Review cost for committers: phase 1 and 2 touch only the new module and camel-jbang; nothing in core.
