# camel-chaos: a fault injector for Camel developers (prototype)

**Status: prototype for discussion, not a pull request.** It lives on the branch `camel-chaos-prototype` of
github.com/allthingssecurity/camel, on top of apache/camel `main`. It is meant for dev and test only: it is off by
default, it refuses to run with the `prod` profile, and nothing changes unless you add the jar to your application.

## The problem

Most Camel applications have an error handler, redeliveries, a dead letter channel, health checks and maybe a circuit
breaker. Whether they actually work is usually found out in production, the first time the backend is slow or a
broker goes away. A developer who wants to check it today has to stop a container by hand, or write a test that
mocks the failure, and then look at logs to guess what happened.

Claus asked on Zulip for a chaos injector that a developer can drive from Camel CLI, from the TUI, or from an AI
assistant through an MCP tool, to see whether the application is robust.

## The idea

1. **Inject faults at seams Camel already has.** No change to Camel core and no change to the routes:
   - calls to endpoints (`to(...)`) are wrapped through the existing `InterceptStrategy` SPI, so a rule can make a
     call fail or slow down;
   - a consumer can be stalled through the `RouteController` (suspend the route, resume it later).
2. **Small rules, added and removed at runtime.** "30% of the calls to `http*` fail, for the next 20 calls",
   "add 2 s to `kafka:*` for one minute", "stall the consumer of route `orders` for 5 s". Rules end by themselves
   after a number of calls or a duration.
3. **A seed decides which calls fail.** Each rule draws from its own random source made from the seed and the rule
   order, so the same seed and the same traffic fail the same calls. In the demo, two separate JVM runs with seed 7
   failed exactly calls 6, 11, 14 and 18 of the first rule.
4. **The report is the point.** For each rule: how many calls matched, how many faults were injected, and the health
   check state before, during and after. For each route: exchanges completed, failed unhandled (possible message
   loss), handled by the error handler (dead letter channel, `onException`), redeliveries, and a verdict:
   `RECOVERED`, `RECOVERED, but N exchange(s) failed unhandled`, or `NOT-RECOVERED` (route not started, or nothing
   completed after the last fault).

```
$ camel cmd chaos chaos-demo --command=add --type=fail --pattern=log:backend* --probability=30% --calls=20
$ camel cmd chaos chaos-demo --command=add --type=stall --route-id=orders --duration=3000
$ camel cmd chaos chaos-demo
Chaos enabled=true seed=7
  rule backend-30pct  fail    log:backend*         ended  matched=20 injected=4 health UP/UP/UP
  rule stall-orders   stall   orders               ended  matched=1 injected=1 health UP/DOWN/UP
  rule backend-down   fail    log:backend*         ended  matched=3 injected=3 health UP/UP/UP
  route orders         completed=68 failed=0 errorHandled=1 redeliveries=6 faults=8 -> RECOVERED
```

(Real output of the demo in [`DEMO.md`](DEMO.md): 30% failures were all recovered by redelivery, the stall took the
`route:orders` and `consumer:orders` health checks DOWN and back UP, and three failures in a row sent one exchange to
the dead letter channel.)

## How it hooks in

| Class | Role |
|---|---|
| `ChaosContextServicePlugin` | found through the `ContextServicePlugin` service loader; installs the manager when the jar is on the classpath |
| `ChaosManager` | the rules, the seed, the `InterceptStrategy`, an `EventNotifier` for the statistics, health sampling, the report |
| `ChaosProcessor` | wraps each processor that sends to an endpoint (`EndpointAware`); with no active rule it reads one volatile list |
| `ChaosRule` | one rule: `fail`, `latency` or `stall`, probability, number of calls, duration, exception class |
| `ChaosDevConsole` | the `chaos` dev console: `action=add/remove/clear/reset/report`, JSON and text |

The control surfaces all end in the dev console:

| Surface | How | Wired in this prototype |
|---|---|---|
| dev console | `chaos` console, `/q/dev/chaos?action=...` over HTTP | yes, tested |
| CLI connector | new action `chaos` in `LocalCliConnector`, passes the options to the console | yes, tested end to end with the real console |
| Camel CLI | `camel cmd chaos [name] --command=add ...` (`CamelChaosAction`) | yes, run against a live application (DEMO.md) |
| MCP | tool `camel_runtime_chaos` in `RuntimeTools` of camel-jbang-mcp, same action as the CLI | yes, compiled and its argument checks unit tested; not run from an MCP client |
| TUI | nothing yet | no |

Enabling: add `camel-chaos` to the application and set `camel.chaos.enabled=true` (property, system property or
`CAMEL_CHAOS_ENABLED`). Without it every `add` is refused. With `camel.main.profile=prod` it is refused even when the
property is set. `camel.chaos.seed` sets the seed (default 42).

## Tests

All in-JVM, no external service:

- `ChaosManagerTest` (8 tests): installed from the classpath and off by default; never on with profile `prod`; 30%
  failures on 100 exchanges are redelivered or dead-lettered and the counters add up (completed + handled = sent);
  "next 3 calls fail" with a chosen exception; same seed gives the same failing calls and another seed does not;
  latency; expiry after a duration; a stalled timer consumer is resumed and the verdict is `RECOVERED`.
- `ChaosDemoTest`: the demo driven only through the dev console: health `UP`, `DOWN` during the stall, `UP` after.
- `LocalCliConnectorChaosActionTest` (camel-cli-connector): an action file with `action=chaos` reaches the real
  console, injects two failures into a route and the report comes back through the connector.
- `RuntimeToolsTest` (camel-jbang-mcp): argument checks of `camel_runtime_chaos`.

## Limits

- Only processors that know their endpoint (`to`, `EndpointAware`) are wrapped. `toD`, `enrich`, `pollEnrich`,
  `wireTap` with dynamic URIs and calls made inside a bean or a component's own client are not.
- Latency is a `Thread.sleep` in the calling thread, so it also slows an async route down in the wrong place.
- A stall suspends the route; a consumer that does not support suspension is stopped and started again instead. It
  is not a "broker went away" fault: the consumer's own reconnect code is not exercised.
- No thread-pool faults yet (rejections, slow pools through `ThreadPoolFactory`), no faults inside a consumer's poll.
- The rules are wired when the routes are built: routes added before the manager is installed are not wrapped. With
  the jar on the classpath this does not happen, since the plugin runs while the context is built.
- The verdict is a heuristic over counters. It does not know whether a message was lost downstream, only whether an
  exchange failed without being handled.
- Not yet in the catalog, the BOM or the website docs; the generated `jbang-commands` and MCP docs pages are updated.

## Next steps

1. Thread faults through `ThreadPoolFactory` / `ExecutorServiceManager`: reject or delay tasks of a named pool.
2. Consumer-side faults: fail or slow down a poll, drop a connection (per component, starting with JMS, Kafka, file).
3. `toD` and other dynamic sends (wrap `SendDynamicProcessor` by its resolved URI).
4. A TUI panel next to the existing ones, and a short "chaos session" that runs a scenario and writes the report.
5. Assertions for tests: `ChaosManager.report()` is already usable from JUnit; a small `@ChaosTest` helper could make
   "this route survives 30% backend failures" a one-line test.

## Feedback welcome

Questions this prototype is meant to answer: is a dev-only `camel-chaos` module (or a part of camel-cli-connector) the
right home; are endpoint calls and consumer stalls the right first faults; and is the report what a developer wants to
see? Comments are welcome on the Camel Zulip.
