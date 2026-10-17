# camel-chaos prototype: commands to reproduce the demo

Branch `camel-chaos-prototype` on github.com/allthingssecurity/camel (based on apache/camel main 9fb9bc911f78).
The module is `components/camel-chaos`. JDK 21, Maven through `./mvnw`. The 4.24.0-SNAPSHOT core jars come from the
Apache snapshot repository (`-P apache-snapshots`); only the modules changed on this branch are built locally.

```bash
git clone https://github.com/allthingssecurity/camel.git && cd camel
git checkout camel-chaos-prototype
export MAVEN_OPTS=-Xmx2g
```

## 1. Unit tests and the in-JVM demo

```bash
./mvnw -B -P apache-snapshots -ntp -pl components/camel-chaos install
# ChaosManagerTest: Tests run: 8, Failures: 0
# ChaosDemoTest:    Tests run: 1, Failures: 0
cat components/camel-chaos/target/chaos-demo-report.txt
cat components/camel-chaos/target/chaos-demo-report.json
```

`ChaosDemoTest` runs a timer route with a dead letter channel and drives everything through the `chaos` dev console:
30% failures on the next 20 backend calls, then the consumer stalled for 600 ms, then the report.

## 2. The CLI connector, Camel CLI and the MCP tool

```bash
./mvnw -B -P apache-snapshots -ntp -pl dsl/camel-cli-connector,dsl/camel-jbang/camel-jbang-core install -DskipTests
./mvnw -B -P apache-snapshots -ntp -pl dsl/camel-cli-connector test -Dtest=LocalCliConnectorChaosActionTest
# Tests run: 2, Failures: 0
./mvnw -B -P apache-snapshots -ntp -pl dsl/camel-jbang/camel-jbang-mcp test -Dtest=RuntimeToolsTest
# Tests run: 15, Failures: 0   (2 of them for camel_runtime_chaos)
```

## 3. Live demo: a running application, driven by `camel cmd chaos`

`components/camel-chaos/demo` has a YAML route (timer, then `log:backend`, with a dead letter channel and 2
redeliveries), an `application.properties` with `camel.chaos.enabled=true` and `camel.chaos.seed=7`, and two small
scripts: `run-app.sh` starts it with `camel-main` (plus camel-cli-connector and camel-chaos), `camel.sh` runs Camel CLI
from the camel-jbang-core jar built in step 2, so the launcher does not need to be built.

```bash
cd components/camel-chaos/demo
./prepare.sh                         # writes app.cp and cli.cp
./run-app.sh > app.log 2>&1 &        # the application, name chaos-demo

# 30% of the next 20 backend calls fail
./camel.sh cmd chaos chaos-demo --command=add --type=fail --pattern='log:backend*' --probability=30% --calls=20 --id=backend-30pct

# stall the consumer for 3 s (health goes DOWN, then UP)
./camel.sh cmd chaos chaos-demo --command=add --type=stall --route-id=orders --duration=3000 --id=stall-orders

# three failures in a row: with 2 redeliveries, one exchange goes to the dead letter channel
./camel.sh cmd chaos chaos-demo --command=add --type=fail --pattern='log:backend*' --calls=3 --exception=java.io.IOException --id=backend-down

# the report (text, or --json for everything, including the timeline)
./camel.sh cmd chaos chaos-demo
./camel.sh cmd chaos chaos-demo --json

./camel.sh stop chaos-demo
```

Output of that run (each command waits for the previous rule to end):

```
added rule backend-30pct
  rule backend-30pct  fail    log:backend*         active matched=0 injected=0 health UP/-/-
  route orders         completed=10 failed=0 errorHandled=0 redeliveries=0 faults=0 -> no-faults
added rule stall-orders
  rule backend-30pct  fail    log:backend*         ended  matched=20 injected=4 health UP/UP/UP
  rule stall-orders   stall   orders               active matched=1 injected=1 health UP/DOWN/-
  route orders         completed=30 failed=0 errorHandled=0 redeliveries=4 faults=5 -> chaos-active
added rule backend-down
  ...
Chaos enabled=true seed=7
  rule backend-30pct  fail    log:backend*         ended  matched=20 injected=4 health UP/UP/UP
  rule stall-orders   stall   orders               ended  matched=1 injected=1 health UP/DOWN/UP
  rule backend-down   fail    log:backend*         ended  matched=3 injected=3 health UP/UP/UP
  route orders         completed=68 failed=0 errorHandled=1 redeliveries=6 faults=8 -> RECOVERED
```

and in `app.log`, the dead letter channel: `WARN dead - Exchange[... Body: order-51]`.

In the JSON report the stall shows which checks went down (`"down": ["route:orders", "consumer:orders"]`) and the
timeline lists `rule-added`, `route-stalled`, `route-resumed`, `rule-ended` with the health at each step.

Reproducibility: the same command sequence in two separate JVMs with seed 7 failed the same calls of the first rule,
`"injectedCallNumbers":[6,11,14,18]` both times.

## 4. From an AI assistant (MCP)

With camel-jbang-mcp running (`camel mcp`), the assistant calls, for the same application:

```json
{ "tool": "camel_runtime_chaos",
  "arguments": { "nameOrPid": "chaos-demo", "action": "add", "type": "fail",
                 "pattern": "log:backend*", "probability": "30%", "calls": "20" } }
{ "tool": "camel_runtime_chaos", "arguments": { "nameOrPid": "chaos-demo" } }
```

The tool sends the same `chaos` action as `camel cmd chaos`. In this prototype it was compiled and its argument
checks were unit tested; it was not run from an MCP client.

## 5. Over HTTP (dev console)

With `camel.server.enabled=true` and `camel.server.devConsoleEnabled=true` (not run in this prototype):

```bash
curl 'http://localhost:8080/q/dev/chaos?action=add&type=latency&pattern=log:backend*&delay=500&duration=10000'
curl 'http://localhost:8080/q/dev/chaos'
```
