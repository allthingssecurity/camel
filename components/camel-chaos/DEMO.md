# camel-chaos prototype: commands to reproduce the demo and the proof points

Branch `camel-chaos-prototype` on github.com/allthingssecurity/camel (based on apache/camel main 9fb9bc911f78).
The module is `components/camel-chaos`. JDK 21, Maven through `./mvnw`. The 4.24.0-SNAPSHOT core jars come from the
Apache snapshot repository (`-P apache-snapshots`); only the modules changed on this branch are built locally.

```bash
git clone https://github.com/allthingssecurity/camel.git && cd camel
git checkout camel-chaos-prototype
export MAVEN_OPTS=-Xmx2g
LOGS=$PWD/../logs-chaos && mkdir -p "$LOGS"      # where the proof logs go
```

## 1. camel-chaos tests and proof points b, c, d, e, a2

```bash
./mvnw -B -P apache-snapshots -ntp -pl components/camel-chaos install \
  -Dchaos.bench=true -Dchaos.bench.rounds=40 -Dchaos.proof.dir=$LOGS
# Tests run: 19, Failures: 0
ls $LOGS   # a2-consumer-dead-while-started.txt b-reproducibility.txt b-report-seed*.json c-overhead.txt d-safety.txt e-verdicts.txt
```

Without `-Dchaos.bench=true` the overhead harness is skipped (18 tests run). `ChaosDemoTest` writes
`target/chaos-demo-report.txt` and `.json`.

## 2. Proof point a1: the slack bug, before and after CAMEL-25684

```bash
# on main: the report says PASS, 4 of 4 messages routed (5 repetitions)
./mvnw -B -P apache-snapshots -ntp -pl components/camel-slack test -Dtest=SlackConsumerChaosTest \
  -Dchaos.proof.dir=$LOGS -Dchaos.codeLabel=main

# the consumer from before the fix: the report says FAIL, 2 lost (5 repetitions)
git checkout 68329f0919bc^ -- components/camel-slack/src/main
./mvnw -B -P apache-snapshots -ntp -pl components/camel-slack test -Dtest=SlackConsumerChaosTest \
  -Dchaos.proof.dir=$LOGS -Dchaos.expect=FAIL -Dchaos.codeLabel=before-CAMEL-25684
git checkout HEAD -- components/camel-slack/src/main

grep "routed after\|^Result" $LOGS/a1-slack-CAMEL-25684.txt | sort | uniq -c
#   5 Result: FAIL
#   5 Result: PASS
#   5 routed after the burst: [m5, m4, m3, m2]
#   5 routed after the burst: [m5, m4]
```

## 3. The CLI connector and the MCP tool

```bash
./mvnw -B -P apache-snapshots -ntp -pl dsl/camel-cli-connector,dsl/camel-jbang/camel-jbang-core install -DskipTests
./mvnw -B -P apache-snapshots -ntp -pl dsl/camel-cli-connector test -Dtest=LocalCliConnectorChaosActionTest
# Tests run: 2, Failures: 0
./mvnw -B -P apache-snapshots -ntp -pl dsl/camel-jbang/camel-jbang-mcp test -Dtest=RuntimeToolsTest
# Tests run: 15, Failures: 0   (2 of them for camel_runtime_chaos)
```

## 4. Live demo: a running application driven by `camel cmd chaos` and by the MCP tool

`components/camel-chaos/demo`: a YAML route (timer, then `log:backend`, dead letter channel with 2 redeliveries),
`application.properties` with `camel.chaos.enabled=true` and `camel.chaos.seed=7`, `run-app.sh` (the application with
camel-main, camel-cli-connector and camel-chaos), `camel.sh` (Camel CLI from the camel-jbang-core jar, no launcher
needed) and `mcp-e2e.sh` (calls the MCP tool method `camel_runtime_chaos` against the application).

```bash
cd components/camel-chaos/demo
./prepare.sh                                           # target/app.cp, target/cli.cp
../../../mvnw -q -pl dsl/camel-jbang/camel-jbang-mcp -f ../../../pom.xml dependency:build-classpath \
  -Dmdep.outputFile=$PWD/target/mcp.cp                 # for mcp-e2e.sh (after step 3)
./run-app.sh > $LOGS/f-live-app.log 2>&1 &             # the application, name chaos-demo

./mcp-e2e.sh                                           # MCP: add a rule, expect, report -> RESULT=PASS

./camel.sh cmd chaos chaos-demo --command=reset
./camel.sh cmd chaos chaos-demo --command=add --type=fail --pattern='log:backend*' --probability=30% --calls=20 --id=backend-30pct
./camel.sh cmd chaos chaos-demo --command=add --type=stall --route-id=orders --duration=3000 --id=stall-orders
./camel.sh cmd chaos chaos-demo --command=expect --route-id=orders --count=30
./camel.sh cmd chaos chaos-demo --command=add --type=fail --pattern='log:backend*' --calls=3 --exception=java.io.IOException --id=backend-down
./camel.sh cmd chaos chaos-demo                        # the report (--json for everything, with the timeline)
./camel.sh stop chaos-demo
```

Output of the last report (each command waited for the previous rule to end):

```
Chaos enabled=true seed=7
  rule backend-30pct  fail    log:backend*         ended  matched=20 injected=4 health UP/UP/UP
  rule stall-orders   stall   orders               ended  matched=1 injected=1 health UP/DOWN/UP
  rule backend-down   fail    log:backend*         ended  matched=3 injected=3 health UP/UP/UP
  route orders         completed=116 failed=0 errorHandled=1 redeliveries=6 faults=8 -> RECOVERED [PASS]
  expect orders        >=30 completed=68 lost=0 [PASS]
Result: PASS
```

In the JSON report the stall lists the checks that went down (`"down": ["route:orders", "consumer:orders"]`).
Three separate application runs with seed 7 (the last one after `--command=reset`, which restarts the rule order) failed the same calls of the 30 % rule, `[6, 11, 14, 18]`.

A stall on the Nth call, in the middle of routing: `--type=stall --pattern='log:backend*' --at-call=3 --duration=2000`.

## 5. Over HTTP (dev console, not run in this prototype)

With `camel.server.enabled=true` and `camel.server.devConsoleEnabled=true`:

```bash
curl 'http://localhost:8080/q/dev/chaos?action=add&type=latency&pattern=log:backend*&delay=500&duration=10000'
curl 'http://localhost:8080/q/dev/chaos'
```
