# Models

One TLA+ module per route. The comment at the top of each module names the route it models, where it comes from, and
which Camel behaviour it encodes. The constants are the knobs (options of the route, and the bounds of the
environment); `run_all.sh` lists every combination that was checked.

| Module | Route | Properties |
|---|---|---|
| `R1_IdempotentFileHttp` | file, idempotent consumer, costly HTTP call (camel-quarkus-examples `jpa-idempotent-repository`) | NoLoss, NoDuplicateEffect |
| `R2_FileSplitAggregateSeda` | file, split, in-memory aggregate, seda, SFTP (camel-quarkus-examples `file-bindy-ftp`) | NoLoss |
| `R3_KafkaToSedaBuffer` | Kafka into a seda buffer (camel-quarkus-examples `kafka`) | NoLoss |
| `R4_AggregateMemoryRestart` | aggregate with a memory repository across a restart (camel-examples `aggregate`) | ResumesAfterRestart |
| `R5_AggregateDistOptimistic` | two instances aggregating into one JDBC repository (camel-examples `aggregate-dist`) | NoLostUpdate |
| `R6_JtaJmsJpa` | transacted JMS send and JPA write under XA (camel-quarkus-examples `jms-jpa`) | Atomic |
| `R7_SedaRequestReply` | request-reply over seda (seda component docs), and a cycle variant | NoDeadlock |

Conventions: a crash means the JVM dies (no `onCompletion`, memory lost); a graceful stop is `CamelContext.stop()`
with the default shutdown strategy. Bounds are one crash and/or one stop and two or three messages. Properties are
checked at rest. "HOLDS" means no counterexample within these bounds.

## Run

Needs Java 11 or later and `tla2tools.jar` from https://github.com/tlaplus/tlaplus/releases.

```
TLA2TOOLS=/path/to/tla2tools.jar ./run_all.sh            # all checks, one TLC at a time, about a minute
TLA2TOOLS=/path/to/tla2tools.jar ./run.sh R1_IdempotentFileHttp eagerJpa NoLoss \
    Eager=TRUE PersistentRepo=TRUE MaxCrashes=1 MaxFailures=1   # one check
```

`run.sh` writes the TLC config to `cfg/` and the TLC output to `out/` (both ignored by git), and prints one result
line. `run_all.sh` also renders a story for each violation into `stories/`, using `narr_R*.json` (one plain-language
line per action of the model) and `../tools/story.py`.

`RESULTS.txt` is the output of `run_all.sh`. TLC runs with one worker, so the results and the stories are the same on
every run. For a violation, the state count is the number of states TLC had seen when it stopped.
