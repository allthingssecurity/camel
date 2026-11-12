# Confirmation tests

Each test runs the route of a model on released Camel 4.23.0 and checks that the real components do what the model
says. They need Java 17 or later and nothing else: no broker, no database server, no network service. This is a
standalone Maven project, not a module of the Camel build.

```
./mvnw test
```

| Test | Model | What it shows |
|---|---|---|
| `R1IdempotentCrashTest` (+ `CrashApp`) | R1 | A child JVM is halted at the crash point and the route is started again. eager=true: the API log ends as `[A]` (B lost). eager=false, crash after the call: `[A, B, B]` (B twice). No crash: `[A, B]`. |
| `R2FileSplitAggregateStopTest` | R2 | With the defaults, a stop within the completion interval uploads nothing and the file is already in `.camel`. With `completeAllOnStop`, the group is uploaded during the stop. |
| `R3SedaBufferStopTest` | R3 (seda half) | Messages buffered in seda are gone after a stop and start, and the stop does not wait for them. |
| `R4AggregateMemoryRestartTest` | R4 | Enter 1, restart, enter 2: the result is 2, not 3, with the memory repository of the example. |
| `R5AggregateDistributedTest` | R5 | Two contexts, four threads, 60 items, one H2 database: with optimistic locking one completion with all 60 items; without it, no completion and failed sends. |
| `R7SedaRequestReplyTest` | R7 | The route of the seda docs replies; the cycle variant times out. |

Substitutions, so that the tests need no external system: the JPA idempotent repository is replaced by
`FileIdempotentRepository` (which also persists the key in `add`), the HTTP call by an fsync'ed append to a log file,
bindy by a line split, SFTP and stream output by a mock or a log, and the Kafka input by `direct`. R6 has no test: it
needs Narayana, Artemis and XA.
