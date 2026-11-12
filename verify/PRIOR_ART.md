# Prior art for `camel verify`

Research date: 2026-11-12. Sources: Apache JIRA, the lists.apache.org archive, apache/camel `main` at `dcb17ac19f96`
(2026-11-11), and public web pages.

The idea under test: `camel verify` statically model-checks a **user's** Camel integration (routes plus the options of its
endpoints) for end-to-end delivery guarantees before it runs. It explores crashes at any step and the redelivery
behaviour of the inputs, and asks: can a message be lost, can a side effect happen twice, can request-reply over `seda`
deadlock, does every aggregation group complete, does one transaction cover both the DB write and the send. A
counterexample is printed as a short step-by-step story.

---

## 1. Short answer

Nothing found does this. The closest pieces exist separately:

* **Inside Camel**, a design proposed on 2026-11-10 (`design/good-to-know.adoc`, CAMEL-26716) asks the **same
  questions** ("when it fails", "twice", "what a restart forgets", "what does not wait") from the route files and new
  per-component `@Facts` metadata (`onFailure`, `deliversAgain`, `inMemory`, `handOff`, `idempotent`, `confirmed`...).
  It answers them by **pattern rules over one route at a time** (a closed condition vocabulary), not by exploring
  states, crash points or interleavings. It gives no counterexample run and makes no "this cannot happen" claim. This is
  the strongest overlap, and also the most useful thing to build on: `camel verify` could use the same facts and act as
  the exhaustive checker behind the hints. It should not compete with them.
* **Research** has translated EIP designs into Petri nets and model-checked them (Fahland & Gierds, CAiSE 2013). It has
  also formalised EIPs with transactions and rollback in timed db-nets, proved that rewrites preserve correctness, and
  evaluated this on 902 real integration scenarios (Ritter et al., 2018–2023). These works
  check **conceptual pattern compositions** for control-flow and data correctness (deadlock, reachability, structural
  and functional correctness). They have **no failure model**: no crash, no broker or consumer redelivery, no
  idempotent repository semantics, and no concrete component options.
* **Products** ship rule-based linters for flow definitions: NiFi Flow Analysis Rules, bpmnlint, mule-lint, and Step Functions
  `ValidateStateMachineDefinition`. None of them explores executions.
* **Industrial formal methods** (TLA+/P at AWS, MongoDB, Cosmos DB, Kafka transactions) verify **the platform's own
  protocols**, not the pipelines users build on them. Flux (OSDI 2023) verifies idempotence under retries, but for
  serverless **function code**, not integration configurations.

---

## 2. Apache Camel itself

### 2.1 JIRA (REST search, project=CAMEL)

Phrase searches (`text ~ "\"…\""`) with **0 hits**: "model check", "model checking", "formal verification", "verify
routes", "deadlock detection", "lint", "validate routes", "circular route", "Petri". The search "TLA" matches many
issues found with TLA+ during the 2026 hunt, all about bugs in Camel's own code, none about a tool for users. Relevant
hits:

| Key | Status | Summary (short) | Relevance |
|---|---|---|---|
| [CAMEL-26716](https://issues.apache.org/jira/browse/CAMEL-26716) | In Progress (4.25.0) | camel-jbang "Good to know": hints from one shared rule set (About screen, MCP tools, Doctor) | **Closest overlap**: static hints on loss/twice/restart/hand-off; see 2.3 |
| [CAMEL-26823](https://issues.apache.org/jira/browse/CAMEL-26823) | Resolved (4.24.0) | Route topology says whether a route waits for the answer of the route it sends to (`reply` on TopologyEdge) | Gives InOut vs hand-off edges, which a request-reply/deadlock check needs |
| [CAMEL-23656](https://issues.apache.org/jira/browse/CAMEL-23656) | Resolved (4.21.0) | RouteTopologyService: inter-route dependency graph (internal direct/seda, external kafka/jms, triggers, cycles) | A graph, with no semantics |
| [CAMEL-25180](https://issues.apache.org/jira/browse/CAMEL-25180) | Resolved (4.24.0) | `camel overview` and `camel validate` share one set of route/endpoint rules | Notes a future "DSL-neutral analysis over the real Camel model" |
| [CAMEL-25208](https://issues.apache.org/jira/browse/CAMEL-25208) | Resolved (4.23.0) | Model-based validation of Java/XML routes (RouteModel + ModelChecks) | Lint level, see 2.2 |
| [CAMEL-24844](https://issues.apache.org/jira/browse/CAMEL-24844) | Open (4.24.0) | Body type flow: carry the body's Java type step by step | Static data-flow (types), not delivery |
| [CAMEL-26330](https://issues.apache.org/jira/browse/CAMEL-26330) | Resolved (4.24.0) | camel-chaos: faults turned on in a running app, per-route report of completed/failed/redelivered/lost | Dynamic, sampled, needs a running app |
| [CAMEL-26714](https://issues.apache.org/jira/browse/CAMEL-26714) / [CAMEL-26768](https://issues.apache.org/jira/browse/CAMEL-26768) | Resolved / In Progress | camel-sanitizer rules R1 (callback exactly once), R2 (consumer commit once) | Runtime checks of Camel's own engine rules |
| [CAMEL-7905](https://issues.apache.org/jira/browse/CAMEL-7905) | Closed (2014) | Option to ignore missing consumers on direct endpoints | Runtime no-consumer behaviour |
| [CAMEL-5832](https://issues.apache.org/jira/browse/CAMEL-5832) | Resolved (2012) | JMS consumer detects JMSReplyTo sent to itself (circular loop) | Runtime guard |
| [CAMEL-21606](https://issues.apache.org/jira/browse/CAMEL-21606) | Resolved (2025) | Route validation mojo fails if it depends on external sources | camel-report-maven-plugin `validate` |

No JIRA issue proposes model checking, formal verification, or exhaustive failure exploration of user routes.

### 2.2 What the existing static checks do (apache/camel `main`)

* **`SourceValidator`** (`dsl/camel-jbang/camel-jbang-core/.../commands/ai/SourceValidator.java`) runs the YAML DSL
  schema, then endpoint URIs and Simple expressions against the catalog. It also checks `.properties` keys against the
  catalog, Citrus test files against their schema, Kamelet files and `kamelet:` endpoints, and bean references declared
  in the file, siblings or `@BindToRegistry`. Further checks: bean construction (builder or no-arg constructor);
  `rest-openapi` path-parameter headers; an SQL upsert in the wrong dialect; a route id duplicated in another file;
  direct/seda endpoints no route consumes; header vs exchange property mix-ups; sftp/ftp without login next to one with
  login; Camel* headers no used component defines. It compiles Java in memory and XSLT with Saxon/JDK, checks XML is
  well formed, and runs the body-type hints (CAMEL-24844, CAMEL-25750). All of these are **syntax, catalog and
  consistency checks**.
* **`RouteModel`** reads Java (via the camel-java-io parser) and XML (via camel-xml-io) into the real Camel model
  (`RouteDefinition`, route configurations, REST) with line numbers, without compiling or running anything, and records
  what it could not read.
* **`ModelChecks`** runs on that model: endpoint URIs against the catalog (unknown options, invalid values, missing
  required path), Simple expressions, a `to` with an expression that needs `toD`, dead letter endpoints, and **direct:
  and seda: endpoints that no route consumes** ("a direct: without a consumer fails the exchange, a seda: one fills a
  queue no one reads").
* **`StructureChecks`** checks one thing only: in a YAML file, `onException`/`onCompletion`/`intercept` written after the
  first route, which fails at startup (CAMEL-24846).
* **`ProjectOverview`** (used by `camel overview`) reports these findings: `duplicate-id`; `missing-route` (send to an
  endpoint no route consumes); `unreferenced`; `cycle` ("Routes call each other in a cycle"); `pass-through`;
  `error-route-loop` (a dead letter route that uses the error handler sending to it); and `unhandled-failures` (a
  `noErrorHandler` route whose senders do not wait, so "no error handler or onException acts on" its failures).
  `unhandled-failures` is the only existing check that reasons about where failures go, and it does so with one
  structural rule.
* **`RouteGraph` / `EndpointConsumers` / `BodyTypeFlow`** (`camel-yaml-dsl-validator`): route-to-route links read from
  the YAML source, and body-type propagation.
* **camel-report-maven-plugin `validate`** (`catalog/camel-report-maven-plugin/.../ValidateMojo.java`, docs:
  <https://camel.apache.org/manual/camel-report-maven-plugin.html>) validates endpoint URIs and Simple expressions,
  duplicate route ids (`camel.duplicateRouteId`), and "direct/seda/disruptor endpoints sending to non-existing
  consumers". The same plugin's `route-coverage` goal reports coverage of EIPs from test runs. The parser is
  `catalog/camel-route-parser`.
* **`camel doctor`** (`Doctor.java`) checks the environment only (Java, JBang, Camel version, Maven repo, container
  runtime, Ollama, ports, disk, install locations). It does not look at routes.
* **Runtime guards** are not static: `DirectConsumerNotAvailableException` with `failIfNoConsumers=true` (camel-direct),
  and the JMS endpoint refusing a replyTo equal to the consumer destination (`JmsEndpoint.java:372`). A seda InOut send
  with no consumer times out only at runtime.

No code on `origin/main` explores executions, models crashes, or decides loss, duplicates, aggregation completion or
transaction coverage. `git log --grep` for "model check|TLA|formal" on main finds only bug fixes in Camel itself.

### 2.3 The closest neighbour: `design/good-to-know.adoc` (CAMEL-26716)

File: `design/good-to-know.adoc` on `origin/main` (commit `33c69f878cd3`, Claus Ibsen, 2026-11-11, status *proposed*).

* **Layer 1, facts:** a new `@Facts` annotation on endpoints, EIPs, data formats and languages, written into the catalog
  JSON. Each fact can be overridden per option (`byOption`). The first fact names are `onFailure`
  (skipped/kept/rolledBack/returned/moved), `keptRetries`, `deliversAgain`, `onSuccess` (removed/left), `feedsItself`,
  `inMemory`, `handOff`, `failureRaised`, `idempotent` (producer: GET/PUT/DELETE vs POST), `confirmed`,
  `timeoutBounded`, `overwrites`, `answersForFailure`. Example: Kafka `onFailure=skipped`, or `kept` with
  `breakOnFirstError=true`; `deliversAgain=yes`.
* **Layer 2, rules:** one JSON file in camel-catalog, with conditions from a **closed vocabulary over one route at a
  time** (`input`, `errorHandler`, `sends`, `has`/`hasNot`, `step`, `option`). Only `deadLetter.canFail` and `receiver`
  follow a send into the receiving route, over `direct` only, stopping at cycles. Examples: `failed-message-skipped`,
  `dead-letter-can-fail`, `delivered-again-not-idempotent`, `error-status-is-a-reply`, `slow-service-slows-the-input`.
* **Non-goals and design choices:** "a hint, not a fix"; static only; "a general expression language for the conditions"
  was rejected as hard to validate. The section "Alternatives considered" does not mention model checking.
* **Origin:** the hints came from camel-chaos AI explorations (CAMEL-26760): "every loss the AI found was a fact that
  could have been a hint before the first fault".

**What it does not do**, and `camel verify` would: state exploration across routes, seda queues and the error handler;
crash points between steps (for example a crash after a JDBC insert and before a file `move`); interleavings of
concurrent consumers; aggregation completion (the completion predicate, size or timeout against the groups that can
form); request-reply deadlock (seda InOut with a bounded queue or `concurrentConsumers` back-pressure into the caller,
`waitForTaskToComplete`); transaction scope (which steps a `transacted` boundary covers versus a send outside it). It
also gives no counterexample trace and no "proved absent" verdict.

**Positioning:** this is the same problem space, owned by the same people. A `camel verify` proposal should present
itself as the exhaustive back end that consumes the `@Facts` and the rule file (each rule is a property to check, and a
counterexample becomes the explanation of the hint). It should not be a parallel taxonomy. The design's "Explaining a
failure" section, where the hint that said it could happen is the "why", fits a counterexample story directly.

### 2.4 camel-chaos (dynamic, not static)

`components/camel-chaos`, with JBang `camel cmd chaos-add|suggest|experiment|explore` (docs under
`docs/user-manual/modules/ROOT/pages/jbang-commands/camel-jbang-cmd-chaos-*.adoc`). It injects faults into a **running**
integration (send failures, delays, poll strategy, route stalls) and reports completed, failed, redelivered and lost
messages per route. `chaos-explore` has "an AI explore by itself how a running integration copes with faults, for a time
budget". This tests by sampling: it can find a loss, but it cannot show that none exists. It needs the integration and
its systems running, and depends on the faults and the traffic chosen.

### 2.5 Mailing lists (lists.apache.org API)

`dev@camel` and `users@camel`, searched for "formal verification", "TLA+" and "Petri": **0 relevant hits**. "model
checking" matched only unrelated threads where the words appear apart (Jenkins builds, website, a 2011 cron policy
thread). "static analysis" matched the monthly "[REPORT] Monthly Camel static code analysis" (Sonar on Camel's own
code) and a 2015 users@ question asking for an SCA tool for Camel routes
([thread](https://lists.apache.org/thread/r2dj8obzp4s96478q7bjv8x5pwdtp8xs), no answer in the archive). Nobody has
proposed model checking user routes on the lists.

### 2.6 Kaoto and Karavan

* **Kaoto**: the docs page (<https://kaoto.io/docs/>) mentions only "configuration forms, and interactive feedback
  mechanisms". Release 2.2 shows required properties first in forms
  ([blog](https://camel.apache.org/blog/2024/09/kaoto-release-2.2.0/)). Release 2.8 lists "OnException Validation:
  enhanced validation for exception handling configurations"
  ([blog](https://camel.apache.org/blog/2025/11/kaoto-release-2.8.0/)), with no detail on what it checks. Everything
  found is visual editing plus schema- and catalog-driven forms; no semantic or delivery verification was found. Kaoto's
  GitHub issues were not searched.
* **Karavan**: visual designer, plus highlighting steps "in red if a sensitive field contains a plain value"
  ([4.8.0 blog](https://camel.apache.org/blog/2024/09/camel-karavan-4.8.0/)). No semantic or delivery verification was
  found.
* The **YAML DSL validator Maven plugin** checks against the JSON schema only and "does not run Camel's YAML route
  loader" (<https://camel.apache.org/manual/camel-yaml-dsl-validator-maven-plugin.html>).
* No community "camel-lint" or Sonar rule set for Camel routes was found. A 2016 Red Hat Fuse Tooling post describes
  XSD and endpoint-URI validation in Eclipse
  ([post](https://developers.redhat.com/blog/2016/05/20/jboss-fuse-tooling-camel-file-validation-existing-improved-and-new)).

---

## 3. Other frameworks and products

| Product / tool | What it checks | Delivery-guarantee reasoning? |
|---|---|---|
| **Spring Integration** `IntegrationGraphServer` ([docs](https://docs.spring.io/spring-integration/reference/graph.html)) | A runtime graph of nodes and links (input/output/error/discard/route) with optional metrics, "to visualize the current state" | None. Visualization only |
| **Apache NiFi Flow Analysis Rules** (NiFi 2.x; [NIFI-8650](https://issues.apache.org/jira/browse/NIFI-8650) fix 2.0.0-M1; [API](https://www.javadoc.io/static/org.apache.nifi/nifi-api/2.0.0-M2/org/apache/nifi/flowanalysis/package-summary.html); [Cloudera post](https://community.cloudera.com/t5/Developer-Blogs/Flow-Analysis-Rules-enforcing-Best-Practices-in-Flow-Design/m-p/413017)) | Per-component or process-group rules, run as Recommendation or Policy. Apache built-ins: `DisallowComponentType`, `RestrictBackpressureSettings`, `RestrictFlowFileExpiration` (expiry drops data), `RequireServerSSLContextService`, `RequireLoadBalancedConnectionAfterPrimaryNodeSourceProcessor`. Cloudera adds e.g. `DisallowDeadEnd`, `RequireHandleHttpResponseAfterHandleHttpRequest` ([CFM notes](https://docs.cloudera.com/cfm/latest/release-notes/topics/cfm-supported-nifi-flow-analysis-rules.html)) | Pattern rules only. `RestrictFlowFileExpiration` and `DisallowDeadEnd` touch on loss, but by configuration thresholds and shape, not by exploring executions |
| **MuleSoft**: Anypoint Code Builder ([troubleshooting](https://docs.mulesoft.com/anypoint-code-builder/troubleshooting)), mule-lint ([site](https://mule-lint.github.io)), Sonar plugins ([Sonar](https://www.sonarsource.com/integrations/mulesoft/)) | Design-time configuration and validation errors; mule-lint does "basic static analysis checking of certain elements"; Sonar has code-quality rules | None found. MUnit is unit testing |
| **Kafka Streams / ksqlDB** ([TopologyTestDriver](https://kafka.apache.org/42/javadoc/org/apache/kafka/streams/TopologyTestDriver.html), [streams config](https://kafka.apache.org/documentation/streams/developer-guide/config-streams/)) | `Topology#describe()` lists the topology; TopologyTestDriver runs records synchronously ("commit.interval.ms … have no effect"); exactly-once is the runtime setting `processing.guarantee` | None. EOS is a runtime mechanism, not a verified property of the user topology (the config page renders by JS; its text was not extracted) |
| **Temporal** ([Java testing](https://docs.temporal.io/develop/java/testing-suite); [workflowcheck](https://pkg.go.dev/go.temporal.io/sdk/contrib/tools/workflowcheck)) | Replay testing checks that workflow code is deterministic against recorded histories; Go `workflowcheck` statically flags non-deterministic calls ("will not catch all cases") | Determinism of code only. Activities are at-least-once by design and nothing checks their idempotence |
| **AWS Step Functions** `ValidateStateMachineDefinition` ([API](https://docs.aws.amazon.com/step-functions/latest/apireference/API_ValidateStateMachineDefinition.html)) | Syntax and semantic diagnostics of ASL. Example warnings: `NO_DOLLAR`, `PASS_RESULT_IS_STATIC`, `NO_PATH` | None. JSONPath and field misuse only; no reachability, retry or idempotency analysis |
| **Azure Logic Apps** `Workflows - Validate` ([REST](https://learn.microsoft.com/en-us/rest/api/logic/workflows/validate)) | Validates the definition against the Workflow Definition Language schema | None |
| **bpmnlint** (Camunda/bpmn.io; [rules](https://github.com/bpmn-io/bpmnlint/tree/main/docs/rules)) | 27 structural rules: `no-disconnected`, `fake-join`, `no-implicit-split`, `end-event-required`... | Structural only |

---

## 4. Research

### 4.1 Formalisations of Enterprise Integration Patterns

* **Fahland & Gierds, "Analyzing and Completing Middleware Designs for Enterprise Integration Using Coloured Petri
  Nets", CAiSE 2013**, <https://doi.org/10.1007/978-3-642-38709-8_26>
  ([PDF](https://dfahland.win.tue.nl/publications/FahlandG_2013_eip_cpn.pdf), read). Each Hohpe/Woolf pattern becomes a
  CPN, and an EIP design becomes a CPN model, used for simulation and performance, **model checking with CPN Tools
  ASK-CTL** (e.g. "if always a loan quote is sent back": with an empty address list none is, "which can be detected
  automatically including a counter example trace"), **controller synthesis for deadlock freedom** when mediating
  stateful services, and execution or code generation. It names Apache Camel and Spring Integration as frameworks that
  "lack formal analysis facilities". **It does not** model crashes, redelivery, guaranteed delivery, transactions or
  idempotent receivers: a grep of the full text finds no "failure", "retry", "duplicate", "transactional" or "persist".
  It works on conceptual designs, not on a framework's concrete configuration. This is the nearest academic ancestor of
  "translate the routes, model-check, print a counterexample trace".
* **Ritter, Rinderle-Ma, Montali, Rivkin, Sinha, "Formalizing Application Integration Patterns", EDOC 2018**,
  <https://doi.org/10.1109/EDOC.2018.00012>. Timed db-nets (CPN plus a persistent DB layer, transactions with rollback,
  and time). The abstract says it targets "complex data, transacted resources and time" that the CPN work missed.
  Catalog: [arXiv:1807.03197](https://arxiv.org/abs/1807.03197). Demonstrator: a CPN Tools extension
  ([repo](https://github.com/dritter-hd/db-net-eip-patterns/blob/master/README.md), simulation only). Journal version:
  **"Formal foundations for responsible application integration"**, Information Systems 2021,
  <https://doi.org/10.1016/j.is.2019.101439>, which shows "how the correctness of these realizations can be shown".
* **Ritter, May, Nordvall Forsberg, Rinderle-Ma, "Optimization Strategies for Integration Pattern Compositions", DEBS
  2018**, <https://doi.org/10.1145/3210284.3210295>. Graph-rewriting optimizations evaluated on more than 900 scenarios.
  **"Catalog of Optimization Strategies and Realizations for Composed Integration Patterns"**,
  [arXiv:1901.01005](https://arxiv.org/abs/1901.01005).
* **Ritter, Nordvall Forsberg, Rinderle-Ma, "Responsible Composition and Optimization of Integration Processes under
  Correctness Preserving Guarantees"**, [arXiv:2305.19196](https://arxiv.org/abs/2305.19196) (2023; full text read).
  Integration pattern contract graphs with timed db-net semantics; proves that optimizations preserve **structural**
  correctness (channel cardinality) and **functional** correctness (contracts, data-element correctness, reachability
  between boundary places). Evaluated on **902 real integration scenarios**, with runtime benchmarks on Apache Camel. Idempotent receivers appear only as patterns that need
  persistent storage, and exceptions only as rollback arcs. Exactly-once delivery appears only as context and is
  **not verified**; the full text has no "crash" at all.
* **Montali, Rivkin, Ritter, "Formalizing Integration Patterns with Multimedia Data", EDOC 2020**,
  [arXiv:2009.04589](https://arxiv.org/abs/2009.04589). Same line of work, for multimedia data.
* **Ritter & Sosulski, "Exception Handling in Message-Based Integration Systems and Modeling Using BPMN"**, IJCIS 2016,
  <https://doi.org/10.1142/S0218843016500040> (also EDOC 2014, <https://doi.org/10.1109/EDOC.2014.13>). Exception types
  and compensation as BPMN "Exception Flow" patterns, evaluated syntactically and semantically. Modelling, not
  verification. Earlier: EIPs in BPMN, [arXiv:1403.4053](https://arxiv.org/abs/1403.4053).
* **Guaraná DSL line (Frantz, Roos-Frantz, Sawicki et al.)**: "Using timed and coloured Petri nets for modelling,
  simulation, and analysis of integration solutions", IJWET 2019, <https://doi.org/10.1504/IJWET.2019.105587>, and
  "Modelling and verification of enterprise application integration processes through coloured Petri nets", IJCAT
  2022, <https://doi.org/10.1504/IJCAT.2022.130291> (the Inderscience pages returned empty; abstracts not read, and the
  keywords per [dblp](https://dblp.dagstuhl.de/rec/journals/ijcat/LencinaRFS22.html) include model checking). Also
  "Using Petri Nets to Enable the Simulation of Application Integration Solutions Conceptual Models", 2015
  ([SciTePress](https://www.scitepress.org/Papers/2015/53519/)). These cover Guaraná models, not Camel. Per search
  snippets, a later paper notes that the CPN translation of Hohpe/Woolf "does not cover all integration patterns found
  in actual implementation of integration platforms".
* **No paper was found** that formally verifies **Apache Camel routes** as written (searches for "Apache Camel" with
  model checking, formal verification, Petri net, Alloy or Event-B). The closest is Ritter et al., who use Camel as the
  runtime in their benchmarks.

### 4.2 Process and workflow verification (BPM, orchestration)

* **Soundness of workflow nets**: Woflan (Verbeek, Basten, van der Aalst, "Diagnosing Workflow Processes using
  Woflan", Computer Journal 2001, <https://doi.org/10.1093/comjnl/44.4.246>). Soundness (option to complete, proper
  completion, no dead tasks) as boundedness and liveness of the short-circuited net.
* **BPEL**: Ouyang et al., "Formal semantics and analysis of control flow in WS-BPEL", SCP 2007,
  <https://doi.org/10.1016/j.scico.2007.03.002> (WofBPEL: unreachable activities, conflicting message receipts);
  Lohmann, "A Feature-Complete Petri Net Semantics for WS-BPEL 2.0" (<https://doi.org/10.1007/978-3-540-79230-7_6>).
* **BPMN**: Dijkman, Dumas, Ouyang, "Semantics and analysis of business process models in BPMN", IST 2008,
  <https://doi.org/10.1016/j.infsof.2008.02.006>.
* **Netflix Conductor**: Camilli, Bellettini, Capra, Monga, "A Formal Framework for Specifying and Verifying
  Microservices Based Process Flows", FAACS 2017 ([PDF](https://fmse.di.unimi.it/faacs2017/papers/paperMSE5.pdf), read).
  Conductor JSON is translated to Time Basic Petri nets ("Conductor2Pn") and checked with off-the-shelf model checkers.
  It covers timeouts and retries of the workflow, not broker or consumer delivery semantics.
* **Flux**: Ding, Wang, Shen, Chen, Chen, "Automated Verification of Idempotence for Stateful Serverless Applications",
  OSDI 2023 ([page](https://www.usenix.org/conference/osdi23/presentation/ding)). It defines "idempotence consistency"
  (a retry is invisible to users), verifies it per function with "idempotence simulation", and repairs the operations
  that break it. It found issues in 12 of 27 apps. **This is the closest work on the "processed twice" property**, but it
  targets serverless function code, not integration routes, endpoint options or brokers.
* What these all share: control-flow soundness and deadlock, or determinism, of a **process definition**. They do not
  model the redelivery semantics of a concrete connector configuration (file move/delete/noop, Kafka commit modes, JMS
  ack and transacted, idempotent-repository eager vs lazy add).

### 4.3 Industrial formal methods (platforms, not user pipelines)

* **AWS**: Newcombe et al., "How Amazon Web Services Uses Formal Methods", CACM 2015,
  <https://doi.org/10.1145/2699417>. Brooker & Desai, "Systems Correctness Practices at Amazon Web Services", CACM
  68(6) 2025, <https://doi.org/10.1145/3729175> ([CACM](https://cacm.acm.org/practice/systems-correctness-practices-at-amazon-web-services),
  403 on fetch; summary from abstract and snippets: TLA+, P, PObserve log checking, property-based testing, fault
  injection, deterministic simulation). P: Desai et al., PLDI 2013, <https://doi.org/10.1145/2491956.2462184>.
  ShardStore: Bornholt et al., "Using Lightweight Formal Methods to Validate a Key-Value Storage Node in Amazon S3", SOSP
  2021, <https://doi.org/10.1145/3477132.3483540> (executable reference models; crash consistency). All of this
  verifies **AWS's own services**. No source found applies it to customer-authored workflows; Step Functions offers only
  definition validation (section 3).
* **MongoDB**: Davis, Hirschhorn, Schvimer, "eXtreme Modelling in Practice", PVLDB 2020,
  <https://doi.org/10.14778/3397230.3397233> ([arXiv:2006.00915](https://arxiv.org/abs/2006.00915)), on model-based trace
  checking and test-case generation. Schultz et al., "Design and Analysis of a Logless Dynamic Reconfiguration
  Protocol", [arXiv:2102.11960](https://arxiv.org/abs/2102.11960), and the TLAPS proof "Formal verification of a
  distributed dynamic reconfiguration protocol", CPP 2022, <https://doi.org/10.1145/3497775.3503688>. All about the
  database protocol.
* **Azure Cosmos DB**: Hackett, Rowe, Kuppe, "Understanding Inconsistency in Azure Cosmos DB with TLA+", ICSE-SEIP 2023,
  <https://doi.org/10.1109/ICSE-SEIP58684.2023.00006>. It specifies the **user-observable** consistency behaviours, which
  were "previously poorly understood outside of the Cosmos DB development team". This is a good methodological analogue:
  `camel verify` would likewise make the user-observable delivery behaviour of component options explicit and checkable.
* **Kafka**: Jack Vanlightly, "Verifying Kafka transactions" diary, Dec 2024 ([entry 1](https://jack-vanlightly.com/analyses/2024/12/2/verifying-kafka-transactions-diary-entry-1-a-first-step)),
  reverse-engineers the **broker and client transaction protocol** into TLA+ and Fizzbee, not user applications. Llingr
  ([dev.to](https://dev.to/illyar80/your-integration-tests-pass-but-your-message-queue-still-double-processes-in-production-223p))
  model-checked **its own** consumer engine's offset-commit protocol and found a commit-during-revocation race that
  caused duplicates. Farooq Qaiser's "Modelling Exactly-Once Using TLA+" ([part 1](https://medium.com/@fqaiser94/exactly-once-modelling-using-tla-3c792655be44))
  is a tutorial spec. These are hand-written models of one protocol, not a tool that derives a model from a user's
  pipeline.
* Flink checkpoint and exactly-once verification was not researched separately.

---

## 5. Comparison table

| Name | Link | What it covers | What it does not cover |
|---|---|---|---|
| Camel "Good to know" design (CAMEL-26716) | `design/good-to-know.adoc` on origin/main; [JIRA](https://issues.apache.org/jira/browse/CAMEL-26716) | Static per-route hints on loss, duplicates, restart, hand-off, from `@Facts` component metadata and a closed rule vocabulary | No state or crash exploration, no cross-route interleavings, no aggregation or deadlock or transaction-scope reasoning, no counterexample, no proof of absence. Status: proposed |
| Camel `ProjectOverview` / `camel overview` | `ProjectOverview.java` | Missing route, cycle, unreferenced, error-route loop, unhandled failures after a hand-off | One-rule structural findings; no failure model |
| Camel `ModelChecks` / `SourceValidator` / `StructureChecks` | `dsl/camel-jbang/camel-jbang-core/.../ai/` | Schema, catalog (URIs, options, Simple), beans, direct/seda with no consumer, top-level order | Syntax and consistency only |
| camel-report-maven-plugin `validate` | [manual](https://camel.apache.org/manual/camel-report-maven-plugin.html) | URIs, Simple, duplicate route ids, direct/seda/disruptor without consumers | Same as above |
| RouteTopologyService (+ `reply`) | [CAMEL-23656](https://issues.apache.org/jira/browse/CAMEL-23656), [CAMEL-26823](https://issues.apache.org/jira/browse/CAMEL-26823) | Inter-route graph, internal/external edges, InOut vs hand-off | A graph without semantics |
| camel-chaos (+ AI explore) | [CAMEL-26330](https://issues.apache.org/jira/browse/CAMEL-26330) | Runtime fault injection, per-route lost/redelivered counts | Sampling: needs a running app, cannot prove absence |
| camel-sanitizer | [CAMEL-26714](https://issues.apache.org/jira/browse/CAMEL-26714) | Runtime checks of Camel engine rules (callback once, commit once) | Camel's code, not the user's integration guarantees |
| Kaoto / Karavan | [Kaoto 2.8](https://camel.apache.org/blog/2025/11/kaoto-release-2.8.0/), [Karavan 4.8](https://camel.apache.org/blog/2024/09/camel-karavan-4.8.0/) | Visual editing, schema and catalog forms, onException form validation, plain-secret highlighting | No semantic or delivery checks found |
| Spring Integration graph | [docs](https://docs.spring.io/spring-integration/reference/graph.html) | Runtime graph and metrics | No validation |
| NiFi Flow Analysis Rules | [API](https://www.javadoc.io/static/org.apache.nifi/nifi-api/2.0.0-M2/org/apache/nifi/flowanalysis/package-summary.html), [Cloudera](https://docs.cloudera.com/cfm/latest/release-notes/topics/cfm-supported-nifi-flow-analysis-rules.html) | Per-component or group policy rules (backpressure, expiry, dead end, component bans) | No execution exploration |
| MuleSoft (ACB, mule-lint, Sonar) | [ACB](https://docs.mulesoft.com/anypoint-code-builder/troubleshooting), [mule-lint](https://mule-lint.github.io) | Config validation, basic lint, code quality | Nothing on delivery |
| Kafka Streams | [TopologyTestDriver](https://kafka.apache.org/42/javadoc/org/apache/kafka/streams/TopologyTestDriver.html) | Topology description, synchronous test driver, EOS as runtime config | No verification of user topologies |
| Temporal | [testing](https://docs.temporal.io/develop/java/testing-suite), [workflowcheck](https://pkg.go.dev/go.temporal.io/sdk/contrib/tools/workflowcheck) | Determinism of workflow code (replay; Go static check) | Activity idempotence, delivery |
| Step Functions validate | [API](https://docs.aws.amazon.com/step-functions/latest/apireference/API_ValidateStateMachineDefinition.html) | ASL syntax and semantic warnings | Reachability, retries, idempotency |
| Azure Logic Apps validate | [REST](https://learn.microsoft.com/en-us/rest/api/logic/workflows/validate) | Schema validation of the definition | Semantics, delivery |
| bpmnlint | [rules](https://github.com/bpmn-io/bpmnlint/tree/main/docs/rules) | Structural BPMN rules | Execution semantics |
| Fahland & Gierds 2013 | <https://doi.org/10.1007/978-3-642-38709-8_26> | EIP design to CPN; simulation, CTL model checking with counterexample, deadlock-free controller synthesis | Failures, redelivery, transactions, idempotency; concrete framework config |
| Ritter et al. (timed db-nets, EDOC 2018, IS 2021, arXiv 2023) | <https://doi.org/10.1109/EDOC.2018.00012>, <https://doi.org/10.1016/j.is.2019.101439>, [arXiv:2305.19196](https://arxiv.org/abs/2305.19196) | EIP semantics with data, transactions/rollback, time; correctness-preserving optimizations; 902 real integration scenarios | Crash/redelivery, exactly-once, idempotent-consumer semantics, endpoint options; not a user-facing checker |
| Guaraná CPN/SPN works | <https://doi.org/10.1504/IJWET.2019.105587>, <https://doi.org/10.1504/IJCAT.2022.130291> | Simulation and verification of Guaraná integration models | Not Camel; abstracts not read |
| Woflan / WofBPEL / BPMN to Petri nets | <https://doi.org/10.1093/comjnl/44.4.246>, <https://doi.org/10.1016/j.scico.2007.03.002>, <https://doi.org/10.1016/j.infsof.2008.02.006> | Soundness, deadlock, unreachable activities of process models | Messaging middleware delivery semantics |
| Conductor to TB nets (FAACS 2017) | [PDF](https://fmse.di.unimi.it/faacs2017/papers/paperMSE5.pdf) | Model checking of orchestration workflows with time | Broker/consumer redelivery, idempotency |
| Flux (OSDI 2023) | [USENIX](https://www.usenix.org/conference/osdi23/presentation/ding) | Verifies idempotence of serverless function code under retries and failures | Integration routes, connectors, loss, aggregation, deadlock |
| AWS TLA+/P, S3 ShardStore | <https://doi.org/10.1145/2699417>, <https://doi.org/10.1145/3729175>, <https://doi.org/10.1145/3477132.3483540> | AWS's own distributed protocols and storage | Customer pipelines |
| MongoDB TLA+ | <https://doi.org/10.14778/3397230.3397233>, [arXiv:2102.11960](https://arxiv.org/abs/2102.11960) | Replication and reconfiguration protocols, trace checking | User applications |
| Cosmos DB TLA+ | <https://doi.org/10.1109/ICSE-SEIP58684.2023.00006> | User-observable consistency semantics of the service | User applications |
| Kafka transactions TLA+ (Vanlightly) | [diary](https://jack-vanlightly.com/analyses/2024/12/2/verifying-kafka-transactions-diary-entry-1-a-first-step) | Kafka's transaction protocol | User pipelines on Kafka |

---

## 6. Honest conclusion

**Not new (prior art exists):**
* Translating EIP compositions into Petri nets or state machines and model-checking them, with counterexample traces
  and deadlock analysis (Fahland & Gierds 2013; Ritter et al. 2018–2023; Guaraná works; Conductor2Pn).
* Formalising transactions and rollback for integration patterns (timed db-nets), evaluated on real integration
  scenarios (Ritter et al.).
* Soundness and deadlock checking of process models (Woflan, WofBPEL, BPMN to Petri nets).
* Verifying "a retry is invisible", i.e. idempotence under failures (Flux, OSDI 2023), for serverless code.
* Rule-based static linting of flow definitions in many products, including NiFi's flow rules, and inside Camel itself (`camel validate`, `camel overview`, and the proposed Good-to-know hints).
* Dynamic fault injection to find loss and duplicates (camel-chaos in Camel; fault injection at AWS).
* TLA+ models of exactly-once and offset-commit protocols, for the platforms themselves.

**New, as far as this search could find:**
1. **The subject**: the user's **concrete** integration (Camel routes as written, with the options of their endpoints,
   error handlers and route configurations). Not a conceptual EIP design, and not a platform protocol.
2. **The failure model derived from real component configuration**: a crash at any step, and redelivery semantics that
   follow from options. Examples: file `move`/`delete`/`noop`/`moveFailed`/idempotent repository; idempotent consumer
   `eager`; Kafka commit mode and `breakOnFirstError`; JMS acknowledgement and `transacted`; JDBC transaction boundaries
   versus sends outside them; the in-memory state of seda, aggregation and idempotent repositories. None of the
   formalisations found model crash or redelivery at all, and none takes connector options as input.
3. **End-to-end properties checked exhaustively across routes**: no loss, no duplicate side effect, every aggregation
   group completes, no request-reply deadlock over seda, one transaction covers both the write and the send. The answer
   is a short counterexample story, or "none within the bounds".
4. **Delivery as a framework CLI** (`camel verify`) that sits next to `camel validate` and needs no Petri-net or TLA+
   knowledge from the user.

**Caveats for the proposal:**
* The Good-to-know design (CAMEL-26716, proposed 2026-11-10 by the Camel lead) covers the same questions with static
  rules and is about to add the `@Facts` metadata `camel verify` would need. Present `camel verify` as the exhaustive
  layer that reuses those facts: each hint becomes a checked property, and each counterexample becomes the explanation
  of the hint. Do not present it as a separate system. Otherwise reviewers will reasonably ask why two rule sets exist.
* Cite Fahland & Gierds and Ritter et al. as the foundation. The novelty claim should be the failure and redelivery
  model from real component options, plus the user-facing tool, not "model checking EIPs".
* A static model is only as good as its facts. The design doc already names this risk ("A wrong fact gives a wrong
  hint"); for a verifier, a wrong fact gives a wrong "verified". Facts should be tied to tests of the component
  (camel-chaos recipes or sanitizer-style runtime checks) so the model and the code cannot drift apart.

**Not checked:** Kaoto GitHub issues; the full
texts of the Guaraná IJCAT/IJWET papers and Brooker & Desai 2025 (fetch blocked); Flink, IBM App Connect, Boomi and
TIBCO.
