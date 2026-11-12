# camel verify (proposal)

![Animated explainer of camel verify: a crash makes an idempotent consumer drop a file, and camel verify finds the run that does it](docs/verify-explainer.gif)

**Status: a proposal shared early for feedback, not a pull request.** Branch `camel-verify-proposal` of
github.com/allthingssecurity/camel, on top of apache/camel `main`. Nothing here changes Camel. The work itself comes
after the exchange sanitizer is finished.

`camel verify` would read the routes of an integration, with the options of their endpoints, and check them for lost
and duplicated messages under crashes, graceful stops and redelivery. When something can go wrong it prints the run
that does it, step by step. When nothing can, it names the guarantee the route gives (at-most-once, at-least-once or
effectively-once). It would read the same `@Facts` as Claus's Good to know design (CAMEL-26716) and act as the
exhaustive checker behind those hints.

On three official examples the hand-written models found a problem that a test then reproduced: an idempotent consumer
that drops a file on a crash, a redeploy that loses aggregated input whose file was already committed, and an
aggregate example whose README promises persistence its configuration does not have.

**The full proposal is [`PROPOSAL.md`](PROPOSAL.md).** Prior art with sources: [`PRIOR_ART.md`](PRIOR_ART.md).

| Folder | What is in it |
|---|---|
| [`models/`](models/) | TLA+ models of seven routes, the TLC results and the counterexample stories |
| [`tests/`](tests/) | JUnit tests that reproduce the model results on Camel 4.23.0, with no external systems |
| [`tools/`](tools/) | `story.py`, which turns a TLC counterexample into numbered plain-text steps |
