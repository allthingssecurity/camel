#!/bin/bash
# Re-runs every model check (one TLC at a time) and renders the counterexample stories.
# usage: TLA2TOOLS=/path/to/tla2tools.jar ./run_all.sh | tee RESULTS.txt
cd "$(dirname "$0")"
R=./run.sh
for inv in NoLoss NoDuplicateEffect; do
  $R R1_IdempotentFileHttp eagerJpa        $inv Eager=TRUE  PersistentRepo=TRUE  MaxCrashes=1 MaxFailures=1
  $R R1_IdempotentFileHttp nonEagerJpa     $inv Eager=FALSE PersistentRepo=TRUE  MaxCrashes=1 MaxFailures=1
  $R R1_IdempotentFileHttp eagerMemory     $inv Eager=TRUE  PersistentRepo=FALSE MaxCrashes=1 MaxFailures=1
  $R R1_IdempotentFileHttp eagerJpaNoCrash $inv Eager=TRUE  PersistentRepo=TRUE  MaxCrashes=0 MaxFailures=2
done
M=R2_FileSplitAggregateSeda
$R $M asIs_crash NoLoss PersistentAgg=FALSE CompleteAllOnStop=FALSE SedaHop=TRUE MaxCrashes=1 MaxStops=0
$R $M asIs_gracefulStop NoLoss PersistentAgg=FALSE CompleteAllOnStop=FALSE SedaHop=TRUE MaxCrashes=0 MaxStops=1
$R $M completeAllOnStop_gracefulStop NoLoss PersistentAgg=FALSE CompleteAllOnStop=TRUE SedaHop=TRUE MaxCrashes=0 MaxStops=1
$R $M completeAllOnStop_crash NoLoss PersistentAgg=FALSE CompleteAllOnStop=TRUE SedaHop=TRUE MaxCrashes=1 MaxStops=1
$R $M persistentAgg_seda_crash NoLoss PersistentAgg=TRUE CompleteAllOnStop=FALSE SedaHop=TRUE MaxCrashes=1 MaxStops=1
$R $M persistentAgg_direct_crash NoLoss PersistentAgg=TRUE CompleteAllOnStop=FALSE SedaHop=FALSE MaxCrashes=1 MaxStops=1
M=R3_KafkaToSedaBuffer
$R $M seda_crash NoLoss N=3 SedaHop=TRUE MaxCrashes=1 MaxStops=0
$R $M seda_stop NoLoss N=3 SedaHop=TRUE MaxCrashes=0 MaxStops=1
$R $M direct_crash_stop NoLoss N=3 SedaHop=FALSE MaxCrashes=1 MaxStops=1
$R R4_AggregateMemoryRestart asShipped ResumesAfterRestart PersistentRepo=FALSE MaxRestarts=1
$R R4_AggregateMemoryRestart persistentRepo ResumesAfterRestart PersistentRepo=TRUE MaxRestarts=1
$R R5_AggregateDistOptimistic optimistic NoLostUpdate Optimistic=TRUE
$R R5_AggregateDistOptimistic noOptimistic NoLostUpdate Optimistic=FALSE
$R R6_JtaJmsJpa xa Atomic XA=TRUE MaxCrashes=1
$R R6_JtaJmsJpa nonXa Atomic XA=FALSE MaxCrashes=1
$R R7_SedaRequestReply docExample NoDeadlock Cycle=FALSE Consumers=1
$R R7_SedaRequestReply docExample2 NoDeadlock Cycle=FALSE Consumers=2
$R R7_SedaRequestReply cycle1 NoDeadlock Cycle=TRUE Consumers=1
$R R7_SedaRequestReply cycle2 NoDeadlock Cycle=TRUE Consumers=2
mkdir -p stories
for f in out/*.txt; do b=$(basename $f .txt); if grep -q "is violated" $f; then
  python3 ../tools/story.py $f narr_${b%%_*}.json > stories/$b.story.txt; fi; done
