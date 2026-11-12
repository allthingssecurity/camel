------------------------ MODULE R4_AggregateMemoryRestart ------------------------
(* Route (camel-examples/aggregate, META-INF/spring/camel-context.xml):
     from stream:in -> aggregate(constant(true), strategy=NumberAggregationStrategy (sum),
       aggregationRepository=myRepo (org.apache.camel.processor.aggregate.MemoryAggregationRepository),
       eagerCheckCompletion=true, completionPredicate ${body} contains 'STOP') -> stream:out
   README.adoc promise: "hit ctrl + c ... start the example again and resume where you left ...
   it remembered previously entered values, as it uses a persistent store."
   Semantics: MemoryAggregationRepository keeps groups in a map that is cleared on stop (doStop) and
   lost with the JVM; completeAllOnStop/forceCompletionOnStop are not set.
   Property (the README's promise): the printed result is the sum of every number entered since the
   previous STOP, across restarts.                                                        *)
EXTENDS Naturals, Sequences

CONSTANTS PersistentRepo, MaxRestarts
Input == <<1, 2, 0>>      \* user types 1, (ctrl+c and restart may happen), 2, then STOP (encoded as 0)

VARIABLES i, group, printed, entered, up, restarts
vars == <<i, group, printed, entered, up, restarts>>

Init == /\ i = 1 /\ group = 0 /\ printed = <<>> /\ entered = 0 /\ up = TRUE /\ restarts = 0

Enter == /\ up /\ i <= Len(Input)
         /\ IF Input[i] = 0
              THEN /\ printed' = Append(printed, group) /\ group' = 0 /\ entered' = 0
              ELSE /\ group' = group + Input[i] /\ entered' = entered + Input[i]
                   /\ UNCHANGED printed
         /\ i' = i + 1 /\ UNCHANGED <<up, restarts>>

CtrlC == /\ up /\ restarts < MaxRestarts
         /\ up' = FALSE /\ restarts' = restarts + 1
         /\ group' = IF PersistentRepo THEN group ELSE 0
         /\ UNCHANGED <<i, printed, entered>>

Start == /\ ~up /\ up' = TRUE /\ UNCHANGED <<i, group, printed, entered, restarts>>

Next == Enter \/ CtrlC \/ Start
Spec == Init /\ [][Next]_vars

\* when a result is printed it equals everything entered since the last STOP (3 for this input)
ResumesAfterRestart == Len(printed) > 0 => printed[1] = 3
===================================================================================
