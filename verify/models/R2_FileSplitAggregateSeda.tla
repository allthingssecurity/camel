------------------------- MODULE R2_FileSplitAggregateSeda -------------------------
(* Route (camel-quarkus-examples/file-bindy-ftp, Routes.java):
     from("file:{{csv.location}}?delay=1000").unmarshal().bindy(...).split(body()).to("direct:aggregateBooks");
     from("direct:aggregateBooks")
        .aggregate(simple("${body.genre}"), new GroupedBodyAggregationStrategy()).completionInterval(5000)
        .to("seda:processed");
     from("seda:processed").marshal()...to("sftp://.../uploads/books");
   Semantics modelled (Camel origin/main):
   - file consumer: the file is moved to .camel when ITS exchange completes; the split sub-exchanges
     complete as soon as the aggregator has stored them (aggregate() does not wait for the group).
   - aggregator default repository = MemoryAggregationRepository (in-memory); completionInterval fires
     for every non-empty group; completeAllOnStop/forceCompletionOnStop default false: on a graceful stop
     pending groups are not completed (AggregateProcessor.prepareShutdown) and the memory repo is cleared.
   - seda: in-memory queue; graceful stop waits for the seda route to drain its queue (SedaConsumer is
     ShutdownAware); a crash loses the queue.
   Knobs: PersistentAgg (e.g. a JDBC repo with recovery), CompleteAllOnStop, SedaHop (FALSE = direct).  *)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS PersistentAgg, CompleteAllOnStop, SedaHop, MaxCrashes, MaxStops

Files == {"f1"}
Books == [f1 |-> {"b1", "b2"}]              \* two books (same genre) in one CSV file
AllBooks == UNION {Books[f] : f \in Files}

VARIABLES inbox, cur, todo, agg, seda, uploaded, up, crashes, stops
vars == <<inbox, cur, todo, agg, seda, uploaded, up, crashes, stops>>

Init == /\ inbox = Files /\ cur = "none" /\ todo = {} /\ agg = {} /\ seda = <<>>
        /\ uploaded = {} /\ up = TRUE /\ crashes = 0 /\ stops = 0

Poll(f) == /\ up /\ cur = "none" /\ f \in inbox
           /\ cur' = f /\ todo' = Books[f]
           /\ UNCHANGED <<inbox, agg, seda, uploaded, up, crashes, stops>>

\* one split part reaches the aggregator and is stored in the group (sub-exchange done)
SplitPart(b) == /\ up /\ b \in todo
                /\ agg' = agg \cup {b} /\ todo' = todo \ {b}
                /\ UNCHANGED <<inbox, cur, seda, uploaded, up, crashes, stops>>

\* all parts handed over: the file exchange completes and the file is moved to .camel
FileDone == /\ up /\ cur # "none" /\ todo = {}
            /\ inbox' = inbox \ {cur} /\ cur' = "none"
            /\ UNCHANGED <<todo, agg, seda, uploaded, up, crashes, stops>>

\* completionInterval timer completes the group
IntervalComplete == /\ up /\ agg # {}
                    /\ IF SedaHop THEN /\ seda' = Append(seda, agg) /\ UNCHANGED uploaded
                                  ELSE /\ uploaded' = uploaded \cup agg /\ UNCHANGED seda
                    /\ agg' = {}
                    /\ UNCHANGED <<inbox, cur, todo, up, crashes, stops>>

SedaUpload == /\ up /\ seda # <<>>
              /\ uploaded' = uploaded \cup Head(seda) /\ seda' = Tail(seda)
              /\ UNCHANGED <<inbox, cur, todo, agg, up, crashes, stops>>

Crash == /\ up /\ crashes < MaxCrashes
         /\ up' = FALSE /\ crashes' = crashes + 1
         /\ cur' = "none" /\ todo' = {} /\ seda' = <<>>
         /\ agg' = IF PersistentAgg THEN agg ELSE {}
         /\ UNCHANGED <<inbox, uploaded, stops>>

\* graceful stop (e.g. rolling restart): in-flight file exchange finishes first (modelled by requiring
\* none in flight), seda route drains, aggregator groups are completed only with CompleteAllOnStop
GracefulStop == /\ up /\ stops < MaxStops /\ cur = "none"
                /\ LET flushed == IF CompleteAllOnStop THEN agg ELSE {}
                       queued  == UNION {seda[i] : i \in DOMAIN seda}
                   IN uploaded' = uploaded \cup queued \cup flushed
                /\ seda' = <<>>
                /\ agg' = IF PersistentAgg THEN agg ELSE {}
                /\ up' = FALSE /\ stops' = stops + 1
                /\ UNCHANGED <<inbox, cur, todo, crashes>>

Restart == /\ ~up /\ up' = TRUE /\ UNCHANGED <<inbox, cur, todo, agg, seda, uploaded, crashes, stops>>

Next == \/ \E f \in Files : Poll(f)
        \/ \E b \in AllBooks : SplitPart(b)
        \/ FileDone \/ IntervalComplete \/ SedaUpload \/ Crash \/ GracefulStop \/ Restart
Spec == Init /\ [][Next]_vars

AtRest == up /\ inbox = {} /\ cur = "none" /\ agg = {} /\ seda = <<>>
NoLoss == AtRest => uploaded = AllBooks
=====================================================================================
