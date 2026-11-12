--------------------------- MODULE R3_KafkaToSedaBuffer ---------------------------
(* Route (camel-quarkus-examples/kafka, Routes.java + KafkaResource.java):
     from("kafka:{{kafka.topic.name}}").log(...).to("seda:kafka-messages");
     REST GET -> consumerTemplate.receiveBody("seda:kafka-messages", 10000)
   Semantics modelled (Camel origin/main):
   - camel-kafka default autoCommitEnable=true, no allowManualCommit, no offset repository, no batching
     -> CommitManagers picks NoopCommitManager and the Kafka client auto-commits: the position of the
     records returned by the previous poll is committed on a later poll (every autoCommitIntervalMs) and
     on consumer close. Records of one poll are processed synchronously by the poll thread.
   - to("seda:x") with InOnly returns as soon as the message is queued (in memory).
   - the seda queue is drained only by the REST call (a polling consumer, not a route), so nothing waits
     for it on shutdown; a stop or crash drops its content.
   Knob SedaHop=FALSE models the same consumer doing the work directly (to a durable target).      *)
EXTENDS Naturals, Sequences

CONSTANTS N, SedaHop, MaxCrashes, MaxStops

VARIABLES pos,        \* next offset the consumer will poll (in memory)
          done,       \* highest offset whose exchange completed (+1), i.e. records 0..done-1 handed on
          committed,  \* committed offset in Kafka (durable)
          seda,       \* buffered offsets (in memory)
          delivered,  \* offsets that reached the REST client / durable target
          up, crashes, stops
vars == <<pos, done, committed, seda, delivered, up, crashes, stops>>

Init == /\ pos = 0 /\ done = 0 /\ committed = 0 /\ seda = <<>> /\ delivered = {}
        /\ up = TRUE /\ crashes = 0 /\ stops = 0

\* poll one record (batch size 1); the client may auto-commit the previous poll's position now
Poll == /\ up /\ pos < N /\ done = pos
        /\ pos' = pos + 1
        /\ committed' \in {committed, pos}           \* auto-commit interval may or may not have elapsed
        /\ UNCHANGED <<done, seda, delivered, up, crashes, stops>>

Process == /\ up /\ done < pos
           /\ IF SedaHop THEN /\ seda' = Append(seda, done) /\ UNCHANGED delivered
                         ELSE /\ delivered' = delivered \cup {done} /\ UNCHANGED seda
           /\ done' = done + 1
           /\ UNCHANGED <<pos, committed, up, crashes, stops>>

\* an idle poll (no new records) also triggers auto-commit
IdlePoll == /\ up /\ done = pos /\ committed' = pos
            /\ UNCHANGED <<pos, done, seda, delivered, up, crashes, stops>>

RestGet == /\ up /\ seda # <<>>
           /\ delivered' = delivered \cup {Head(seda)} /\ seda' = Tail(seda)
           /\ UNCHANGED <<pos, done, committed, up, crashes, stops>>

Crash == /\ up /\ crashes < MaxCrashes
         /\ up' = FALSE /\ crashes' = crashes + 1 /\ seda' = <<>>
         /\ pos' = committed /\ done' = committed
         /\ UNCHANGED <<committed, delivered, stops>>

\* graceful stop: the poll loop finishes the current record, consumer.close() auto-commits
GracefulStop == /\ up /\ stops < MaxStops /\ done = pos
                /\ committed' = pos /\ seda' = <<>> /\ up' = FALSE /\ stops' = stops + 1
                /\ UNCHANGED <<pos, done, delivered, crashes>>

Restart == /\ ~up /\ up' = TRUE /\ pos' = committed /\ done' = committed
           /\ UNCHANGED <<committed, seda, delivered, crashes, stops>>

Next == Poll \/ Process \/ IdlePoll \/ RestGet \/ Crash \/ GracefulStop \/ Restart
Spec == Init /\ [][Next]_vars

AtRest == up /\ done = N /\ seda = <<>>
NoLoss == AtRest => delivered = 0..(N-1)
=====================================================================================
