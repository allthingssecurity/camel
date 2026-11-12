---------------------------- MODULE R7_SedaRequestReply ----------------------------
(* Route (camel docs, components/camel-seda/src/main/docs/seda-component.adoc "Use of Request Reply"):
     from("mina:tcp://0.0.0.0:9876?textline=true&sync=true").to("seda:input");
     from("seda:input").to("bean:processInput").to("bean:createResponse");
   Semantics: the mina exchange is InOut, so to("seda:input") queues the exchange and blocks the caller
   until the seda consumer completes it (or timeout=30000 ms -> ExchangeTimedOutException).
   seda consumer: concurrentConsumers=1 (default) thread.
   Knob Cycle=TRUE: the seda:input route additionally calls to("seda:input") InOut (e.g. a nested step
   routed back through the same queue) -- a wait-for cycle on a single consumer thread.
   NoDeadlock: while some request is unanswered, some step can still happen (timeouts not modelled:
   with them a deadlock shows up as every such request failing after 30 s).                     *)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS Cycle, Consumers
Reqs == {"r1", "r2"}

VARIABLES queue,    \* seda:input queue: sequence of <<req, depth>>
          busy,     \* consumer threads in use: set of <<thread, item, phase>>
          replied
vars == <<queue, busy, replied>>
Threads == 1..Consumers

Init == /\ queue = <<>> /\ busy = {} /\ replied = {}

\* a TCP client sends a request: the mina thread enqueues and waits
Send(r) == /\ r \notin replied /\ \A i \in DOMAIN queue : queue[i][1] # r
           /\ \A b \in busy : b[2][1] # r
           /\ queue' = Append(queue, <<r, 0>>) /\ UNCHANGED <<busy, replied>>

Take(t) == /\ queue # <<>> /\ \A b \in busy : b[1] # t
           /\ busy' = busy \cup {<<t, Head(queue), "run">>} /\ queue' = Tail(queue)
           /\ UNCHANGED replied

\* the route body: in the cycle variant a depth-0 item first calls seda:input InOut and waits
Step(b) == /\ b \in busy /\ b[3] = "run"
           /\ IF Cycle /\ b[2][2] = 0
                THEN /\ queue' = Append(queue, <<b[2][1], 1>>)
                     /\ busy' = (busy \ {b}) \cup {<<b[1], b[2], "wait">>}
                     /\ UNCHANGED replied
                ELSE /\ busy' = busy \ {b} /\ UNCHANGED queue
                     /\ replied' = IF b[2][2] = 0 THEN replied \cup {b[2][1]} ELSE replied

\* a waiting depth-0 item resumes when no depth-1 item for it is queued or running
Resume(b) == /\ b \in busy /\ b[3] = "wait"
             /\ \A i \in DOMAIN queue : queue[i] # <<b[2][1], 1>>
             /\ \A c \in busy : c[2] # <<b[2][1], 1>>
             /\ busy' = (busy \ {b}) \cup {<<b[1], b[2], "run2">>} /\ UNCHANGED <<queue, replied>>

Finish(b) == /\ b \in busy /\ b[3] = "run2"
             /\ busy' = busy \ {b} /\ replied' = replied \cup {b[2][1]} /\ UNCHANGED queue

Next == \/ \E r \in Reqs : Send(r)
        \/ \E t \in Threads : Take(t)
        \/ \E b \in busy : Step(b)
        \/ \E b \in busy : Resume(b)
        \/ \E b \in busy : Finish(b)
Spec == Init /\ [][Next]_vars

Started == queue # <<>> \/ busy # {}
NoDeadlock == (Started /\ replied # Reqs) => ENABLED Next
====================================================================================
