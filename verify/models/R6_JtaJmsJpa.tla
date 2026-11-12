----------------------------- MODULE R6_JtaJmsJpa -----------------------------
(* Route (camel-quarkus-examples/jms-jpa, CamelRoutes.java):
     from("direct:trans").transacted()
        .process(enlist DummyXAResource) .to("jms:outbound?disableReplyTo=true")
        .to("bean:auditLog?method=createAuditLog(${body})").to("jpa:org.acme.AuditLog")
        .choice().when(body().startsWith("fail")) -> throw RuntimeException
     from("xajms:outbound").transacted().to(bean auditLog "-ok").to("jpa:org.acme.AuditLog");
   Semantics: with the example's Narayana JTA + XA pooled JMS connection factory, the JMS send and the
   JPA insert are enlisted in one global transaction; an exception rolls back both; commit is 2PC
   and in-doubt branches are completed by the transaction manager's recovery (assumes its log survives).
   Knob XA=FALSE: the JMS connection is not enlisted (e.g. a plain connection factory), the send is
   committed immediately while the JPA insert waits for the route's transaction.            *)
EXTENDS Naturals, FiniteSets

CONSTANTS XA, MaxCrashes
Msgs == {"ok1", "fail1"}
Fails(m) == m = "fail1"

VARIABLES req, cur, st, sentTx, queue, audit, up, crashes
vars == <<req, cur, st, sentTx, queue, audit, up, crashes>>
\* audit: set of rows; <<m, "in">> written by direct:trans, <<m, "ok">> by the JMS consumer

Init == /\ req = Msgs /\ cur = "none" /\ st = "idle" /\ sentTx = FALSE
        /\ queue = {} /\ audit = {} /\ up = TRUE /\ crashes = 0

Begin(m) == /\ up /\ st = "idle" /\ m \in req
            /\ cur' = m /\ st' = "begun" /\ UNCHANGED <<req, sentTx, queue, audit, up, crashes>>

SendJms == /\ up /\ st = "begun"
           /\ IF XA THEN /\ sentTx' = TRUE /\ UNCHANGED queue        \* pending in the global tx
                    ELSE /\ queue' = queue \cup {cur} /\ UNCHANGED sentTx   \* committed now
           /\ st' = "sent" /\ UNCHANGED <<req, cur, audit, up, crashes>>

\* JPA insert, then either the choice() throws (rollback) or the transaction commits
Finish == /\ up /\ st = "sent"
          /\ IF Fails(cur)
               THEN /\ UNCHANGED <<queue, audit>>                       \* rollback of the global tx
               ELSE /\ audit' = audit \cup {<<cur, "in">>}
                    /\ queue' = IF XA THEN queue \cup {cur} ELSE queue
          /\ req' = req \ {cur} /\ cur' = "none" /\ st' = "idle" /\ sentTx' = FALSE
          /\ UNCHANGED <<up, crashes>>

\* transacted JMS consumer: consume + insert "-ok" in one transaction
Consume(m) == /\ up /\ m \in queue
              /\ queue' = queue \ {m} /\ audit' = audit \cup {<<m, "ok">>}
              /\ UNCHANGED <<req, cur, st, sentTx, up, crashes>>

\* crash before commit: the global tx is rolled back (XA) -- the REST caller gets an error
Crash == /\ up /\ crashes < MaxCrashes
         /\ up' = FALSE /\ crashes' = crashes + 1 /\ cur' = "none" /\ st' = "idle" /\ sentTx' = FALSE
         /\ UNCHANGED <<req, queue, audit>>
Restart == /\ ~up /\ up' = TRUE /\ UNCHANGED <<req, cur, st, sentTx, queue, audit, crashes>>

Next == \/ \E m \in Msgs : Begin(m) \/ Consume(m)
        \/ SendJms \/ Finish \/ Crash \/ Restart
Spec == Init /\ [][Next]_vars

AtRest == up /\ req = {} /\ st = "idle" /\ queue = {}
\* once everything settled, every "-ok" row has its originating row and vice versa:
\* the JMS send never survives a rolled-back insert, and a committed insert always has its message
Atomic == AtRest => \A m \in Msgs : (<<m, "ok">> \in audit) <=> (<<m, "in">> \in audit)
===============================================================================
