/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.camel.jta;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.transaction.Status;
import jakarta.transaction.Transaction;
import jakarta.transaction.TransactionManager;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A route of a PROPAGATION_REQUIRES_NEW policy called from a route of the same policy runs in its own transaction, as
 * with the camel-spring transaction error handler: a markRollbackOnlyLast() in it rolls back only that transaction.
 */
class TransactionErrorHandlerRequiresNewNestedTest {

    private final TransactionManager tm = com.arjuna.ats.jta.TransactionManager.transactionManager();
    private final List<Transaction> begun = new CopyOnWriteArrayList<>();
    private final List<Transaction> committed = new CopyOnWriteArrayList<>();
    private final List<Transaction> rolledBack = new CopyOnWriteArrayList<>();
    private volatile Transaction outerTx;
    private volatile Transaction innerTx;
    private volatile Transaction afterInnerTx;
    private volatile boolean transactedAfterInner;
    private CamelContext camelContext;

    @BeforeEach
    void setUp() throws Exception {
        camelContext = new DefaultCamelContext();
        // as the PROPAGATION_REQUIRES_NEW and PROPAGATION_REQUIRED policies of Camel Quarkus
        camelContext.getRegistry().bind("PROPAGATION_REQUIRES_NEW", new TestPolicy(true));
        camelContext.getRegistry().bind("PROPAGATION_REQUIRED", new TestPolicy(false));
        camelContext.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                from("direct:outer").transacted("PROPAGATION_REQUIRES_NEW")
                        .process(e -> outerTx = tm.getTransaction())
                        .to("direct:inner")
                        .process(e -> {
                            afterInnerTx = tm.getTransaction();
                            transactedAfterInner = e.isTransacted();
                        });

                from("direct:inner").transacted("PROPAGATION_REQUIRES_NEW")
                        .process(e -> innerTx = tm.getTransaction())
                        .markRollbackOnlyLast();

                from("direct:outer-required").transacted("PROPAGATION_REQUIRED")
                        .process(e -> outerTx = tm.getTransaction())
                        .to("direct:inner-required")
                        .process(e -> transactedAfterInner = e.isTransacted());

                from("direct:inner-required").transacted("PROPAGATION_REQUIRED")
                        .process(e -> innerTx = tm.getTransaction());
            }
        });
        camelContext.start();
    }

    @AfterEach
    void tearDown() {
        if (camelContext != null) {
            camelContext.stop();
        }
    }

    @Test
    void nestedRequiresNewRunsInItsOwnTransaction() throws Exception {
        Exchange result = camelContext.createProducerTemplate().send("direct:outer", e -> e.getIn().setBody("Hello"));

        assertNull(result.getException());
        assertEquals(2, begun.size(), "the inner route begins its own transaction");
        assertNotSame(outerTx, innerTx);
        // rollback only last: only the inner transaction is rolled back, the outer one goes on and commits
        assertEquals(List.of(innerTx), rolledBack);
        assertEquals(List.of(outerTx), committed);
        assertSame(outerTx, afterInnerTx, "the outer transaction is resumed after the inner one");
        assertTrue(transactedAfterInner, "the exchange is still transacted after the inner transaction");
        assertNull(tm.getTransaction());
    }

    @Test
    void nestedRequiredJoinsTheTransaction() throws Exception {
        Exchange result
                = camelContext.createProducerTemplate().send("direct:outer-required", e -> e.getIn().setBody("Hello"));

        assertNull(result.getException());
        assertEquals(1, begun.size());
        assertSame(outerTx, innerTx);
        assertEquals(List.of(outerTx), committed);
        assertTrue(transactedAfterInner);
        assertNull(tm.getTransaction());
    }

    private final class TestPolicy extends JtaTransactionPolicy {
        private final boolean requiresNew;

        TestPolicy(boolean requiresNew) {
            this.requiresNew = requiresNew;
        }

        @Override
        public boolean isRequiresNew() {
            return requiresNew;
        }

        @Override
        public void run(Runnable runnable) throws Throwable {
            Transaction suspended = null;
            boolean isNew = requiresNew || tm.getStatus() == Status.STATUS_NO_TRANSACTION;
            if (isNew) {
                if (tm.getStatus() != Status.STATUS_NO_TRANSACTION) {
                    suspended = tm.suspend();
                }
                tm.begin();
                begun.add(tm.getTransaction());
            }
            try {
                try {
                    runnable.run();
                } catch (Throwable t) {
                    if (isNew) {
                        rolledBack.add(tm.getTransaction());
                        tm.rollback();
                    } else {
                        tm.setRollbackOnly();
                    }
                    throw t;
                }
                if (isNew) {
                    committed.add(tm.getTransaction());
                    tm.commit();
                }
            } finally {
                if (suspended != null) {
                    tm.resume(suspended);
                }
            }
        }
    }
}
